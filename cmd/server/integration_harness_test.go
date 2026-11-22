package main

// T9 integration harness. Every scenario runs the production assembly
// (newMatchingRegistry + handleWebSocket) behind a real httptest server and
// drives it with real WebSocket clients. Inputs and outputs cross the real v1
// encoder/decoder. The only injected dependencies are the synthetic test-v1
// catalog, a controllable song provider and a fake clock, all passed through
// the same assembly function that production uses. Assertions use wire
// messages only; no private server state is read to decide success.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	message "temp-ws/internal/message/v1"
	catalogstore "temp-ws/internal/music"
	music "temp-ws/internal/music/v1"
	"temp-ws/internal/room"
)

const (
	// integrationDeadline bounds every scenario so a missing event fails
	// instead of hanging. Nothing waits on wall-clock sleeps.
	integrationDeadline = 20 * time.Second
	fixtureDir          = "../../docs/specs/random-matchmaking/fixtures/"
	syntheticVersion    = "test-v1"
)

// Request categories taken from the synthetic test-v1 allowedRequests.
var (
	catAny           = music.Category{Decade: "any", ArtistType: "any", Gender: "any", Genre: "any"}
	catAllDecades    = music.Category{Decade: "all", ArtistType: "any", Gender: "any", Genre: "any"}
	catBallad2010s   = music.Category{Decade: "2010s", ArtistType: "any", Gender: "any", Genre: "korean_ballad"}
	catSoloFemale    = music.Category{Decade: "2010s", ArtistType: "solo", Gender: "female", Genre: "korean_ballad"}
	catPopGroupMale  = music.Category{Decade: "2020s", ArtistType: "group", Gender: "male", Genre: "synthetic_pop"}
	roomAll          = music.Category{Decade: "all", ArtistType: "all", Gender: "all", Genre: "all"}
	roomBallad2010s  = music.Category{Decade: "2010s", ArtistType: "all", Gender: "all", Genre: "korean_ballad"}
	roomPopGroupMale = catPopGroupMale
)

// ---------------------------------------------------------------------------
// Fake clock: the Room actor's preload guard and round ticks fire only when a
// test calls Advance. Shared by all rooms of one server instance.

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	clock *fakeClock
	at    time.Time
	c     chan time.Time
	done  bool
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTimer(d time.Duration) room.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{clock: c, at: c.now.Add(d), c: make(chan time.Time, 1)}
	c.timers = append(c.timers, t)
	return t
}

// Advance moves time forward and delivers every due timer before returning.
// A Room arms its next timer in the same actor turn that emits the event the
// test reads, so "read event, then Advance" never races the arming.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	live := c.timers[:0]
	for _, t := range c.timers {
		if t.done {
			continue
		}
		if !t.at.After(c.now) {
			t.done = true
			t.c <- c.now
			continue
		}
		live = append(live, t)
	}
	c.timers = live
}

func (t *fakeTimer) C() <-chan time.Time { return t.c }
func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	was := !t.done
	t.done = true
	return was
}

// ---------------------------------------------------------------------------
// Controlled song provider (the injected MusicProvider). Held categories hand
// each lookup to the test, which decides when and how it completes; other
// categories pass straight through to the synthetic catalog.

type fetchCall struct {
	version  string
	category music.Category
	reply    chan error
}

type controlledSongs struct {
	store *catalogstore.CatalogStore
	hold  func(music.Category) bool
	calls chan fetchCall
}

var errInjectedLookup = errors.New("injected song lookup failure")

func (p *controlledSongs) FetchSongs(ctx context.Context, version string, category music.Category) ([]music.ManifestSong, error) {
	if p.hold != nil && p.hold(category) {
		call := fetchCall{version: version, category: category, reply: make(chan error, 1)}
		select {
		case p.calls <- call:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case err := <-call.reply:
			if err != nil {
				return nil, err
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.store.FetchSongs(ctx, version, category)
}

// ---------------------------------------------------------------------------
// Server

type itServer struct {
	t      *testing.T
	ctx    context.Context
	url    string
	clock  *fakeClock
	songs  *controlledSongs
	exited chan struct{} // one value per finished WebSocket handler
	cancel context.CancelFunc
	done   <-chan struct{} // matchmaker (and therefore every Room) stopped
}

type serverConfig struct {
	hold func(music.Category) bool
}

func loadSyntheticCatalog(t *testing.T) *catalogstore.CatalogStore {
	t.Helper()
	store, err := loadMatchingCatalog(fixtureDir+"catalog.json", fixtureDir+"manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	if store.Version() != syntheticVersion {
		t.Fatalf("synthetic catalog version %q", store.Version())
	}
	return store
}

func startServer(t *testing.T, cfg serverConfig) *itServer {
	t.Helper()
	store := loadSyntheticCatalog(t)
	ctx, cancel := context.WithTimeout(context.Background(), integrationDeadline)
	clock := newFakeClock()
	songs := &controlledSongs{store: store, hold: cfg.hold, calls: make(chan fetchCall, 16)}
	registry, done := newMatchingRegistry(ctx, store, room.WithSongs(songs), room.WithClock(clock))
	s := &itServer{t: t, ctx: ctx, clock: clock, songs: songs, exited: make(chan struct{}, 1024), cancel: cancel, done: done}
	ws := handleWebSocket(registry)
	// The wrapper only observes that the production handler (including its
	// deferred Registry.Disconnect) has returned; it changes no behavior.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws(w, r)
		s.exited <- struct{}{}
	}))
	s.url = "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?protocolVersion=1"
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("matchmaker did not stop after context cancellation")
		}
		server.Close()
	})
	return s
}

// awaitHandlerExit waits until one WebSocket handler has returned, which
// proves its disconnect has been enqueued to the matchmaker before any input
// that the test sends afterwards.
func (s *itServer) awaitHandlerExit(t *testing.T) {
	t.Helper()
	select {
	case <-s.exited:
	case <-s.ctx.Done():
		t.Fatal("WebSocket handler did not exit")
	}
}

func (s *itServer) nextFetch(t *testing.T) fetchCall {
	t.Helper()
	select {
	case call := <-s.songs.calls:
		return call
	case <-s.ctx.Done():
		t.Fatal("song lookup was not requested")
		return fetchCall{}
	}
}

func (s *itServer) noPendingFetch(t *testing.T) {
	t.Helper()
	select {
	case call := <-s.songs.calls:
		t.Fatalf("unexpected extra song lookup: %+v", call.category)
	default:
	}
}

// ---------------------------------------------------------------------------
// Client

type wsClient struct {
	t        *testing.T
	ctx      context.Context
	conn     *websocket.Conn
	name     string
	playerID string
	roomID   string // current room per the wire contract; "" in the lobby
	roomReq  string // requestID whose WELCOME created the current membership
}

func (s *itServer) dial(t *testing.T, name string) *wsClient {
	t.Helper()
	conn, resp, err := websocket.Dial(s.ctx, s.url, nil)
	if err != nil {
		t.Fatalf("%s: dial: %v", name, err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	conn.SetReadLimit(message.MaxFrameBytes)
	t.Cleanup(func() { _ = conn.CloseNow() })
	return &wsClient{t: t, ctx: s.ctx, conn: conn, name: name}
}

func (c *wsClient) close() { _ = c.conn.CloseNow() }

func (c *wsClient) send(kind string, payload any) {
	c.t.Helper()
	data, err := message.Encode(message.Message{Type: kind, Payload: payload}, message.ClientToServer)
	if err != nil {
		c.t.Fatalf("%s: encode %s: %v", c.name, kind, err)
	}
	if err := c.conn.Write(c.ctx, websocket.MessageText, data); err != nil {
		c.t.Fatalf("%s: write %s: %v", c.name, kind, err)
	}
}

// sendRaw writes a frame verbatim, e.g. a legacy client's extra fields that
// the encoder would strip. The server still decodes it with the real decoder.
func (c *wsClient) sendRaw(frame string) {
	c.t.Helper()
	if err := c.conn.Write(c.ctx, websocket.MessageText, []byte(frame)); err != nil {
		c.t.Fatalf("%s: write: %v", c.name, err)
	}
}

func roomIDOf(payload any) (string, bool) {
	switch p := payload.(type) {
	case *message.RoomPayload:
		return p.RoomID, true
	case *message.PlayerPayload:
		return p.RoomID, true
	case *message.RoomCommandFailedPayload:
		return p.RoomID, true
	case *message.GameAbortedPayload:
		return p.RoomID, true
	case *message.PreloadSongPayload:
		return p.RoomID, true
	case *message.RoundStartPayload:
		return p.RoomID, true
	case *message.CountDownPayload:
		return p.RoomID, true
	case *message.WrongAnswerPayload:
		return p.RoomID, true
	case *message.RoundResultPayload:
		return p.RoomID, true
	case *message.GameOverPayload:
		return p.RoomID, true
	}
	return "", false
}

// next reads one frame through the real decoder and enforces envelope and
// room-scope contracts: a server timestamp, and every room event except
// WELCOME/ROOM_LEFT carrying the roomID of the client's current membership.
func (c *wsClient) next() message.Message {
	c.t.Helper()
	kind, data, err := c.conn.Read(c.ctx)
	if err != nil {
		c.t.Fatalf("%s: read: %v", c.name, err)
	}
	if kind != websocket.MessageText {
		c.t.Fatalf("%s: non-text frame", c.name)
	}
	msg, err := message.Decode(data, message.ServerToClient)
	if err != nil {
		c.t.Fatalf("%s: decode %s: %v", c.name, data, err)
	}
	if msg.Timestamp == nil || *msg.Timestamp <= 0 {
		c.t.Fatalf("%s: %s without server timestamp", c.name, msg.Type)
	}
	switch p := msg.Payload.(type) {
	case *message.WelcomePayload:
		if c.playerID != "" && p.PlayerID != c.playerID {
			c.t.Fatalf("%s: playerID changed on the same connection: %s -> %s", c.name, c.playerID, p.PlayerID)
		}
		c.playerID, c.roomID, c.roomReq = p.PlayerID, p.RoomID, p.RequestID
		return msg
	case *message.MatchCancelledPayload:
		if msg.Type == message.TypeMatchCancelled && p.RequestID == c.roomReq && c.roomID != "" {
			c.roomID, c.roomReq = "", ""
		}
		return msg
	}
	if msg.Type == message.TypeRoomLeft {
		if p := msg.Payload.(*message.RoomPayload); p.RoomID == c.roomID {
			c.roomID, c.roomReq = "", ""
		}
		return msg
	}
	if id, scoped := roomIDOf(msg.Payload); scoped {
		if c.roomID == "" || id != c.roomID {
			c.t.Fatalf("%s: %s for room %s while current room is %q", c.name, msg.Type, id, c.roomID)
		}
		if msg.Type == message.TypePlayerKicked {
			c.roomID, c.roomReq = "", ""
		}
	}
	return msg
}

func (c *wsClient) expect(kind string) message.Message {
	c.t.Helper()
	msg := c.next()
	if msg.Type != kind {
		c.t.Fatalf("%s: want %s, got %s %+v", c.name, kind, msg.Type, msg.Payload)
	}
	return msg
}

func (c *wsClient) requestWith(id, version string, category music.Category) {
	c.t.Helper()
	c.send(message.TypeMatchRequest, message.MatchRequestPayload{RequestID: id, CatalogVersion: version, Category: category})
}

func (c *wsClient) request(category music.Category) string {
	c.t.Helper()
	id := uuid.NewString()
	c.requestWith(id, syntheticVersion, category)
	return id
}

func (c *wsClient) cancel(id string) {
	c.t.Helper()
	c.send(message.TypeMatchCancel, message.MatchCancelPayload{RequestID: id})
}

func (c *wsClient) expectWaiting(id string) {
	c.t.Helper()
	if p := c.expect(message.TypeMatchWaiting).Payload.(*message.RequestPayload); p.RequestID != id {
		c.t.Fatalf("%s: MATCH_WAITING for %s, want %s", c.name, p.RequestID, id)
	}
}

func (c *wsClient) expectCancelled(id string) {
	c.t.Helper()
	if p := c.expect(message.TypeMatchCancelled).Payload.(*message.RequestPayload); p.RequestID != id {
		c.t.Fatalf("%s: MATCH_CANCELLED for %s, want %s", c.name, p.RequestID, id)
	}
}

func (c *wsClient) expectFailed(id string, code message.MatchFailureCode) {
	c.t.Helper()
	p := c.expect(message.TypeMatchFailed).Payload.(*message.MatchFailedPayload)
	if p.RequestID != id || p.Code != code {
		c.t.Fatalf("%s: MATCH_FAILED %+v, want %s/%s", c.name, p, id, code)
	}
}

// expectWelcome accepts the WELCOME for id. A MATCH_WAITING for the same id
// may precede it (protocol: normal assignment need not be preceded by it).
func (c *wsClient) expectWelcome(id string) *message.WelcomePayload {
	c.t.Helper()
	msg := c.next()
	if msg.Type == message.TypeMatchWaiting && msg.Payload.(*message.RequestPayload).RequestID == id {
		msg = c.next()
	}
	if msg.Type != message.TypeWelcome {
		c.t.Fatalf("%s: want WELCOME for %s, got %s %+v", c.name, id, msg.Type, msg.Payload)
	}
	w := msg.Payload.(*message.WelcomePayload)
	c.checkWelcome(w, id)
	return w
}

func (c *wsClient) checkWelcome(w *message.WelcomePayload, id string) {
	c.t.Helper()
	switch {
	case w.RequestID != id:
		c.t.Fatalf("%s: WELCOME requestID %s, want %s", c.name, w.RequestID, id)
	case !slices.Contains(w.Players, w.PlayerID) || !slices.Contains(w.Players, w.HostID):
		c.t.Fatalf("%s: WELCOME players %v must include self %s and host %s", c.name, w.Players, w.PlayerID, w.HostID)
	case len(w.Players) < 2 || len(w.Players) > message.MaxPlayers:
		c.t.Fatalf("%s: WELCOME player count %d", c.name, len(w.Players))
	case w.RoomState != "waiting" || w.MaxPlayers != message.MaxPlayers || w.CatalogVersion != syntheticVersion:
		c.t.Fatalf("%s: WELCOME room fields %+v", c.name, w)
	case w.Category.ValidateResolved() != nil:
		c.t.Fatalf("%s: WELCOME category is not a resolved room category: %+v", c.name, w.Category)
	}
	seen := map[string]bool{}
	for _, p := range w.Players {
		if seen[p] {
			c.t.Fatalf("%s: duplicate player in WELCOME %v", c.name, w.Players)
		}
		seen[p] = true
	}
}

func (c *wsClient) expectPlayer(kind, playerID string) {
	c.t.Helper()
	if p := c.expect(kind).Payload.(*message.PlayerPayload); p.PlayerID != playerID {
		c.t.Fatalf("%s: %s for %s, want %s", c.name, kind, p.PlayerID, playerID)
	}
}

func (c *wsClient) start() {
	c.t.Helper()
	c.send(message.TypeStartGame, message.RoomPayload{RoomID: c.roomID})
}

func (c *wsClient) expectCommandFailed(code message.RoomFailureCode) {
	c.t.Helper()
	p := c.expect(message.TypeRoomCommandFailed).Payload.(*message.RoomCommandFailedPayload)
	if p.Command != message.TypeStartGame || p.Code != code {
		c.t.Fatalf("%s: ROOM_COMMAND_FAILED %+v, want %s", c.name, p, code)
	}
}

func (c *wsClient) expectAborted(reason message.AbortReason) {
	c.t.Helper()
	if p := c.expect(message.TypeGameAborted).Payload.(*message.GameAbortedPayload); p.Reason != reason {
		c.t.Fatalf("%s: GAME_ABORTED %s, want %s", c.name, p.Reason, reason)
	}
}

// leave sends LEAVE_ROOM for the current room and waits for its ROOM_LEFT.
func (c *wsClient) leave() {
	c.t.Helper()
	id := c.roomID
	if id == "" {
		c.t.Fatalf("%s: leave outside a room", c.name)
	}
	c.send(message.TypeLeaveRoom, message.RoomPayload{RoomID: id})
	if p := c.expect(message.TypeRoomLeft).Payload.(*message.RoomPayload); p.RoomID != id {
		c.t.Fatalf("%s: ROOM_LEFT for %s, want %s", c.name, p.RoomID, id)
	}
	if c.roomID != "" {
		c.t.Fatalf("%s: still in room after ROOM_LEFT", c.name)
	}
}

func (c *wsClient) ready(round int) {
	c.t.Helper()
	c.send(message.TypeReadyToPlay, message.ReadyToPlayPayload{RoomID: c.roomID, RoundNumber: round})
}

func (c *wsClient) answer(text string) {
	c.t.Helper()
	c.send(message.TypeSubmitAnswer, message.SubmitAnswerPayload{RoomID: c.roomID, Answer: text})
}

// lobbyBarrier proves, through this connection's FIFO, that the matchmaker
// has processed every earlier input from it: an unknown-ID MATCH_CANCEL is
// answered with MATCH_CANCELLED and has no other effect (protocol tombstone).
func (c *wsClient) lobbyBarrier() {
	c.t.Helper()
	id := uuid.NewString()
	c.cancel(id)
	c.expectCancelled(id)
}

// roomBarrier proves, through the Room actor's FIFO, that the Room has handled
// every earlier game command from this member: START_GAME outside waiting (or
// from a non-host) is answered only to the sender and changes nothing.
func (c *wsClient) roomBarrier(code message.RoomFailureCode) {
	c.t.Helper()
	c.start()
	c.expectCommandFailed(code)
}

// pair matches two fresh lobby clients and returns their WELCOMEs.
func pair(t *testing.T, a, b *wsClient, category music.Category) (*message.WelcomePayload, *message.WelcomePayload) {
	t.Helper()
	ra := a.request(category)
	a.expectWaiting(ra)
	rb := b.request(category)
	wb := b.expectWelcome(rb)
	wa := a.expectWelcome(ra)
	if wa.RoomID != wb.RoomID || !reflect.DeepEqual(wa.Players, []string{wa.PlayerID, wb.PlayerID}) || wa.HostID != wa.PlayerID {
		t.Fatalf("pair: %+v / %+v", wa, wb)
	}
	return wa, wb
}

// join adds a lobby client to the room of existing members, checking that the
// newcomer's WELCOME precedes its own PLAYER_JOINED and that members are told.
func join(t *testing.T, newcomer *wsClient, category music.Category, members ...*wsClient) *message.WelcomePayload {
	t.Helper()
	id := newcomer.request(category)
	w := newcomer.expectWelcome(id)
	newcomer.expectPlayer(message.TypePlayerJoined, w.PlayerID)
	for _, m := range members {
		if m.roomID != w.RoomID {
			t.Fatalf("%s joined %s, member %s is in %s", newcomer.name, w.RoomID, m.name, m.roomID)
		}
		m.expectPlayer(message.TypePlayerJoined, w.PlayerID)
	}
	return w
}

// waitGoroutines checks a condition, not elapsed time: it re-reads the
// goroutine count until it is at most limit, failing at a bounded deadline
// with a stack dump. Exiting goroutines finish asynchronously after the wire
// events that caused them, so a single immediate sample would be racy.
func waitGoroutines(t *testing.T, limit int) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for runtime.NumGoroutine() > limit {
		if time.Now().After(deadline) {
			_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			t.Errorf("goroutines did not return to %d: %d", limit, runtime.NumGoroutine())
			return false
		}
		<-ticker.C
	}
	return true
}
