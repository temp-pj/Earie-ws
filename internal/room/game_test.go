package room

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	message "temp-ws/internal/message/v1"
	music "temp-ws/internal/music/v1"
	sc "temp-ws/internal/sessioncontract"
)

// fakeClock fires timers synchronously inside Advance. The actor arms every
// timer before emitting the batch that announces its phase, so a test that has
// received that batch can Advance without racing timer creation.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}
type fakeTimer struct {
	clock *fakeClock
	when  time.Time
	ch    chan time.Time
	done  bool
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1_800_000_000, 0)} }
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (c *fakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{clock: c, when: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, t)
	return t
}
func (t *fakeTimer) C() <-chan time.Time { return t.ch }
func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	was := !t.done
	t.done = true
	return was
}

// Advance returns how many timers fired.
func (c *fakeClock) Advance(d time.Duration) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	fired := 0
	for _, t := range c.timers {
		if !t.done && !t.when.After(c.now) {
			t.done = true
			t.ch <- c.now
			fired++
		}
	}
	return fired
}
func (c *fakeClock) active() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if !t.done {
			n++
		}
	}
	return n
}

type fetchCall struct {
	ctx      context.Context
	version  string
	category music.Category
	reply    chan fetchReply
}
type fetchReply struct {
	songs []music.ManifestSong
	err   error
}

// gateSongs hands each lookup to the test, which decides result and timing.
type gateSongs struct {
	calls  chan fetchCall
	exited chan struct{}
}

func newGateSongs() *gateSongs {
	return &gateSongs{calls: make(chan fetchCall, 8), exited: make(chan struct{}, 8)}
}
func (g *gateSongs) FetchSongs(ctx context.Context, version string, category music.Category) ([]music.ManifestSong, error) {
	defer func() { g.exited <- struct{}{} }()
	call := fetchCall{ctx: ctx, version: version, category: category, reply: make(chan fetchReply, 1)}
	g.calls <- call
	select {
	case r := <-call.reply:
		return r.songs, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (g *gateSongs) next(t *testing.T) fetchCall {
	t.Helper()
	select {
	case c := <-g.calls:
		return c
	case <-time.After(3 * time.Second):
		t.Fatal("song lookup deadline")
		return fetchCall{}
	}
}

// testSongs are synthetic; answer is the existing PrimaryTitle of the title.
func testSongs(n int) ([]music.ManifestSong, map[string]string) {
	songs := make([]music.ManifestSong, n)
	answers := make(map[string]string, n)
	for i := range songs {
		isrc := fmt.Sprintf("ZZTST26%05d", i+1)
		songs[i] = music.ManifestSong{ISRC: isrc, MBID: fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1), Title: fmt.Sprintf("Song %02d", i+1), Artist: "Synthetic", ReleaseDate: "2015-01-01", Duration: 200, StartTime: 10}
		answers[isrc] = fmt.Sprintf("Song%02d", i+1)
	}
	return songs, answers
}

func startMessage(roomID string) message.Message {
	return message.Message{Type: message.TypeStartGame, Payload: &message.RoomPayload{RoomID: roomID}}
}
func readyMessage(roomID string, round int) message.Message {
	return message.Message{Type: message.TypeReadyToPlay, Payload: &message.ReadyToPlayPayload{RoomID: roomID, RoundNumber: round}}
}
func answerMessage(roomID, answer string) message.Message {
	return message.Message{Type: message.TypeSubmitAnswer, Payload: &message.SubmitAnswerPayload{RoomID: roomID, Answer: answer}}
}

type gameRoom struct {
	t       *testing.T
	g       *Gateway
	sink    *testSink
	h       sc.RoomHandle
	members []sc.Member // arrival order; members[0] is host
	songs   *gateSongs
	clock   *fakeClock
}

func newGameRoom(t *testing.T, players int) *gameRoom {
	t.Helper()
	songs, clock := newGateSongs(), newFakeClock()
	g, s := newTestGateway(t, WithSongs(songs), WithClock(clock))
	h, cmd := createPair(t, g, s)
	publishRoom(t, g, s, h)
	gr := &gameRoom{t: t, g: g, sink: s, h: h, members: []sc.Member{cmd.Members[1], cmd.Members[0]}, songs: songs, clock: clock}
	for i := 3; i <= players; i++ {
		m := testMember(i)
		if err := g.Register(context.Background(), sc.RegisterCommand{Token: sc.OperationToken(100 + i), RoomID: h.RoomID, Member: m}); err != nil {
			t.Fatal(err)
		}
		if b := nextBatch(t, s); b.Events[0].(sc.RegistrationResult).Rejection != "" {
			t.Fatalf("register: %+v", b)
		}
		gr.members = append(gr.members, m)
	}
	return gr
}
func (gr *gameRoom) command(m sc.Member, msg message.Message) {
	gr.t.Helper()
	if err := gr.g.Command(context.Background(), gr.h.RoomID, m, msg); err != nil {
		gr.t.Fatal(err)
	}
}
func (gr *gameRoom) batch() sc.EventBatch { gr.t.Helper(); return nextBatch(gr.t, gr.sink) }
func (gr *gameRoom) leave(m sc.Member) sc.EventBatch {
	gr.t.Helper()
	if err := gr.g.Remove(context.Background(), sc.RemoveCommand{Token: 900, RoomID: gr.h.RoomID, PlayerID: m.PlayerID, RequestID: m.RequestID}); err != nil {
		gr.t.Fatal(err)
	}
	return gr.batch()
}

// start runs START_GAME through a successful lookup and returns the first
// PRELOAD_SONG together with the synthetic answers.
func (gr *gameRoom) start() (message.PreloadSongPayload, map[string]string) {
	gr.t.Helper()
	gr.command(gr.members[0], startMessage(gr.h.RoomID))
	if b := gr.batch(); b.Snapshot.Accepting || len(b.Events) != 0 {
		gr.t.Fatalf("starting barrier: %+v", b)
	}
	songs, answers := testSongs(music.TracksPerGame)
	gr.songs.next(gr.t).reply <- fetchReply{songs: songs}
	b := gr.batch()
	wires := wireMessages(b)
	if len(wires) != 2 || wires[0].Type != message.TypeGameStarted || wires[1].Type != message.TypePreloadSong {
		gr.t.Fatalf("game start: %+v", wires)
	}
	return wires[1].Payload.(message.PreloadSongPayload), answers
}

// readyAll sends READY_TO_PLAY from every member and returns ROUND_STARTED.
func (gr *gameRoom) readyAll(round int) message.RoundStartPayload {
	gr.t.Helper()
	for _, m := range gr.members {
		gr.command(m, readyMessage(gr.h.RoomID, round))
	}
	wires := wireMessages(gr.batch())
	if len(wires) != 1 || wires[0].Type != message.TypeRoundStarted {
		gr.t.Fatalf("round start: %+v", wires)
	}
	return wires[0].Payload.(message.RoundStartPayload)
}
func wireMessages(b sc.EventBatch) []message.Message {
	var out []message.Message
	for _, e := range b.Events {
		if w, ok := e.(sc.WireEvent); ok {
			out = append(out, w.Message)
		}
	}
	return out
}
func wireOf(t *testing.T, b sc.EventBatch, kind string) sc.WireEvent {
	t.Helper()
	for _, e := range b.Events {
		if w, ok := e.(sc.WireEvent); ok && w.Message.Type == kind {
			return w
		}
	}
	t.Fatalf("missing %s in %+v", kind, b)
	return sc.WireEvent{}
}
func hasWire(b sc.EventBatch, kind string) bool {
	for _, m := range wireMessages(b) {
		if m.Type == kind {
			return true
		}
	}
	return false
}
func failureCode(t *testing.T, b sc.EventBatch, to sc.Member) message.RoomFailureCode {
	t.Helper()
	w := wireOf(t, b, message.TypeRoomCommandFailed)
	if len(w.Recipients) != 1 || w.Recipients[0] != to {
		t.Fatalf("failure recipients: %+v", w.Recipients)
	}
	return w.Message.Payload.(message.RoomCommandFailedPayload).Code
}

func TestStartGamePermissionAndState(t *testing.T) {
	gr := newGameRoom(t, 2)
	host, guest := gr.members[0], gr.members[1]
	gr.command(guest, startMessage(gr.h.RoomID))
	if code := failureCode(t, gr.batch(), guest); code != message.NotHost {
		t.Fatal(code)
	}
	// A stale membership of the host player (old request) is ignored entirely;
	// the next batch belongs to the following valid command.
	stale := host
	stale.RequestID = guest.RequestID
	gr.command(stale, startMessage(gr.h.RoomID))
	gr.command(host, startMessage(gr.h.RoomID))
	if b := gr.batch(); b.Snapshot.Accepting || len(b.Events) != 0 {
		t.Fatalf("stale command produced events or start missing: %+v", b)
	}
	call := gr.songs.next(t)
	// Server authority: the fixed room category/version, never client input.
	if call.version != "test-v1" || call.category != (music.Category{Decade: "all", ArtistType: "all", Gender: "all", Genre: "all"}) {
		t.Fatalf("lookup: %+v", call)
	}
	// Duplicate START_GAME while starting fails without a second lookup.
	gr.command(host, startMessage(gr.h.RoomID))
	if code := failureCode(t, gr.batch(), host); code != message.InvalidState {
		t.Fatal(code)
	}
	select {
	case c := <-gr.songs.calls:
		t.Fatalf("duplicate lookup %+v", c)
	default:
	}
	// Starting closes admission in the same Room FIFO.
	if err := gr.g.Register(context.Background(), sc.RegisterCommand{Token: 50, RoomID: gr.h.RoomID, Member: testMember(9)}); err != nil {
		t.Fatal(err)
	}
	if r := gr.batch().Events[0].(sc.RegistrationResult); r.Rejection != sc.AdmissionStarted {
		t.Fatal(r)
	}
	songs, _ := testSongs(music.TracksPerGame)
	call.reply <- fetchReply{songs: songs}
	if b := gr.batch(); !hasWire(b, message.TypeGameStarted) {
		t.Fatalf("game did not start: %+v", b)
	}
	gr.command(host, startMessage(gr.h.RoomID))
	if code := failureCode(t, gr.batch(), host); code != message.InvalidState {
		t.Fatal(code)
	}
}

func TestStartGameNeedsTwoPlayers(t *testing.T) {
	gr := newGameRoom(t, 2)
	gr.leave(gr.members[1])
	// One member: room remains waiting and accepting, but cannot start.
	gr.command(gr.members[0], startMessage(gr.h.RoomID))
	b := gr.batch()
	if code := failureCode(t, b, gr.members[0]); code != message.NotEnoughPlayers || !b.Snapshot.Accepting {
		t.Fatalf("%s %+v", code, b.Snapshot)
	}
}

func TestStartLookupFailureReturnsToWaiting(t *testing.T) {
	songs, _ := testSongs(music.TracksPerGame)
	duplicated := append(append([]music.ManifestSong(nil), songs[:9]...), songs[0])
	for _, tc := range []struct {
		name  string
		reply fetchReply
	}{
		{"error", fetchReply{err: errors.New("lookup failed")}},
		{"nine songs", fetchReply{songs: songs[:9]}},
		{"duplicate ISRC", fetchReply{songs: duplicated}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gr := newGameRoom(t, 2)
			gr.command(gr.members[0], startMessage(gr.h.RoomID))
			gr.batch()
			gr.songs.next(t).reply <- tc.reply
			b := gr.batch()
			if code := failureCode(t, b, gr.members[0]); code != message.RoomInternalError || !b.Snapshot.Accepting || hasWire(b, message.TypeGameStarted) {
				t.Fatalf("%s %+v", code, b)
			}
			// Waiting again: admission and a retry both work.
			if err := gr.g.Register(context.Background(), sc.RegisterCommand{Token: 51, RoomID: gr.h.RoomID, Member: testMember(9)}); err != nil {
				t.Fatal(err)
			}
			if r := gr.batch().Events[0].(sc.RegistrationResult); r.Rejection != "" {
				t.Fatal(r)
			}
			gr.command(gr.members[0], startMessage(gr.h.RoomID))
			gr.batch()
			gr.songs.next(t).reply <- fetchReply{songs: songs}
			if b := gr.batch(); !hasWire(b, message.TypeGameStarted) || len(wireOf(t, b, message.TypeGameStarted).Recipients) != 3 {
				t.Fatalf("retry: %+v", b)
			}
		})
	}
}

func TestDepartureWhileStarting(t *testing.T) {
	t.Run("guest leaves, host told NOT_ENOUGH_PLAYERS, late songs discarded", func(t *testing.T) {
		gr := newGameRoom(t, 2)
		gr.command(gr.members[0], startMessage(gr.h.RoomID))
		gr.batch()
		call := gr.songs.next(t)
		b := gr.leave(gr.members[1])
		if code := failureCode(t, b, gr.members[0]); code != message.NotEnoughPlayers || !b.Snapshot.Accepting || b.Snapshot.PlayerCount != 1 {
			t.Fatalf("%s %+v", code, b.Snapshot)
		}
		songs, _ := testSongs(music.TracksPerGame)
		call.reply <- fetchReply{songs: songs}
		<-gr.songs.exited
		newcomer := testMember(9)
		if err := gr.g.Register(context.Background(), sc.RegisterCommand{Token: 52, RoomID: gr.h.RoomID, Member: newcomer}); err != nil {
			t.Fatal(err)
		}
		b = gr.batch()
		if r := b.Events[0].(sc.RegistrationResult); r.Rejection != "" || hasWire(b, message.TypeGameStarted) {
			t.Fatalf("late lookup started game: %+v", b)
		}
		// Ensure the stale result was consumed and ignored: no game ever starts.
		done, _ := gr.g.Close(context.Background(), sc.CloseCommand{RoomID: gr.h.RoomID})
		awaitDone(t, done)
		for len(gr.sink.batches) > 0 {
			if b := <-gr.sink.batches; hasWire(b, message.TypeGameStarted) {
				t.Fatal("stale start result applied")
			}
		}
	})
	t.Run("host leaves, successor inherits without a failure", func(t *testing.T) {
		gr := newGameRoom(t, 2)
		gr.command(gr.members[0], startMessage(gr.h.RoomID))
		gr.batch()
		gr.songs.next(t)
		b := gr.leave(gr.members[0])
		if hasWire(b, message.TypeRoomCommandFailed) || wireOf(t, b, message.TypeHostChanged).Message.Payload.(message.HostChangedPayload).PlayerID != gr.members[1].PlayerID || !b.Snapshot.Accepting {
			t.Fatalf("%+v", b)
		}
	})
	t.Run("two remain, start continues", func(t *testing.T) {
		gr := newGameRoom(t, 3)
		gr.command(gr.members[0], startMessage(gr.h.RoomID))
		gr.batch()
		call := gr.songs.next(t)
		if b := gr.leave(gr.members[2]); hasWire(b, message.TypeRoomCommandFailed) || b.Snapshot.Accepting {
			t.Fatalf("%+v", b)
		}
		songs, _ := testSongs(music.TracksPerGame)
		call.reply <- fetchReply{songs: songs}
		b := gr.batch()
		if w := wireOf(t, b, message.TypeGameStarted); len(w.Recipients) != 2 {
			t.Fatalf("participants: %+v", w.Recipients)
		}
	})
	t.Run("all leave, worker stops before Done", func(t *testing.T) {
		gr := newGameRoom(t, 2)
		gr.command(gr.members[0], startMessage(gr.h.RoomID))
		gr.batch()
		call := gr.songs.next(t)
		gr.leave(gr.members[1])
		gr.leave(gr.members[0])
		awaitDone(t, gr.h.Done)
		// Done implies the lookup worker observed cancellation and returned.
		select {
		case <-gr.songs.exited:
		default:
			t.Fatal("Done closed while lookup worker was running")
		}
		if call.ctx.Err() == nil {
			t.Fatal("lookup context not cancelled")
		}
		if err := gr.g.Command(context.Background(), gr.h.RoomID, gr.members[0], startMessage(gr.h.RoomID)); err != nil {
			t.Fatal("late command to closed room must be ignored:", err)
		}
	})
}

func TestFullGameTenRoundsThenResult(t *testing.T) {
	gr := newGameRoom(t, 2)
	host, guest := gr.members[0], gr.members[1]
	preload, answers := gr.start()
	seen := map[string]bool{}
	for round := 1; round <= music.TracksPerGame; round++ {
		if preload.RoundNumber != round || seen[preload.ISRC] || preload.RoomID != gr.h.RoomID {
			t.Fatalf("round %d preload %+v", round, preload)
		}
		seen[preload.ISRC] = true
		// Stale round number and duplicate READY do not start the round early.
		gr.command(host, readyMessage(gr.h.RoomID, round+1))
		gr.command(host, readyMessage(gr.h.RoomID, round))
		gr.command(host, readyMessage(gr.h.RoomID, round))
		gr.command(guest, readyMessage(gr.h.RoomID, round))
		b := gr.batch()
		started := wireOf(t, b, message.TypeRoundStarted).Message.Payload.(message.RoundStartPayload)
		if started.RoundNumber != round || started.TotalRounds != music.TracksPerGame {
			t.Fatalf("%+v", started)
		}
		gr.command(guest, answerMessage(gr.h.RoomID, "wrong"))
		if w := wireMessages(gr.batch()); len(w) != 1 || w[0].Type != message.TypeWrongAnswer {
			t.Fatalf("%+v", w)
		}
		gr.command(host, answerMessage(gr.h.RoomID, answers[preload.ISRC]))
		b = gr.batch()
		result := wireOf(t, b, message.TypeRoundResult).Message.Payload.(message.RoundResultPayload)
		if result.Winner != host.PlayerID || result.Scores[host.PlayerID] != round {
			t.Fatalf("%+v", result)
		}
		if round < music.TracksPerGame {
			preload = wireOf(t, b, message.TypePreloadSong).Message.Payload.(message.PreloadSongPayload)
			continue
		}
		over := wireOf(t, b, message.TypeGameOver).Message.Payload.(message.GameOverPayload)
		if over.Winner != host.PlayerID || hasWire(b, message.TypePreloadSong) || b.Snapshot.Accepting {
			t.Fatalf("game over: %+v", b)
		}
	}
	if gr.clock.active() != 0 {
		t.Fatal("timer left armed after GAME_OVER")
	}
	// Result: no restart, no admission, late game commands ignored.
	gr.command(host, startMessage(gr.h.RoomID))
	if code := failureCode(t, gr.batch(), host); code != message.InvalidState {
		t.Fatal(code)
	}
	gr.command(host, answerMessage(gr.h.RoomID, "late"))
	if err := gr.g.Register(context.Background(), sc.RegisterCommand{Token: 53, RoomID: gr.h.RoomID, Member: testMember(9)}); err != nil {
		t.Fatal(err)
	}
	b := gr.batch()
	if r, ok := b.Events[0].(sc.RegistrationResult); !ok || r.Rejection != sc.AdmissionStarted {
		t.Fatalf("result admission or late answer: %+v", b)
	}
	// Each member leaves individually; the last departure closes the Room.
	if b := gr.leave(guest); hasWire(b, message.TypeGameAborted) || b.Snapshot.PlayerCount != 1 {
		t.Fatalf("%+v", b)
	}
	gr.leave(host)
	awaitDone(t, gr.h.Done)
	if _, ok := gr.batch().Events[0].(sc.Closed); !ok {
		t.Fatal("missing Closed")
	}
}

func TestReadyReevaluatedOnDeparture(t *testing.T) {
	gr := newGameRoom(t, 3)
	gr.start()
	gr.command(gr.members[0], readyMessage(gr.h.RoomID, 1))
	gr.command(gr.members[1], readyMessage(gr.h.RoomID, 1))
	b := gr.leave(gr.members[2])
	wires := wireMessages(b)
	if len(wires) != 2 || wires[0].Type != message.TypePlayerLeft || wires[1].Type != message.TypeRoundStarted {
		t.Fatalf("%+v", wires)
	}
	// The round already started: a further READY cannot start it twice.
	gr.command(gr.members[0], readyMessage(gr.h.RoomID, 1))
	gr.command(gr.members[0], answerMessage(gr.h.RoomID, "wrong"))
	if w := wireMessages(gr.batch()); len(w) != 1 || w[0].Type != message.TypeWrongAnswer {
		t.Fatalf("%+v", w)
	}
}

func TestDepartureWhilePlaying(t *testing.T) {
	t.Run("host succession keeps game running", func(t *testing.T) {
		gr := newGameRoom(t, 3)
		gr.start()
		b := gr.leave(gr.members[0])
		if hasWire(b, message.TypeGameAborted) || wireOf(t, b, message.TypeHostChanged).Message.Payload.(message.HostChangedPayload).PlayerID != gr.members[1].PlayerID {
			t.Fatalf("%+v", b)
		}
		gr.members = gr.members[1:]
		gr.readyAll(1)
	})
	t.Run("one remains: GAME_ABORTED insufficient players", func(t *testing.T) {
		gr := newGameRoom(t, 2)
		gr.start()
		gr.readyAll(1)
		b := gr.leave(gr.members[0])
		abort := wireOf(t, b, message.TypeGameAborted)
		if p := abort.Message.Payload.(message.GameAbortedPayload); p.Reason != message.InsufficientPlayers || len(abort.Recipients) != 1 || abort.Recipients[0] != gr.members[1] || b.Snapshot.Accepting {
			t.Fatalf("%+v", b)
		}
		if gr.clock.active() != 0 || gr.clock.Advance(time.Minute) != 0 {
			t.Fatal("round timer survived abort")
		}
		// Result state for the remaining new host: no restart.
		gr.command(gr.members[1], startMessage(gr.h.RoomID))
		if code := failureCode(t, gr.batch(), gr.members[1]); code != message.InvalidState {
			t.Fatal(code)
		}
		gr.leave(gr.members[1])
		awaitDone(t, gr.h.Done)
	})
	t.Run("zero remain closes room and timers", func(t *testing.T) {
		gr := newGameRoom(t, 2)
		gr.start()
		gr.leave(gr.members[0])
		gr.leave(gr.members[1])
		awaitDone(t, gr.h.Done)
		if gr.clock.active() != 0 {
			t.Fatal("timer survived close")
		}
	})
}

func TestPreloadTimeoutAbortsWithControlledClock(t *testing.T) {
	gr := newGameRoom(t, 2)
	gr.start()
	gr.command(gr.members[0], readyMessage(gr.h.RoomID, 1))
	if fired := gr.clock.Advance(sc.PreloadTimeout - time.Millisecond); fired != 0 {
		t.Fatal("preload deadline fired early")
	}
	gr.clock.Advance(time.Millisecond)
	b := gr.batch()
	abort := wireOf(t, b, message.TypeGameAborted)
	if abort.Message.Payload.(message.GameAbortedPayload).Reason != message.AbortInternalError || len(abort.Recipients) != 2 || b.Snapshot.Accepting {
		t.Fatalf("%+v", b)
	}
	if gr.clock.active() != 0 {
		t.Fatal("timer left armed after abort")
	}
	// The late READY of the unresponsive member is ignored in result.
	gr.command(gr.members[1], readyMessage(gr.h.RoomID, 1))
	gr.command(gr.members[0], startMessage(gr.h.RoomID))
	if code := failureCode(t, gr.batch(), gr.members[0]); code != message.InvalidState {
		t.Fatal(code)
	}
}

func TestRoundCountdownAndTimeout(t *testing.T) {
	gr := newGameRoom(t, 2)
	gr.start()
	gr.readyAll(1)
	for remaining := RoundSeconds - 1; remaining >= 1; remaining-- {
		if gr.clock.Advance(time.Second) != 1 {
			t.Fatal("tick not armed")
		}
		w := wireMessages(gr.batch())
		if len(w) != 1 || w[0].Payload.(message.CountDownPayload).Remaining != remaining {
			t.Fatalf("countdown %d: %+v", remaining, w)
		}
	}
	gr.clock.Advance(time.Second)
	b := gr.batch()
	if r := wireOf(t, b, message.TypeRoundResult).Message.Payload.(message.RoundResultPayload); r.Winner != "" {
		t.Fatalf("%+v", r)
	}
	if p := wireOf(t, b, message.TypePreloadSong).Message.Payload.(message.PreloadSongPayload); p.RoundNumber != 2 {
		t.Fatal(p)
	}
	// Only the fresh round-2 preload guard is armed; round 1 cannot fire again.
	if gr.clock.active() != 1 {
		t.Fatalf("active timers %d", gr.clock.active())
	}
	gr.command(gr.members[0], answerMessage(gr.h.RoomID, "late"))
	gr.readyAll(2)
}

func TestCloseDuringPlayingStopsTimer(t *testing.T) {
	gr := newGameRoom(t, 2)
	gr.start()
	gr.readyAll(1)
	done, err := gr.g.Close(context.Background(), sc.CloseCommand{RoomID: gr.h.RoomID})
	if err != nil {
		t.Fatal(err)
	}
	awaitDone(t, done)
	if gr.clock.active() != 0 {
		t.Fatal("timer survived Done")
	}
}
