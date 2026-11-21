package matchmaking

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	message "temp-ws/internal/message/v1"
	"temp-ws/internal/music"
	musicv1 "temp-ws/internal/music/v1"
	"temp-ws/internal/room"
	"temp-ws/internal/session"
	sc "temp-ws/internal/sessioncontract"
)

var anyCategory = musicv1.Category{Decade: "any", ArtistType: "any", Gender: "any", Genre: "any"}
var ballad = musicv1.Category{Decade: "2010s", ArtistType: "any", Gender: "any", Genre: "korean_ballad"}
var pop = musicv1.Category{Decade: "2020s", ArtistType: "group", Gender: "male", Genre: "synthetic_pop"}

type transport struct {
	messages    []message.Message
	failWelcome bool
	stopped     bool
}

func (t *transport) Send(msg message.Message) error {
	if t.failWelcome && msg.Type == message.TypeWelcome {
		return sc.ErrBackpressure
	}
	t.messages = append(t.messages, msg)
	return nil
}
func (t *transport) Stop()                 { t.stopped = true }
func (t *transport) last() message.Message { return t.messages[len(t.messages)-1] }

type fakeGateway struct {
	pairs          []sc.CreateInitialPairCommand
	handles        []sc.RoomHandle
	registrations  []sc.RegisterCommand
	removals       []sc.RemoveCommand
	publications   []sc.PublishCommand
	closes         []sc.CloseCommand
	rejectRegister map[string]error
	pairError      error
}

func (g *fakeGateway) CreateInitialPair(_ context.Context, c sc.CreateInitialPairCommand) (sc.RoomHandle, error) {
	g.pairs = append(g.pairs, c)
	if g.pairError != nil {
		return sc.RoomHandle{}, g.pairError
	}
	h := sc.RoomHandle{RoomID: uuid.NewString(), CreationSequence: sc.Sequence(len(g.handles) + 1), Done: make(chan struct{})}
	g.handles = append(g.handles, h)
	return h, nil
}
func (g *fakeGateway) Register(_ context.Context, c sc.RegisterCommand) error {
	g.registrations = append(g.registrations, c)
	return g.rejectRegister[c.RoomID]
}
func (g *fakeGateway) Remove(_ context.Context, c sc.RemoveCommand) error {
	g.removals = append(g.removals, c)
	return nil
}
func (g *fakeGateway) Publish(_ context.Context, c sc.PublishCommand) error {
	g.publications = append(g.publications, c)
	return nil
}
func (g *fakeGateway) Close(_ context.Context, c sc.CloseCommand) (<-chan struct{}, error) {
	g.closes = append(g.closes, c)
	return make(chan struct{}), nil
}

func catalog(t *testing.T) *music.CatalogStore {
	t.Helper()
	base := "../../docs/specs/random-matchmaking/fixtures"
	c, err := os.Open(filepath.Join(base, "catalog.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	s, err := os.Open(filepath.Join(base, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	store, err := music.LoadCatalog(c, s)
	if err != nil {
		t.Fatal(err)
	}
	return store
}
func newMatcher(t *testing.T) (*Matchmaker, *fakeGateway) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	g := &fakeGateway{rejectRegister: make(map[string]error)}
	return New(ctx, g, catalog(t), nil), g
}
func connect(m *Matchmaker) (session.Connection, *transport) {
	out := &transport{}
	return m.registry.Connect(uuid.NewString(), out), out
}
func submit(m *Matchmaker, c session.Connection, category musicv1.Category) *request {
	m.accept(input{connection: c, payload: message.MatchRequestPayload{RequestID: uuid.NewString(), CatalogVersion: "test-v1", Category: category}})
	m.schedule()
	return m.requests[c.ID]
}
func candidate(m *Matchmaker, seq sc.Sequence, category musicv1.Category) string {
	id := uuid.NewString()
	m.rooms[id] = &trackedRoom{handle: sc.RoomHandle{RoomID: id, CreationSequence: seq, Done: make(chan struct{})}, public: true, cleanup: make(map[sc.OperationToken]sc.RemoveCommand), snapshot: sc.RoomSnapshot{RoomID: id, CreationSequence: seq, Category: category, CatalogVersion: "test-v1", PlayerCount: 2, MaxPlayers: 8, Accepting: true, Revision: 1}}
	return id
}
func resolved(m *Matchmaker, c musicv1.Category) musicv1.Category {
	r, _, _ := m.catalog.ResolvePair("test-v1", c, c)
	return r
}
func pairBatch(g *fakeGateway, index int, rejection sc.AdmissionRejection) sc.EventBatch {
	cmd, h := g.pairs[index], g.handles[index]
	players := []string{cmd.Members[0].PlayerID, cmd.Members[1].PlayerID}
	return sc.EventBatch{RoomID: h.RoomID, Revision: 1, Snapshot: sc.RoomSnapshot{RoomID: h.RoomID, CreationSequence: h.CreationSequence, Category: cmd.Category, CatalogVersion: cmd.CatalogVersion, PlayerCount: 2, MaxPlayers: 8, Revision: 1}, Events: []sc.Event{sc.InitialPairResult{Token: cmd.Token, Members: cmd.Members, Rejection: rejection, Admission: sc.Admission{HostID: players[0], Players: players, State: sc.RoomWaiting}}}}
}
func advance(m *Matchmaker, id string, events ...sc.Event) sc.EventBatch {
	s := m.rooms[id].snapshot
	s.Revision++
	return sc.EventBatch{RoomID: id, Revision: s.Revision, Snapshot: s, Events: events}
}

func TestWaitingValidationAndOldestCompatiblePair(t *testing.T) {
	m, g := newMatcher(t)
	a, oa := connect(m)
	b, _ := connect(m)
	c, _ := connect(m)
	ra := submit(m, a, pop)
	rb := submit(m, b, ballad)
	if len(g.pairs) != 0 || oa.last().Type != message.TypeMatchWaiting {
		t.Fatal("incompatible requests must wait without a room")
	}
	rc := submit(m, c, anyCategory)
	if len(g.pairs) != 1 || g.pairs[0].Members != [2]sc.Member{ra.member, rc.member} || rb.state != sc.RequestWaiting {
		t.Fatal("did not choose earliest compatible waiter")
	}
	if ra.member.ArrivalSequence >= rb.member.ArrivalSequence || rb.member.ArrivalSequence >= rc.member.ArrivalSequence {
		t.Fatal("nonmonotonic arrival")
	}
	d, od := connect(m)
	m.accept(input{connection: d, payload: message.MatchRequestPayload{RequestID: uuid.NewString(), CatalogVersion: "unknown", Category: anyCategory}})
	if od.last().Payload.(message.MatchFailedPayload).Code != message.InvalidCriteria || m.requests[d.ID] != nil {
		t.Fatal("invalid version admitted")
	}
}

func TestExistingRoomsPrecedePairAndUseCreationOrder(t *testing.T) {
	m, g := newMatcher(t)
	late := candidate(m, 30, resolved(m, anyCategory))
	early := candidate(m, 10, resolved(m, anyCategory))
	candidate(m, 1, resolved(m, pop))
	a, _ := connect(m)
	r := submit(m, a, anyCategory)
	// any accepts the oldest pop room too; make it unavailable and retry.
	cmd := g.registrations[0]
	if cmd.RoomID == early || cmd.RoomID == late {
		t.Fatal("oldest compatible room was skipped")
	}
	batch := advance(m, cmd.RoomID, sc.RegistrationResult{Token: cmd.Token, Member: r.member, Rejection: sc.AdmissionStarted})
	batch.Snapshot.Accepting = false
	m.apply(batch)
	m.schedule()
	if len(g.registrations) != 2 || g.registrations[1].RoomID != early || len(g.pairs) != 0 {
		t.Fatal("existing-room priority/order lost")
	}
	m.schedule()
	m.schedule()
	if len(g.registrations) != 2 {
		t.Fatal("multiple operations for assigning request")
	}
}

func TestOneActiveRequestAndDuplicateAdmission(t *testing.T) {
	m, g := newMatcher(t)
	a, out := connect(m)
	b, _ := connect(m)
	r := submit(m, a, anyCategory)
	seq := r.member.ArrivalSequence
	m.accept(input{connection: a, payload: r.payload})
	m.schedule()
	if r.member.ArrivalSequence != seq || len(g.pairs) != 0 {
		t.Fatal("duplicate changed sequence")
	}
	newer := message.MatchRequestPayload{RequestID: uuid.NewString(), CatalogVersion: "test-v1", Category: anyCategory}
	m.accept(input{connection: a, payload: newer})
	if out.last().Payload.(message.MatchFailedPayload).Code != message.AlreadyMatching {
		t.Fatal("second active request accepted")
	}
	submit(m, b, anyCategory)
	m.accept(input{connection: a, payload: r.payload})
	m.schedule()
	if len(g.pairs) != 1 {
		t.Fatal("duplicate pending pair")
	}
	m.apply(pairBatch(g, 0, sc.AdmissionAccepted))
	m.accept(input{connection: a, payload: newer})
	if out.last().Payload.(message.MatchFailedPayload).Code != message.AlreadyInRoom {
		t.Fatal("matched request replaced")
	}
	m.accept(input{connection: a, payload: r.payload})
	if out.last().Type != message.TypeWelcome {
		t.Fatal("matched duplicate lost welcome")
	}
}

func TestPrivatePairWaitsForCleanupBeforePublication(t *testing.T) {
	for _, failedCount := range []int{1, 2} {
		t.Run(string(rune('0'+failedCount)), func(t *testing.T) {
			m, g := newMatcher(t)
			a, oa := connect(m)
			b, ob := connect(m)
			submit(m, a, anyCategory)
			submit(m, b, anyCategory)
			oa.failWelcome = true
			ob.failWelcome = failedCount == 2
			batch := pairBatch(g, 0, sc.AdmissionAccepted)
			m.apply(batch)
			if len(g.publications) != 0 || len(g.removals) != failedCount || m.rooms[batch.RoomID].public {
				t.Fatal("published before scoped cleanup")
			}
			for _, cmd := range g.removals {
				next := advance(m, batch.RoomID, sc.RemovalResult{Token: cmd.Token, PlayerID: cmd.PlayerID, RequestID: cmd.RequestID})
				next.Snapshot.PlayerCount--
				m.apply(next)
			}
			if failedCount == 1 && len(g.publications) != 1 {
				t.Fatal("survivor room not published")
			}
			if failedCount == 2 && (len(g.publications) != 0 || len(g.closes) == 0) {
				t.Fatal("empty private room published")
			}
		})
	}
}

func TestDisconnectDuringInitialRegistration(t *testing.T) {
	m, g := newMatcher(t)
	a, oa := connect(m)
	b, _ := connect(m)
	submit(m, a, anyCategory)
	submit(m, b, anyCategory)
	m.accept(input{connection: a, disconnected: true})
	m.apply(pairBatch(g, 0, sc.AdmissionAccepted))
	if oa.last().Type == message.TypeWelcome || len(g.removals) != 1 || len(g.publications) != 0 {
		t.Fatal("disconnected member welcomed or cleanup bypassed")
	}
}

func TestRegistrationWelcomePrecedesWireAndRemovalAllowsNewRequest(t *testing.T) {
	m, g := newMatcher(t)
	id := candidate(m, 1, resolved(m, anyCategory))
	a, out := connect(m)
	r := submit(m, a, anyCategory)
	cmd := g.registrations[0]
	batch := advance(m, id, sc.RegistrationResult{Token: cmd.Token, Member: r.member, Admission: sc.Admission{HostID: r.member.PlayerID, Players: []string{r.member.PlayerID}, State: sc.RoomWaiting}}, sc.WireEvent{Recipients: []sc.Member{r.member}, Message: message.Message{Type: message.TypePlayerJoined}})
	m.apply(batch)
	n := len(out.messages)
	if out.messages[n-2].Type != message.TypeWelcome || out.messages[n-1].Type != message.TypePlayerJoined {
		t.Fatal("wire overtook WELCOME")
	}
	m.apply(advance(m, id, sc.RemovalResult{Token: session.NextOperationToken(), PlayerID: r.member.PlayerID, RequestID: r.member.RequestID}))
	replacement := submit(m, a, anyCategory)
	if replacement == r || replacement.member.ArrivalSequence <= r.member.ArrivalSequence {
		t.Fatal("leave did not permit fresh request")
	}
}

func TestRejectionRetryAndStaleSnapshot(t *testing.T) {
	for _, reason := range []sc.AdmissionRejection{sc.AdmissionFull, sc.AdmissionStarted, sc.AdmissionClosed} {
		t.Run(string(reason), func(t *testing.T) {
			m, g := newMatcher(t)
			first := candidate(m, 1, resolved(m, anyCategory))
			second := candidate(m, 2, resolved(m, anyCategory))
			a, _ := connect(m)
			r := submit(m, a, anyCategory)
			seq := r.member.ArrivalSequence
			cmd := g.registrations[0]
			rejected := advance(m, first, sc.RegistrationResult{Token: cmd.Token, Member: r.member, Rejection: reason})
			// Even an apparently accepting rejection snapshot must be suppressed at
			// exactly this revision; retries must not spin waiting for another actor.
			m.apply(rejected)
			m.schedule()
			if len(g.registrations) != 2 || g.registrations[1].RoomID != second || r.member.ArrivalSequence != seq {
				t.Fatal("rejection did not try next room with original order")
			}
			cmd = g.registrations[1]
			m.apply(advance(m, second, sc.RegistrationResult{Token: cmd.Token, Member: r.member, Rejection: reason}))
			m.schedule()
			for range 20 {
				m.schedule()
			}
			if len(g.registrations) != 2 || r.state != sc.RequestWaiting {
				t.Fatal("same-revision busy loop")
			}
			stale := rejected
			stale.Revision = 1
			stale.Snapshot.Revision = 1
			m.apply(stale)
			m.schedule()
			if len(g.registrations) != 2 {
				t.Fatal("old snapshot revived candidate")
			}
			m.apply(advance(m, first))
			m.schedule()
			if len(g.registrations) != 3 || g.registrations[2].RoomID != first {
				t.Fatal("new revision did not allow retry")
			}
		})
	}
}

func TestInitialFailurePreservesOrderWithoutBusyLoop(t *testing.T) {
	m, g := newMatcher(t)
	a, _ := connect(m)
	b, _ := connect(m)
	ra := submit(m, a, anyCategory)
	rb := submit(m, b, anyCategory)
	seq := ra.member.ArrivalSequence
	batch := pairBatch(g, 0, sc.AdmissionInternalError)
	batch.Snapshot.PlayerCount = 0
	m.apply(batch)
	m.schedule()
	m.apply(advance(m, batch.RoomID, sc.Closed{}))
	m.schedule()
	for range 20 {
		m.schedule()
	}
	if len(g.pairs) != 1 || ra.state != sc.RequestWaiting || rb.state != sc.RequestWaiting || ra.member.ArrivalSequence != seq {
		t.Fatal("failed pair spun or lost queue order")
	}
	c, _ := connect(m)
	submit(m, c, pop)
	if len(g.pairs) != 2 || g.pairs[1].Members != [2]sc.Member{ra.member, rb.member} {
		t.Fatal("external input did not resume oldest pair")
	}
}

func TestEnqueueRejectionTriesOtherCandidates(t *testing.T) {
	m, g := newMatcher(t)
	first := candidate(m, 1, resolved(m, anyCategory))
	second := candidate(m, 2, resolved(m, anyCategory))
	g.rejectRegister[first] = sc.ErrClosed
	a, _ := connect(m)
	submit(m, a, anyCategory)
	if len(g.registrations) != 2 || g.registrations[1].RoomID != second {
		t.Fatal("enqueue rejection stalled search")
	}
}

func nextBatch(t *testing.T, ctx context.Context, q *session.EventQueue) sc.EventBatch {
	t.Helper()
	select {
	case b := <-q.Batches():
		return b
	case <-ctx.Done():
		t.Fatal("missing room event")
		return sc.EventBatch{}
	}
}
func realMatcher(t *testing.T) (*Matchmaker, *room.Gateway, *session.EventQueue, sc.Member, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	q := session.NewEventQueue(ctx, 256)
	g := room.NewGateway(ctx, q)
	m := New(ctx, g, catalog(t), q.Batches())
	t.Cleanup(func() { cancel(); m.shutdown() })
	a, _ := connect(m)
	b, _ := connect(m)
	ra := submit(m, a, anyCategory)
	submit(m, b, anyCategory)
	m.apply(nextBatch(t, ctx, q))
	m.apply(nextBatch(t, ctx, q))
	return m, g, q, ra.member, ra.roomID
}
func TestRealRoomNinthRequestWaitsAndNoDoubleRegistration(t *testing.T) {
	m, _, q, _, id := realMatcher(t)
	var last *request
	for range 7 {
		c, _ := connect(m)
		last = submit(m, c, anyCategory)
	}
	for range 7 {
		m.apply(nextBatch(t, m.ctx, q))
		m.schedule()
	}
	if m.rooms[id].snapshot.PlayerCount != 8 || last.state != sc.RequestWaiting {
		t.Fatal("capacity exceeded or ninth request lost")
	}
	for range 10 {
		m.schedule()
	}
	select {
	case b := <-q.Batches():
		t.Fatalf("unexpected extra registration: %+v", b)
	default:
	}
}
func TestRealRoomStartRegistrationOrdering(t *testing.T) {
	for _, startFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "start first", false: "register first"}[startFirst], func(t *testing.T) {
			m, g, q, host, id := realMatcher(t)
			c, out := connect(m)
			if startFirst {
				if err := g.BeginStart(m.ctx, id, host); err != nil {
					t.Fatal(err)
				}
			}
			r := submit(m, c, anyCategory)
			if !startFirst {
				if err := g.BeginStart(m.ctx, id, host); err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				m.apply(nextBatch(t, m.ctx, q))
				m.schedule()
			}
			if m.rooms[id].snapshot.Accepting {
				t.Fatal("starting room accepting")
			}
			if startFirst && (r.state != sc.RequestWaiting || out.last().Type != message.TypeMatchWaiting) {
				t.Fatal("start loser should keep waiting without failure")
			}
			if !startFirst && r.state != sc.RequestMatched {
				t.Fatal("registration before start excluded")
			}
		})
	}
}

func TestStaleOperationTokenCannotWelcomeOrRemoveCurrentRequest(t *testing.T) {
	m, g := newMatcher(t)
	id := candidate(m, 1, resolved(m, anyCategory))
	c, out := connect(m)
	r := submit(m, c, anyCategory)
	cmd := g.registrations[0]
	result := sc.RegistrationResult{Token: cmd.Token + 100, Member: r.member, Admission: sc.Admission{HostID: r.member.PlayerID, Players: []string{r.member.PlayerID}, State: sc.RoomWaiting}}
	m.apply(advance(m, id, result))
	if r.state != sc.RequestAssigning || out.last().Type != message.TypeMatchWaiting || len(g.removals) != 0 {
		t.Fatal("stale token changed pending membership")
	}
	result.Token = cmd.Token
	m.apply(advance(m, id, result))
	n := len(out.messages)
	m.apply(advance(m, id, result))
	if r.state != sc.RequestMatched || len(out.messages) != n || len(g.removals) != 0 {
		t.Fatal("duplicate result changed admitted membership")
	}
}

func TestTerminalRoomDoesNotReenterCandidates(t *testing.T) {
	m, g := newMatcher(t)
	id := candidate(m, 1, resolved(m, anyCategory))
	m.apply(advance(m, id, sc.Closed{}))
	late := advance(m, id)
	late.Snapshot.Accepting = true
	m.apply(late)
	c, _ := connect(m)
	r := submit(m, c, anyCategory)
	if len(g.registrations) != 0 || r.state != sc.RequestWaiting {
		t.Fatal("late snapshot revived terminal room")
	}
	m.retire(id)
	m.apply(late)
	m.schedule()
	if _, exists := m.rooms[id]; exists {
		t.Fatal("drained room resurrected")
	}
}

func TestPendingInitialRoomDoesNotBlockIndependentPair(t *testing.T) {
	m, g := newMatcher(t)
	for range 2 {
		c, _ := connect(m)
		submit(m, c, ballad)
	}
	// Withhold the first completion; no Room result or publication is available.
	for range 2 {
		c, _ := connect(m)
		submit(m, c, pop)
	}
	if len(g.pairs) != 2 || len(g.publications) != 0 {
		t.Fatal("pending room blocked independent work or was published early")
	}
	m.apply(pairBatch(g, 1, sc.AdmissionAccepted))
	if len(g.publications) != 1 || g.publications[0].RoomID != g.handles[1].RoomID {
		t.Fatal("out-of-order rooms stalled completion")
	}
}

func TestPublishedRoomWinsOverCompatibleWaitingPair(t *testing.T) {
	m, g := newMatcher(t)
	for range 2 {
		c, _ := connect(m)
		m.accept(input{connection: c, payload: message.MatchRequestPayload{RequestID: uuid.NewString(), CatalogVersion: "test-v1", Category: anyCategory}})
	}
	id := candidate(m, 1, resolved(m, anyCategory))
	m.schedule()
	if len(g.pairs) != 0 || len(g.registrations) != 2 || g.registrations[0].RoomID != id || g.registrations[1].RoomID != id {
		t.Fatal("created a pair while an existing room could accept both waiters")
	}
	if g.registrations[0].Member.ArrivalSequence >= g.registrations[1].Member.ArrivalSequence {
		t.Fatal("waiting visitation order lost")
	}
}

func TestInitialEnqueueFailureDoesNotSpinOrConsumeRequest(t *testing.T) {
	m, g := newMatcher(t)
	g.pairError = sc.ErrBackpressure
	a, _ := connect(m)
	b, _ := connect(m)
	ra := submit(m, a, anyCategory)
	rb := submit(m, b, anyCategory)
	for range 20 {
		m.schedule()
	}
	if len(g.pairs) != 1 || len(m.rooms) != 0 || ra.state != sc.RequestWaiting || rb.state != sc.RequestWaiting {
		t.Fatal("failed enqueue lost requests or spun")
	}
}
