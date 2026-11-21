package session

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	message "temp-ws/internal/message/v1"
	music "temp-ws/internal/music/v1"
	sc "temp-ws/internal/sessioncontract"
)

type memoryTransport struct {
	messages []message.Message
	stopped  bool
	fail     bool
}

func (m *memoryTransport) Send(msg message.Message) error {
	if m.fail {
		return sc.ErrBackpressure
	}
	m.messages = append(m.messages, msg)
	return nil
}
func (m *memoryTransport) Stop() { m.stopped = true }

type memoryGateway struct {
	removed []sc.RemoveCommand
	closed  []sc.CloseCommand
	err     error
	done    chan struct{}
}

func (*memoryGateway) CreateInitialPair(context.Context, sc.CreateInitialPairCommand) (sc.RoomHandle, error) {
	return sc.RoomHandle{}, nil
}
func (*memoryGateway) Register(context.Context, sc.RegisterCommand) error { return nil }
func (g *memoryGateway) Remove(_ context.Context, c sc.RemoveCommand) error {
	if g.err != nil {
		return g.err
	}
	g.removed = append(g.removed, c)
	return nil
}
func (*memoryGateway) Publish(context.Context, sc.PublishCommand) error { return nil }
func (g *memoryGateway) Close(_ context.Context, c sc.CloseCommand) (<-chan struct{}, error) {
	g.closed = append(g.closed, c)
	return g.done, nil
}

type memoryCommander struct {
	calls  int
	member sc.Member
}

func (c *memoryCommander) Command(_ context.Context, _ string, m sc.Member, _ message.Message) error {
	c.calls++
	c.member = m
	return nil
}
func setupRegistry(t *testing.T) (*Registry, *memoryGateway, *memoryTransport, Connection, sc.Member, sc.EventBatch) {
	t.Helper()
	g := &memoryGateway{done: make(chan struct{})}
	r := NewRegistry(context.Background(), g, nil, nil)
	out := &memoryTransport{}
	c := r.Connect(uuid.NewString(), out)
	m := sc.Member{PlayerID: c.PlayerID, RequestID: uuid.NewString(), ArrivalSequence: 1}
	roomID := uuid.NewString()
	b := sc.EventBatch{RoomID: roomID, Revision: 1, Snapshot: sc.RoomSnapshot{RoomID: roomID, Revision: 1, Category: music.Category{Decade: "all", ArtistType: "all", Gender: "all", Genre: "all"}, CatalogVersion: "test-v1", MaxPlayers: 8, PlayerCount: 1}, Events: []sc.Event{sc.RegistrationResult{Token: 1, Member: m, Admission: sc.Admission{HostID: m.PlayerID, Players: []string{m.PlayerID}, State: sc.RoomWaiting}}}}
	return r, g, out, c, m, b
}
func leaveMessage(roomID string) message.Message {
	return message.Message{Type: message.TypeLeaveRoom, Payload: &message.LeaveRoomPayload{RoomID: roomID}}
}
func TestRegistryLeaveWaitsForScopedCompletionAndCoalesces(t *testing.T) {
	r, g, out, c, m, b := setupRegistry(t)
	r.Apply(b, []sc.Member{m})
	out.messages = nil
	for i := 0; i < 2; i++ {
		if err := r.Route(c.ID, leaveMessage(b.RoomID)); err != nil {
			t.Fatal(err)
		}
	}
	if len(g.removed) != 1 || len(out.messages) != 0 {
		t.Fatal("duplicate cleanup or early ROOM_LEFT")
	}
	cmd := g.removed[0]
	stale := sc.EventBatch{RoomID: b.RoomID, Revision: 2, Events: []sc.Event{sc.RemovalResult{Token: cmd.Token, PlayerID: m.PlayerID, RequestID: uuid.NewString()}}}
	r.Apply(stale, nil)
	if len(out.messages) != 0 || r.connections[c.ID].roomID == "" {
		t.Fatal("wrong request completed leave")
	}
	stale.Revision = 3
	stale.Events = []sc.Event{sc.RemovalResult{Token: cmd.Token, PlayerID: m.PlayerID, RequestID: m.RequestID}}
	r.Apply(stale, nil)
	if len(out.messages) != 2 || out.messages[0].Type != message.TypeRoomLeft || out.stopped || r.connections[c.ID].roomID != "" {
		t.Fatal("leave did not preserve connection")
	}
	if err := r.Route(c.ID, leaveMessage(b.RoomID)); err != nil {
		t.Fatal(err)
	}
	if len(out.messages) != 3 {
		t.Fatal("absent leave not acknowledged")
	}
}
func TestRegistryLateAdmissionsAndDisconnect(t *testing.T) {
	for _, scenario := range []string{"cancelled", "disconnected", "send failure"} {
		t.Run(scenario, func(t *testing.T) {
			r, _, out, c, m, b := setupRegistry(t)
			approved := []sc.Member{m}
			switch scenario {
			case "cancelled":
				approved = nil
			case "disconnected":
				r.Disconnect(c.ID)
			case "send failure":
				out.fail = true
			}
			cleanup := r.Apply(b, approved)
			if len(cleanup) != 1 || cleanup[0].RoomID != b.RoomID || cleanup[0].RequestID != m.RequestID || len(out.messages) != 0 {
				t.Fatalf("late admission: %+v", cleanup)
			}
			if len(r.Apply(b, approved)) != 0 {
				t.Fatal("duplicate batch repeated cleanup")
			}
		})
	}
}
func TestRegistryWelcomeBeforeWireAndMembershipFilter(t *testing.T) {
	r, _, out, c, m, b := setupRegistry(t)
	b.Events = append(b.Events, sc.WireEvent{Recipients: []sc.Member{m}, Message: serverMessage(message.TypePlayerJoined, message.PlayerJoinedPayload{RoomID: b.RoomID, PlayerID: m.PlayerID})})
	r.Apply(b, []sc.Member{m})
	if len(out.messages) != 2 || out.messages[0].Type != message.TypeWelcome || out.messages[1].Type != message.TypePlayerJoined {
		t.Fatal("WELCOME ordering")
	}
	old := m
	old.RequestID = uuid.NewString()
	b.Revision = 2
	b.Events = []sc.Event{sc.WireEvent{Recipients: []sc.Member{old}, Message: serverMessage(message.TypePlayerLeft, message.PlayerLeftPayload{RoomID: b.RoomID, PlayerID: old.PlayerID})}}
	r.Apply(b, nil)
	if len(out.messages) != 2 {
		t.Fatal("stale request wire escaped")
	}
	commands := &memoryCommander{}
	r.commands = commands
	if err := r.Route(c.ID, message.Message{Type: message.TypeStartGame, Payload: &message.StartGamePayload{RoomID: uuid.NewString()}}); err != nil {
		t.Fatal(err)
	}
	if commands.calls != 0 {
		t.Fatal("old room command routed")
	}
	if err := r.Route(c.ID, message.Message{Type: message.TypeStartGame, Payload: &message.StartGamePayload{RoomID: b.RoomID}}); err != nil {
		t.Fatal(err)
	}
	if commands.calls != 1 || commands.member != m {
		t.Fatal("actual membership not used")
	}
}
func TestRegistryTerminalRoomIgnoresLateEvents(t *testing.T) {
	r, _, out, _, m, b := setupRegistry(t)
	r.Apply(b, []sc.Member{m})
	r.Apply(sc.EventBatch{RoomID: b.RoomID, Revision: 2, Events: []sc.Event{sc.Closed{}}}, nil)
	if !out.stopped {
		t.Fatal("closed room did not stop member")
	}
	b.Revision = 3
	if cleanup := r.Apply(b, []sc.Member{m}); len(cleanup) != 0 || len(out.messages) != 1 {
		t.Fatal("terminal room resurrected")
	}
	r.ForgetRoom(b.RoomID)
	if len(r.revisions) != 0 || len(r.terminal) != 0 {
		t.Fatal("drained terminal retained")
	}
}
func TestRegistryCleanupFailureIsNotAcknowledged(t *testing.T) {
	r, g, out, c, m, b := setupRegistry(t)
	r.Apply(b, []sc.Member{m})
	g.err = sc.ErrBackpressure
	if err := r.Route(c.ID, leaveMessage(b.RoomID)); !errors.Is(err, sc.ErrBackpressure) {
		t.Fatal(err)
	}
	if len(g.closed) != 1 || len(out.messages) != 1 {
		t.Fatal("failed cleanup acknowledged")
	}
	// The stream owner uses Done/drain to report failure if Closed could not enqueue.
	close(g.done)
	r.Terminated(b.RoomID)
	if !out.stopped {
		t.Fatal("failed sink left connection alive")
	}
}
func TestRegistryDisconnectIsIdempotentAndScoped(t *testing.T) {
	r, g, _, c, m, b := setupRegistry(t)
	r.Apply(b, []sc.Member{m})
	r.Disconnect(c.ID)
	r.Disconnect(c.ID)
	if len(g.removed) != 1 || g.removed[0].RoomID != b.RoomID || g.removed[0].RequestID != m.RequestID || len(r.connections) != 0 || len(r.players) != 0 {
		t.Fatal("disconnect cleanup")
	}
}
func TestEventQueueAtomicBackpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	q := NewEventQueue(ctx, 1)
	batch := sc.EventBatch{RoomID: uuid.NewString(), Events: []sc.Event{sc.RemovalResult{}, sc.Closed{}}}
	if err := q.TryPublish(batch); err != nil {
		t.Fatal(err)
	}
	if err := q.TryPublish(sc.EventBatch{}); !errors.Is(err, sc.ErrBackpressure) {
		t.Fatal(err)
	}
	got := <-q.Batches()
	if got.RoomID != batch.RoomID || len(got.Events) != 2 {
		t.Fatal("partial batch")
	}
	cancel()
	if err := q.TryPublish(batch); !errors.Is(err, sc.ErrClosed) {
		t.Fatal(err)
	}
}
