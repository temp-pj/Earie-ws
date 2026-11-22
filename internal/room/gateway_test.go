package room

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	message "temp-ws/internal/message/v1"
	music "temp-ws/internal/music/v1"
	sc "temp-ws/internal/sessioncontract"
)

type testSink struct{ batches chan sc.EventBatch }

func (s *testSink) TryPublish(b sc.EventBatch) error {
	select {
	case s.batches <- b:
		return nil
	default:
		return sc.ErrBackpressure
	}
}
func testMember(seq int) sc.Member {
	return sc.Member{PlayerID: uuid.NewString(), RequestID: uuid.NewString(), ArrivalSequence: sc.Sequence(seq)}
}
func pairCommand() sc.CreateInitialPairCommand {
	return sc.CreateInitialPairCommand{Token: 1, Category: music.Category{Decade: "all", ArtistType: "all", Gender: "all", Genre: "all"}, CatalogVersion: "test-v1", Members: [2]sc.Member{testMember(2), testMember(1)}}
}
func nextBatch(t *testing.T, sink *testSink) sc.EventBatch {
	t.Helper()
	select {
	case b := <-sink.batches:
		return b
	case <-time.After(3 * time.Second):
		t.Fatal("room batch deadline")
		return sc.EventBatch{}
	}
}
func awaitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Done deadline")
	}
}

// newTestGateway defaults to a lookup that stays pending until the test replies
// or the Room stops, so START_GAME ordering tests stay deterministic.
func newTestGateway(t *testing.T, options ...Option) (*Gateway, *testSink) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	sink := &testSink{make(chan sc.EventBatch, 256)}
	g := NewGateway(ctx, sink, append([]Option{WithSongs(newGateSongs())}, options...)...)
	t.Cleanup(func() {
		cancel()
		g.mu.Lock()
		rooms := make([]*lifecycleRoom, 0, len(g.rooms))
		for _, r := range g.rooms {
			rooms = append(rooms, r)
		}
		g.mu.Unlock()
		for _, r := range rooms {
			awaitDone(t, r.done)
		}
	})
	return g, sink
}
func createPair(t *testing.T, g *Gateway, s *testSink) (sc.RoomHandle, sc.CreateInitialPairCommand) {
	t.Helper()
	cmd := pairCommand()
	h, err := g.CreateInitialPair(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	b := nextBatch(t, s)
	result := b.Events[0].(sc.InitialPairResult)
	if result.Rejection != "" || len(result.Admission.Players) != 2 || result.Admission.HostID != cmd.Members[1].PlayerID || b.Snapshot.Accepting || b.Snapshot.PlayerCount != 2 || b.Revision != 1 {
		t.Fatalf("initial pair: %+v", b)
	}
	return h, cmd
}
func publishRoom(t *testing.T, g *Gateway, s *testSink, h sc.RoomHandle) {
	t.Helper()
	if err := g.Publish(context.Background(), sc.PublishCommand{Token: 2, RoomID: h.RoomID}); err != nil {
		t.Fatal(err)
	}
	if b := nextBatch(t, s); !b.Snapshot.Accepting {
		t.Fatalf("not accepting: %+v", b)
	}
}
func TestGatewayInitialPairAtomicAndPrivate(t *testing.T) {
	g, s := newTestGateway(t)
	h, cmd := createPair(t, g, s)
	// Cancellation of one member before publication leaves exactly one visible member.
	if err := g.Remove(context.Background(), sc.RemoveCommand{Token: 3, RoomID: h.RoomID, PlayerID: cmd.Members[1].PlayerID, RequestID: cmd.Members[1].RequestID}); err != nil {
		t.Fatal(err)
	}
	b := nextBatch(t, s)
	if b.Snapshot.PlayerCount != 1 || b.Snapshot.Accepting {
		t.Fatalf("private removal: %+v", b)
	}
	publishRoom(t, g, s, h)
	for _, event := range b.Events {
		if wire, ok := event.(sc.WireEvent); ok && wire.Message.Type == message.TypeHostChanged {
			if wire.Message.Payload.(message.HostChangedPayload).PlayerID != cmd.Members[0].PlayerID {
				t.Fatal("host order")
			}
		}
	}
}
func TestGatewayFailedPairHasNoPartialMembership(t *testing.T) {
	g, s := newTestGateway(t)
	cmd := pairCommand()
	cmd.Members[1].PlayerID = cmd.Members[0].PlayerID
	h, err := g.CreateInitialPair(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	b := nextBatch(t, s)
	if b.Events[0].(sc.InitialPairResult).Rejection == "" || b.Snapshot.PlayerCount != 0 {
		t.Fatalf("partial pair: %+v", b)
	}
	awaitDone(t, h.Done)
	closed := nextBatch(t, s)
	if _, ok := closed.Events[0].(sc.Closed); !ok {
		t.Fatal("missing terminal event")
	}
}
func TestGatewayScopedRemovalAndRevision(t *testing.T) {
	g, s := newTestGateway(t)
	h, cmd := createPair(t, g, s)
	publishRoom(t, g, s, h)
	original := cmd.Members[0]
	remove := sc.RemoveCommand{Token: 10, RoomID: h.RoomID, PlayerID: original.PlayerID, RequestID: original.RequestID}
	if err := g.Remove(context.Background(), remove); err != nil {
		t.Fatal(err)
	}
	b := nextBatch(t, s)
	replacement := original
	replacement.RequestID = uuid.NewString()
	replacement.ArrivalSequence = 3
	if err := g.Register(context.Background(), sc.RegisterCommand{Token: 11, RoomID: h.RoomID, Member: replacement}); err != nil {
		t.Fatal(err)
	}
	joined := nextBatch(t, s)
	if joined.Revision <= b.Revision {
		t.Fatal("revision did not increase")
	}
	remove.Token = 12
	if err := g.Remove(context.Background(), remove); err != nil {
		t.Fatal(err)
	}
	stale := nextBatch(t, s)
	if stale.Snapshot.PlayerCount != 2 || len(stale.Events) != 1 || stale.Events[0].(sc.RemovalResult).Token != 12 {
		t.Fatalf("stale removal affected replacement: %+v", stale)
	}
}
func TestGatewayCapacityAndStartOrder(t *testing.T) {
	for _, startFirst := range []bool{false, true} {
		t.Run(map[bool]string{true: "start-first", false: "register-first"}[startFirst], func(t *testing.T) {
			g, s := newTestGateway(t)
			h, cmd := createPair(t, g, s)
			publishRoom(t, g, s, h)
			for i := 3; i <= 7; i++ {
				if err := g.Register(context.Background(), sc.RegisterCommand{Token: sc.OperationToken(i), RoomID: h.RoomID, Member: testMember(i)}); err != nil {
					t.Fatal(err)
				}
				nextBatch(t, s)
			}
			register := func() {
				if err := g.Register(context.Background(), sc.RegisterCommand{Token: 8, RoomID: h.RoomID, Member: testMember(8)}); err != nil {
					t.Fatal(err)
				}
			}
			start := func() {
				if err := g.Command(context.Background(), h.RoomID, cmd.Members[1], startMessage(h.RoomID)); err != nil {
					t.Fatal(err)
				}
			}
			if startFirst {
				start()
				register()
			} else {
				register()
				start()
			}
			first, second := nextBatch(t, s), nextBatch(t, s)
			if second.Revision != first.Revision+1 || second.Snapshot.Accepting {
				t.Fatal("unordered starting barrier")
			}
			if startFirst {
				if second.Events[0].(sc.RegistrationResult).Rejection != sc.AdmissionStarted || second.Snapshot.PlayerCount != 7 {
					t.Fatal("admitted after start")
				}
			} else {
				if first.Events[0].(sc.RegistrationResult).Rejection != "" || second.Snapshot.PlayerCount != 8 {
					t.Fatal("lost registration before start")
				}
			}
		})
	}
	g, s := newTestGateway(t)
	h, _ := createPair(t, g, s)
	publishRoom(t, g, s, h)
	var wg sync.WaitGroup
	for i := 3; i <= 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := g.Register(context.Background(), sc.RegisterCommand{Token: sc.OperationToken(i), RoomID: h.RoomID, Member: testMember(i)}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	accepted, full := 0, 0
	for i := 0; i < 8; i++ {
		b := nextBatch(t, s)
		if b.Snapshot.PlayerCount > 8 {
			t.Fatal("over capacity")
		}
		switch b.Events[0].(sc.RegistrationResult).Rejection {
		case "":
			accepted++
		case sc.AdmissionFull:
			full++
		}
	}
	if accepted != 6 || full != 2 {
		t.Fatalf("accepted=%d full=%d", accepted, full)
	}
}
func TestGatewayCloseDoneAndLateCommands(t *testing.T) {
	g, s := newTestGateway(t)
	h, _ := createPair(t, g, s)
	done, err := g.Close(context.Background(), sc.CloseCommand{Token: 99, RoomID: h.RoomID})
	if err != nil {
		t.Fatal(err)
	}
	awaitDone(t, done)
	again, err := g.Close(context.Background(), sc.CloseCommand{RoomID: h.RoomID})
	if err != nil || again != done || done != h.Done {
		t.Fatal("changed lifetime barrier")
	}
	if err := g.Register(context.Background(), sc.RegisterCommand{RoomID: h.RoomID, Member: testMember(3)}); !errors.Is(err, sc.ErrClosed) {
		t.Fatalf("late register: %v", err)
	}
	b := nextBatch(t, s)
	if b.Events[0].(sc.Closed).Token != 99 || b.Snapshot.PlayerCount != 0 || b.Snapshot.Accepting {
		t.Fatalf("closed: %+v", b)
	}
	select {
	case b := <-s.batches:
		t.Fatalf("event after Done/drain: %+v", b)
	default:
	}
	if err := g.Forget(h.RoomID); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Close(context.Background(), sc.CloseCommand{RoomID: h.RoomID}); !errors.Is(err, sc.ErrRoomNotFound) {
		t.Fatal(err)
	}
}
func TestGatewayBackpressureAndContextTermination(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A full sink must terminate instead of blocking an actor or dropping a prefix.
	g := NewGateway(ctx, &testSink{make(chan sc.EventBatch)})
	h, err := g.CreateInitialPair(ctx, pairCommand())
	if err != nil {
		t.Fatal(err)
	}
	awaitDone(t, h.Done)
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if _, err := g.CreateInitialPair(cancelled, pairCommand()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	r := &lifecycleRoom{ctx: ctx, commands: make(chan any, 1)}
	if err := r.enqueue(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := r.enqueue(ctx, 2); !errors.Is(err, sc.ErrBackpressure) {
		t.Fatal(err)
	}
	if err := r.enqueue(cancelled, 3); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
