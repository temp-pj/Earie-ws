package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	message "temp-ws/internal/message/v1"
	sc "temp-ws/internal/sessioncontract"
)

func TestConnectionOutboundLimits(t *testing.T) {
	for _, tc := range []struct {
		name        string
		size, count int
	}{
		{"frames", 1, sc.MaxOutboundFrames}, {"bytes", sc.MaxOutboundBytes / 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewConnection(context.Background(), nil)
			defer c.Stop()
			for i := 0; i < tc.count; i++ {
				if err := c.enqueue(make([]byte, tc.size)); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.enqueue([]byte{1}); !errors.Is(err, sc.ErrBackpressure) {
				t.Fatalf("overflow: %v", err)
			}
			if c.Context().Err() == nil {
				t.Fatal("slow receiver not terminated")
			}
			if err := c.enqueue(nil); !errors.Is(err, sc.ErrClosed) {
				t.Fatal(err)
			}
		})
	}
}
func TestConnectionSendCopiesAndFIFO(t *testing.T) {
	c := NewConnection(context.Background(), nil)
	defer c.Stop()
	now := time.Now().UnixMilli()
	payload := &message.RoomLeftPayload{RoomID: uuid.NewString()}
	first := payload.RoomID
	if err := c.Send(message.Message{Type: message.TypeRoomLeft, Payload: payload, Timestamp: &now}); err != nil {
		t.Fatal(err)
	}
	payload.RoomID = uuid.NewString()
	second := payload.RoomID
	if err := c.Send(message.Message{Type: message.TypeRoomLeft, Payload: payload, Timestamp: &now}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{first, second} {
		msg, err := message.Decode(c.take(), message.ServerToClient)
		if err != nil {
			t.Fatal(err)
		}
		if msg.Payload.(*message.RoomLeftPayload).RoomID != want {
			t.Fatal("FIFO/copy violated")
		}
	}
	if c.take() != nil || c.bytes != 0 {
		t.Fatal("queue accounting")
	}
}

type observedOperation struct {
	ctx  context.Context
	data []byte
}
type fakeSocket struct {
	writes chan observedOperation
	pings  chan observedOperation
	closed chan struct{}
	once   sync.Once
}

func (s *fakeSocket) SetReadLimit(int64) {}
func (s *fakeSocket) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	<-ctx.Done()
	return 0, nil, ctx.Err()
}
func (s *fakeSocket) Write(ctx context.Context, _ websocket.MessageType, data []byte) error {
	s.writes <- observedOperation{ctx, data}
	<-ctx.Done()
	return ctx.Err()
}
func (s *fakeSocket) Ping(ctx context.Context) error {
	s.pings <- observedOperation{ctx: ctx}
	<-ctx.Done()
	return ctx.Err()
}
func (s *fakeSocket) Close(websocket.StatusCode, string) error { return s.CloseNow() }
func (s *fakeSocket) CloseNow() error                          { s.once.Do(func() { close(s.closed) }); return nil }
func receiveOperation(t *testing.T, ch <-chan observedOperation) observedOperation {
	t.Helper()
	select {
	case op := <-ch:
		return op
	case <-time.After(time.Second):
		t.Fatal("worker deadline")
		return observedOperation{}
	}
}
func assertDeadline(t *testing.T, ctx context.Context, want time.Duration) {
	t.Helper()
	deadline, ok := ctx.Deadline()
	remaining := time.Until(deadline)
	if !ok || remaining <= 0 || remaining > want {
		t.Fatalf("missing/incorrect deadline: %v %v", deadline, ok)
	}
}
func waitWorker(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker failed to stop")
	}
}
func TestConnectionWorkersUseDeadlinesAndTerminate(t *testing.T) {
	s := &fakeSocket{writes: make(chan observedOperation, 1), pings: make(chan observedOperation, 1), closed: make(chan struct{})}
	c := NewConnection(context.Background(), nil)
	c.conn = s
	if err := c.enqueue([]byte("queued")); err != nil {
		t.Fatal(err)
	}
	go c.Run(func(message.Message) error { return nil })
	write := receiveOperation(t, s.writes)
	assertDeadline(t, write.ctx, sc.WriteTimeout)
	// Inject a tick, without sleeping for the production 20-second interval.
	ticks := make(chan time.Time, 1)
	ticks <- time.Time{}
	pingDone := make(chan struct{})
	go func() { defer close(pingDone); c.pingOn(ticks) }()
	ping := receiveOperation(t, s.pings)
	assertDeadline(t, ping.ctx, sc.PongTimeout)
	c.Stop()
	waitWorker(t, pingDone)
	waitWorker(t, c.Done())
	waitWorker(t, s.closed)
	if write.ctx.Err() == nil || ping.ctx.Err() == nil {
		t.Fatal("I/O contexts survived connection")
	}
}
func TestConnectionConcurrentSendAndStop(t *testing.T) {
	c := NewConnection(context.Background(), nil)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = c.enqueue([]byte("frame"))
			}
		}()
	}
	c.Stop()
	wg.Wait()
	if c.Context().Err() == nil {
		t.Fatal("not stopped")
	}
}
