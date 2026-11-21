package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	message "temp-ws/internal/message/v1"
	music "temp-ws/internal/music/v1"
	"temp-ws/internal/room"
	"temp-ws/internal/session"
	sc "temp-ws/internal/sessioncontract"
)

type matchInput struct {
	connection session.Connection
	message    message.Message
}
type captureHandler struct {
	inputs       chan matchInput
	disconnected chan session.Connection
}

func (h *captureHandler) Match(_ context.Context, c session.Connection, m message.Message) error {
	h.inputs <- matchInput{c, m}
	return nil
}
func (h *captureHandler) Disconnected(c session.Connection) { h.disconnected <- c }

type socketHarness struct {
	ctx      context.Context
	server   *httptest.Server
	gateway  *room.Gateway
	registry *session.Registry
	events   *session.EventQueue
	handler  *captureHandler
}

func newSocketHarness(t *testing.T) *socketHarness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	events := session.NewEventQueue(ctx, 256)
	gateway := room.NewGateway(ctx, events)
	handler := &captureHandler{make(chan matchInput, 32), make(chan session.Connection, 32)}
	registry := session.NewRegistry(ctx, gateway, handler, nil)
	server := httptest.NewServer(handleWebSocket(registry))
	t.Cleanup(func() { cancel(); server.Close() })
	return &socketHarness{ctx, server, gateway, registry, events, handler}
}
func (h *socketHarness) dial(t *testing.T) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.Dial(h.ctx, "ws"+strings.TrimPrefix(h.server.URL, "http")+"?protocolVersion=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	return c
}
func sendJSON(t *testing.T, ctx context.Context, c *websocket.Conn, raw string) {
	t.Helper()
	if err := c.Write(ctx, websocket.MessageText, []byte(raw)); err != nil {
		t.Fatal(err)
	}
}
func readWire(t *testing.T, ctx context.Context, c *websocket.Conn, kind string) message.Message {
	t.Helper()
	frame, data, err := c.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if frame != websocket.MessageText {
		t.Fatal("nontext event")
	}
	m, err := message.Decode(data, message.ServerToClient)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != kind {
		t.Fatalf("want %s got %s", kind, m.Type)
	}
	return m
}
func (h *socketHarness) identify(t *testing.T, c *websocket.Conn) session.Connection {
	t.Helper()
	sendJSON(t, h.ctx, c, fmt.Sprintf(`{"type":"MATCH_CANCEL","payload":{"requestID":%q}}`, uuid.NewString()))
	select {
	case input := <-h.handler.inputs:
		return input.connection
	case <-h.ctx.Done():
		t.Fatal("missing delegated match input")
		return session.Connection{}
	}
}
func (h *socketHarness) batch(t *testing.T) sc.EventBatch {
	t.Helper()
	select {
	case b := <-h.events.Batches():
		return b
	case <-h.ctx.Done():
		t.Fatal("missing room batch")
		return sc.EventBatch{}
	}
}
func (h *socketHarness) pair(t *testing.T, a, b session.Connection, sequence int) (sc.RoomHandle, [2]sc.Member) {
	t.Helper()
	members := [2]sc.Member{{PlayerID: a.PlayerID, RequestID: uuid.NewString(), ArrivalSequence: sc.Sequence(sequence)}, {PlayerID: b.PlayerID, RequestID: uuid.NewString(), ArrivalSequence: sc.Sequence(sequence + 1)}}
	handle, err := h.gateway.CreateInitialPair(h.ctx, sc.CreateInitialPairCommand{Token: session.NextOperationToken(), Category: music.Category{Decade: "all", ArtistType: "all", Gender: "all", Genre: "all"}, CatalogVersion: "test-v1", Members: members})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		done, err := h.gateway.Close(context.Background(), sc.CloseCommand{RoomID: handle.RoomID})
		if err == nil {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("room did not terminate")
			}
		}
	})
	batch := h.batch(t)
	if cleanup := h.registry.Apply(batch, members[:]); len(cleanup) != 0 {
		t.Fatal(cleanup)
	}
	if err := h.gateway.Publish(h.ctx, sc.PublishCommand{Token: session.NextOperationToken(), RoomID: handle.RoomID}); err != nil {
		t.Fatal(err)
	}
	h.registry.Apply(h.batch(t), nil)
	return handle, members
}
func TestWebSocketProtocolVersion(t *testing.T) {
	h := newSocketHarness(t)
	for _, query := range []string{"", "?protocolVersion=2", "?protocolVersion=1&room=", "?protocolVersion=1&room=x", "?protocolVersion=1&protocolVersion=1", "?protocolVersion=1&bad=%zz"} {
		response, err := http.Get(h.server.URL + query)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != 400 {
			t.Fatalf("%s: %d", query, response.StatusCode)
		}
	}
	c := h.dial(t)
	h.identify(t, c)
	// Delegation proves a live lobby socket; no room is created by the endpoint.
	select {
	case b := <-h.events.Batches():
		t.Fatalf("connection created a room: %+v", b)
	default:
	}
}
func TestWebSocketRejectsInvalidFrames(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind websocket.MessageType
		data string
		code websocket.StatusCode
	}{
		{"binary", websocket.MessageBinary, "{}", websocket.StatusUnsupportedData},
		{"json", websocket.MessageText, "{", websocket.StatusPolicyViolation},
		{"unknown", websocket.MessageText, `{"type":"NOPE","payload":{}}`, websocket.StatusPolicyViolation},
		{"missing", websocket.MessageText, `{"type":"LEAVE_ROOM","payload":{}}`, websocket.StatusPolicyViolation},
		{"invalid uuid", websocket.MessageText, `{"type":"LEAVE_ROOM","payload":{"roomID":"x"}}`, websocket.StatusPolicyViolation},
		{"oversize", websocket.MessageText, strings.Repeat("x", message.MaxFrameBytes+1), websocket.StatusMessageTooBig},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newSocketHarness(t)
			c := h.dial(t)
			if err := c.Write(h.ctx, tc.kind, []byte(tc.data)); err != nil {
				t.Fatal(err)
			}
			_, _, err := c.Read(h.ctx)
			if websocket.CloseStatus(err) != tc.code {
				t.Fatalf("want close %d got %v", tc.code, err)
			}
		})
	}
}
func TestWebSocketLeaveAndReuseWithOrderedWelcome(t *testing.T) {
	h := newSocketHarness(t)
	a, b, c := h.dial(t), h.dial(t), h.dial(t)
	ca, cb, cc := h.identify(t, a), h.identify(t, b), h.identify(t, c)
	first, members := h.pair(t, ca, cb, 1)
	readWire(t, h.ctx, a, message.TypeWelcome)
	readWire(t, h.ctx, b, message.TypeWelcome)
	third := sc.Member{PlayerID: cc.PlayerID, RequestID: uuid.NewString(), ArrivalSequence: 3}
	if err := h.gateway.Register(h.ctx, sc.RegisterCommand{Token: session.NextOperationToken(), RoomID: first.RoomID, Member: third}); err != nil {
		t.Fatal(err)
	}
	h.registry.Apply(h.batch(t), []sc.Member{third})
	// The same actor batch contains admission then room events for the newcomer.
	readWire(t, h.ctx, c, message.TypeWelcome)
	readWire(t, h.ctx, c, message.TypePlayerJoined)
	readWire(t, h.ctx, a, message.TypePlayerJoined)
	readWire(t, h.ctx, b, message.TypePlayerJoined)
	sendJSON(t, h.ctx, a, fmt.Sprintf(`{"type":"LEAVE_ROOM","payload":{"roomID":%q}}`, first.RoomID))
	h.registry.Apply(h.batch(t), nil)
	readWire(t, h.ctx, a, message.TypeRoomLeft)
	readWire(t, h.ctx, b, message.TypePlayerLeft)
	readWire(t, h.ctx, b, message.TypeHostChanged)
	readWire(t, h.ctx, c, message.TypePlayerLeft)
	readWire(t, h.ctx, c, message.TypeHostChanged)
	// Reuse the SAME socket/player in another room with a different request.
	d := h.dial(t)
	cd := h.identify(t, d)
	second, _ := h.pair(t, ca, cd, 4)
	welcome := readWire(t, h.ctx, a, message.TypeWelcome).Payload.(*message.WelcomePayload)
	if welcome.PlayerID != ca.PlayerID || welcome.RoomID != second.RoomID {
		t.Fatal("connection was not reused")
	}
	readWire(t, h.ctx, d, message.TypeWelcome)
	// Late leave and old request cleanup cannot remove the new membership.
	sendJSON(t, h.ctx, a, fmt.Sprintf(`{"type":"LEAVE_ROOM","payload":{"roomID":%q}}`, first.RoomID))
	readWire(t, h.ctx, a, message.TypeRoomLeft)
	if err := h.gateway.Remove(h.ctx, sc.RemoveCommand{Token: session.NextOperationToken(), RoomID: first.RoomID, PlayerID: ca.PlayerID, RequestID: members[0].RequestID}); err != nil {
		t.Fatal(err)
	}
	h.registry.Apply(h.batch(t), nil)
	sendJSON(t, h.ctx, a, fmt.Sprintf(`{"type":"LEAVE_ROOM","payload":{"roomID":%q}}`, second.RoomID))
	batch := h.batch(t)
	if batch.RoomID != second.RoomID || batch.Snapshot.PlayerCount != 1 {
		t.Fatal("replacement membership lost")
	}
	h.registry.Apply(batch, nil)
	readWire(t, h.ctx, a, message.TypeRoomLeft)
}
func TestWebSocketDisconnectRemovesMembership(t *testing.T) {
	h := newSocketHarness(t)
	a, b := h.dial(t), h.dial(t)
	ca, cb := h.identify(t, a), h.identify(t, b)
	handle, _ := h.pair(t, ca, cb, 1)
	readWire(t, h.ctx, a, message.TypeWelcome)
	readWire(t, h.ctx, b, message.TypeWelcome)
	_ = a.CloseNow()
	batch := h.batch(t)
	if batch.RoomID != handle.RoomID || batch.Snapshot.PlayerCount != 1 {
		t.Fatalf("disconnect cleanup: %+v", batch)
	}
	h.registry.Apply(batch, nil)
	readWire(t, h.ctx, b, message.TypePlayerLeft)
	readWire(t, h.ctx, b, message.TypeHostChanged)
	select {
	case c := <-h.handler.disconnected:
		if c != ca {
			t.Fatal("wrong disconnect")
		}
	case <-h.ctx.Done():
		t.Fatal("no disconnect notification")
	}
}
