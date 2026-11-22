package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	message "temp-ws/internal/message/v1"
)

func cancellationSockets(t *testing.T) (context.Context, func() *websocket.Conn) {
	t.Helper()
	store, err := loadMatchingCatalog("../../docs/specs/random-matchmaking/fixtures/catalog.json", "../../docs/specs/random-matchmaking/fixtures/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	registry, done := newMatchingRegistry(ctx, store)
	server := httptest.NewServer(handleWebSocket(registry))
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("matcher did not stop")
		}
		server.Close()
	})
	return ctx, func() *websocket.Conn {
		c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"?protocolVersion=1", nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.CloseNow() })
		return c
	}
}
func matchingJSON(id, version string) string {
	return fmt.Sprintf(`{"type":"MATCH_REQUEST","payload":{"requestID":%q,"catalogVersion":%q,"category":{"decade":"any","artistType":"any","gender":"any","genre":"any"}}}`, id, version)
}
func cancellingJSON(id string) string {
	return fmt.Sprintf(`{"type":"MATCH_CANCEL","payload":{"requestID":%q}}`, id)
}

func TestCancellationAndSameSocketRematch(t *testing.T) {
	ctx, dial := cancellationSockets(t)
	a, b := dial(), dial()
	old, peer, next := uuid.NewString(), uuid.NewString(), uuid.NewString()
	sendJSON(t, ctx, a, matchingJSON(old, "test-v1"))
	readWire(t, ctx, a, message.TypeMatchWaiting)
	sendJSON(t, ctx, b, matchingJSON(peer, "test-v1"))
	readWire(t, ctx, b, message.TypeMatchWaiting)
	original := readWire(t, ctx, a, message.TypeWelcome).Payload.(*message.WelcomePayload)
	readWire(t, ctx, b, message.TypeWelcome)
	// Send cancellation and replacement back-to-back, before reading cleanup.
	sendJSON(t, ctx, a, cancellingJSON(old))
	sendJSON(t, ctx, a, matchingJSON(next, "test-v1"))
	seenCancelled, seenWaiting := false, false
	for !seenCancelled || !seenWaiting {
		_, raw, err := a.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		msg, err := message.Decode(raw, message.ServerToClient)
		if err != nil {
			t.Fatal(err)
		}
		switch msg.Type {
		case message.TypeMatchCancelled:
			p := msg.Payload.(*message.MatchCancelledPayload)
			if p.RequestID != old {
				t.Fatal("wrong cancelled ID")
			}
			seenCancelled = true
		case message.TypeMatchWaiting:
			p := msg.Payload.(*message.MatchWaitingPayload)
			if p.RequestID != next {
				t.Fatal("wrong replacement ID")
			}
			seenWaiting = true
		default:
			t.Fatalf("unexpected response before cleanup: %s", msg.Type)
		}
	}
	replacement := readWire(t, ctx, a, message.TypeWelcome).Payload.(*message.WelcomePayload)
	if replacement.RequestID != next || replacement.PlayerID != original.PlayerID || replacement.RoomID != original.RoomID {
		t.Fatal("replacement membership lost")
	}
	readWire(t, ctx, a, message.TypePlayerJoined)
	sendJSON(t, ctx, a, matchingJSON(old, "test-v1"))
	readWire(t, ctx, a, message.TypeMatchCancelled)
	sendJSON(t, ctx, a, matchingJSON(next, "test-v1"))
	replay := readWire(t, ctx, a, message.TypeWelcome).Payload.(*message.WelcomePayload)
	if replay.RequestID != next || replay.RoomID != replacement.RoomID {
		t.Fatal("new request replay changed")
	}
}

func TestRequestPolicyCloseOverWebSocket(t *testing.T) {
	ctx, dial := cancellationSockets(t)
	for _, state := range []string{"waiting", "failed", "unknown cancel"} {
		t.Run(state, func(t *testing.T) {
			c := dial()
			id := uuid.NewString()
			version := "test-v1"
			if state == "unknown cancel" {
				sendJSON(t, ctx, c, cancellingJSON(id))
				readWire(t, ctx, c, message.TypeMatchCancelled)
			}
			if state == "failed" {
				version = "unknown"
			}
			sendJSON(t, ctx, c, matchingJSON(id, version))
			kind := message.TypeMatchWaiting
			if state == "failed" {
				kind = message.TypeMatchFailed
			}
			if state == "unknown cancel" {
				kind = message.TypeMatchCancelled
			}
			readWire(t, ctx, c, kind)
			sendJSON(t, ctx, c, matchingJSON(id, "changed"))
			_, _, err := c.Read(ctx)
			if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
				t.Fatalf("want 1008, got %v", err)
			}
		})
	}
}

func TestPipelinedPolicyViolationKeepsCloseCode(t *testing.T) {
	ctx, dial := cancellationSockets(t)
	c := dial()
	id := uuid.NewString()
	sendJSON(t, ctx, c, matchingJSON(id, "test-v1"))
	readWire(t, ctx, c, message.TypeMatchWaiting)
	// A second frame can reach Route after the policy closure removed Registry
	// membership, while the connection writer is still sending the close frame.
	for range 8 {
		if err := c.Write(ctx, websocket.MessageText, []byte(matchingJSON(id, "changed"))); err != nil {
			break
		}
	}
	_, _, err := c.Read(ctx)
	if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatalf("want 1008 for pipelined violation, got %v", err)
	}
}
