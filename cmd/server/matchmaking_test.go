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

func TestMatchingAssemblyOverWebSocket(t *testing.T) {
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
	dial := func() *websocket.Conn {
		c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"?protocolVersion=1", nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.CloseNow() })
		return c
	}
	match := func(c *websocket.Conn) string {
		id := uuid.NewString()
		sendJSON(t, ctx, c, fmt.Sprintf(`{"type":"MATCH_REQUEST","payload":{"requestID":%q,"catalogVersion":"test-v1","category":{"decade":"any","artistType":"any","gender":"any","genre":"any"}}}`, id))
		waiting := readWire(t, ctx, c, message.TypeMatchWaiting).Payload.(*message.MatchWaitingPayload)
		if waiting.RequestID != id {
			t.Fatal("waiting correlation lost")
		}
		return id
	}
	a, b, c := dial(), dial(), dial()
	firstID := match(a)
	match(b)
	wa := readWire(t, ctx, a, message.TypeWelcome).Payload.(*message.WelcomePayload)
	wb := readWire(t, ctx, b, message.TypeWelcome).Payload.(*message.WelcomePayload)
	if wa.RequestID != firstID || wa.RoomID != wb.RoomID || wa.HostID != wa.PlayerID || len(wa.Players) != 2 || wa.Players[0] != wa.PlayerID || wa.Players[1] != wb.PlayerID {
		t.Fatal("initial pair order/identity mismatch")
	}
	match(c)
	wc := readWire(t, ctx, c, message.TypeWelcome).Payload.(*message.WelcomePayload)
	if wc.RoomID != wa.RoomID || len(wc.Players) != 3 {
		t.Fatal("did not refill existing room")
	}
	readWire(t, ctx, c, message.TypePlayerJoined)
	readWire(t, ctx, a, message.TypePlayerJoined)
	readWire(t, ctx, b, message.TypePlayerJoined)
	sendJSON(t, ctx, c, fmt.Sprintf(`{"type":"LEAVE_ROOM","payload":{"roomID":%q}}`, wc.RoomID))
	readWire(t, ctx, c, message.TypeRoomLeft)
	readWire(t, ctx, a, message.TypePlayerLeft)
	readWire(t, ctx, b, message.TypePlayerLeft)
	replacementID := match(c)
	replacement := readWire(t, ctx, c, message.TypeWelcome).Payload.(*message.WelcomePayload)
	if replacement.PlayerID != wc.PlayerID || replacement.RequestID != replacementID || replacement.RoomID != wa.RoomID {
		t.Fatal("same connection not reusable after leave")
	}
	readWire(t, ctx, c, message.TypePlayerJoined)
}

func TestMatchingCatalogRequiresValidatedFiles(t *testing.T) {
	base := "../../docs/specs/random-matchmaking/fixtures/"
	for _, paths := range [][2]string{{"", ""}, {base + "catalog.json", ""}, {base + "manifest.json", base + "manifest.json"}} {
		if _, err := loadMatchingCatalog(paths[0], paths[1]); err == nil {
			t.Fatalf("accepted unavailable/invalid catalog: %v", paths)
		}
	}
}
