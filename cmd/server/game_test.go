package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"temp-ws/internal/game"
	message "temp-ws/internal/message/v1"
)

// readGame reads the next event of kind, skipping wall-clock COUNT_DOWN ticks
// that may interleave when the machine is slow. Ordering is otherwise strict.
func readGame(t *testing.T, ctx context.Context, c *websocket.Conn, kind string) message.Message {
	t.Helper()
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		m, err := message.Decode(data, message.ServerToClient)
		if err != nil {
			t.Fatal(err)
		}
		if m.Type == message.TypeCountDown && kind != message.TypeCountDown {
			continue
		}
		if m.Type != kind {
			t.Fatalf("want %s got %s", kind, m.Type)
		}
		return m
	}
}

// Synthetic answers come from the public test fixture, never private state.
func fixtureAnswers(t *testing.T) map[string]string {
	t.Helper()
	data, err := os.ReadFile("../../docs/specs/random-matchmaking/fixtures/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Songs []struct{ ISRC, Title string } `json:"songs"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	answers := make(map[string]string, len(manifest.Songs))
	for _, s := range manifest.Songs {
		answers[s.ISRC] = game.PrimaryTitle(s.Title)
	}
	return answers
}

func TestGameOverLeaveAndReuseOverWebSocket(t *testing.T) {
	store, err := loadMatchingCatalog("../../docs/specs/random-matchmaking/fixtures/catalog.json", "../../docs/specs/random-matchmaking/fixtures/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	answers := fixtureAnswers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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
	match := func(c *websocket.Conn) {
		sendJSON(t, ctx, c, fmt.Sprintf(`{"type":"MATCH_REQUEST","payload":{"requestID":%q,"catalogVersion":"test-v1","category":{"decade":"any","artistType":"any","gender":"any","genre":"any"}}}`, uuid.NewString()))
		readWire(t, ctx, c, message.TypeMatchWaiting)
	}
	pair := func(a, b *websocket.Conn) (*message.WelcomePayload, *message.WelcomePayload) {
		match(a)
		match(b)
		return readWire(t, ctx, a, message.TypeWelcome).Payload.(*message.WelcomePayload), readWire(t, ctx, b, message.TypeWelcome).Payload.(*message.WelcomePayload)
	}
	a, b := dial(), dial()
	wa, wb := pair(a, b)
	room := wa.RoomID
	if wa.HostID != wa.PlayerID {
		t.Fatal("first request is not host")
	}
	// Non-host and wrong-roomID commands: failure only to the sender, or ignored.
	sendJSON(t, ctx, b, fmt.Sprintf(`{"type":"START_GAME","payload":{"roomID":%q}}`, room))
	if p := readWire(t, ctx, b, message.TypeRoomCommandFailed).Payload.(*message.RoomCommandFailedPayload); p.Code != message.NotHost {
		t.Fatal(p.Code)
	}
	sendJSON(t, ctx, a, fmt.Sprintf(`{"type":"START_GAME","payload":{"roomID":%q}}`, uuid.NewString()))
	// Legacy client fields are ignored: the server uses the room category,
	// 10 tracks and the default time limit.
	sendJSON(t, ctx, a, fmt.Sprintf(`{"type":"START_GAME","payload":{"roomID":%q,"category":{"genre":"x"},"trackCount":3,"timeLimit":5}}`, room))
	for _, c := range []*websocket.Conn{a, b} {
		if p := readGame(t, ctx, c, message.TypeGameStarted).Payload.(*message.RoomPayload); p.RoomID != room {
			t.Fatal("GAME_STARTED roomID")
		}
	}
	isrcs := map[string]bool{}
	for round := 1; ; round++ {
		var preload *message.PreloadSongPayload
		for _, c := range []*websocket.Conn{a, b} {
			preload = readGame(t, ctx, c, message.TypePreloadSong).Payload.(*message.PreloadSongPayload)
			sendJSON(t, ctx, c, fmt.Sprintf(`{"type":"READY_TO_PLAY","payload":{"roomID":%q,"roundNumber":%d}}`, room, round))
		}
		if preload.RoundNumber != round || isrcs[preload.ISRC] || answers[preload.ISRC] == "" {
			t.Fatalf("round %d: %+v", round, preload)
		}
		isrcs[preload.ISRC] = true
		for _, c := range []*websocket.Conn{a, b} {
			if p := readGame(t, ctx, c, message.TypeRoundStarted).Payload.(*message.RoundStartPayload); p.TotalRounds != 10 {
				t.Fatal(p.TotalRounds)
			}
		}
		sendJSON(t, ctx, b, fmt.Sprintf(`{"type":"SUBMIT_ANSWER","payload":{"roomID":%q,"answer":%q}}`, room, answers[preload.ISRC]))
		for _, c := range []*websocket.Conn{a, b} {
			if p := readGame(t, ctx, c, message.TypeRoundResult).Payload.(*message.RoundResultPayload); p.Winner != wb.PlayerID {
				t.Fatal(p.Winner)
			}
		}
		if round == 10 {
			break
		}
	}
	for _, c := range []*websocket.Conn{a, b} {
		if p := readGame(t, ctx, c, message.TypeGameOver).Payload.(*message.GameOverPayload); p.Winner != wb.PlayerID || p.Scores[wb.PlayerID] != 10 {
			t.Fatalf("%+v", p)
		}
	}
	// Result: no restart. Each player leaves; the connections stay open.
	sendJSON(t, ctx, a, fmt.Sprintf(`{"type":"START_GAME","payload":{"roomID":%q}}`, room))
	if p := readWire(t, ctx, a, message.TypeRoomCommandFailed).Payload.(*message.RoomCommandFailedPayload); p.Code != message.InvalidState {
		t.Fatal(p.Code)
	}
	sendJSON(t, ctx, a, fmt.Sprintf(`{"type":"LEAVE_ROOM","payload":{"roomID":%q}}`, room))
	readWire(t, ctx, a, message.TypeRoomLeft)
	readWire(t, ctx, b, message.TypePlayerLeft)
	readWire(t, ctx, b, message.TypeHostChanged)
	sendJSON(t, ctx, b, fmt.Sprintf(`{"type":"LEAVE_ROOM","payload":{"roomID":%q}}`, room))
	readWire(t, ctx, b, message.TypeRoomLeft)
	// Same sockets, same players, new requests: a brand-new room.
	na, nb := pair(a, b)
	if na.PlayerID != wa.PlayerID || nb.PlayerID != wb.PlayerID || na.RoomID == room || na.RoomID != nb.RoomID {
		t.Fatalf("reuse: %+v %+v", na, nb)
	}
}
