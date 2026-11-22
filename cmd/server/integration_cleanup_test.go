package main

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	message "temp-ws/internal/message/v1"
	music "temp-ws/internal/music/v1"
)

// runCycle drives one complete room lifetime on fresh connections: pair,
// refill, start, one played round, host disconnect, abort by departure, leave,
// cancel and re-request on the same socket, then disconnect everyone.
func runCycle(t *testing.T, s *itServer, category music.Category, answers map[string]string) {
	t.Helper()
	p, q, r := s.dial(t, "P"), s.dial(t, "Q"), s.dial(t, "R")
	wp, wq := pair(t, p, q, category)
	wr := join(t, r, category, p, q)
	p.start()
	preload := expectGameStarted(t, p, q, r)
	for _, c := range []*wsClient{p, q, r} {
		c.ready(1)
	}
	for _, c := range []*wsClient{p, q, r} {
		c.expect(message.TypeRoundStarted)
	}
	q.answer(answers[preload.ISRC])
	for _, c := range []*wsClient{p, q, r} {
		c.expect(message.TypeRoundResult)
		c.expect(message.TypePreloadSong)
	}
	p.close()
	for _, c := range []*wsClient{q, r} {
		c.expectPlayer(message.TypePlayerLeft, wp.PlayerID)
		c.expectPlayer(message.TypeHostChanged, wq.PlayerID)
	}
	r.leave()
	q.expectPlayer(message.TypePlayerLeft, wr.PlayerID)
	q.expectAborted(message.InsufficientPlayers)
	q.leave() // zero members: the room closes
	id := r.request(category)
	r.expectWaiting(id)
	r.cancel(id)
	r.expectCancelled(id)
	q.close()
	r.close()
}

// Task T9 "반복 실행 후 요청/방/goroutine 정리 및 actor timeout 격리":
// a Room whose song lookup is stuck in starting does not block the central
// matchmaker or other Rooms. Two groups with mutually incompatible categories
// run repeated full cycles concurrently meanwhile; then the stuck Room
// completes normally. Afterwards every connection, request and Room goroutine
// is released (back to the idle server's goroutine count), and the whole
// assembly stops within its deadline (checked by startServer cleanup).
func TestIntegrationRepeatedCyclesAndActorIsolation(t *testing.T) {
	answers := fixtureAnswers(t)
	s := startServer(t, serverConfig{hold: func(c music.Category) bool { return c == catSoloFemale }})
	idle := runtime.NumGoroutine()

	h1, h2 := s.dial(t, "H1"), s.dial(t, "H2")
	pair(t, h1, h2, catSoloFemale)
	h1.start()
	stuck := s.nextFetch(t)

	t.Run("concurrent cycles", func(t *testing.T) {
		for name, category := range map[string]music.Category{"pop": catPopGroupMale, "all-decades": catAllDecades} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				for range 3 {
					runCycle(t, s, category, answers)
				}
			})
		}
	})
	if t.Failed() {
		return
	}

	// The stuck Room was isolated, not broken: it still starts normally.
	h1.roomBarrier(message.InvalidState)
	stuck.reply <- nil
	expectGameStarted(t, h1, h2)
	h1.close()
	h2.expectPlayer(message.TypePlayerLeft, h1.playerID)
	h2.expectPlayer(message.TypeHostChanged, h2.playerID)
	h2.expectAborted(message.InsufficientPlayers)
	h2.leave()
	h2.close()

	waitGoroutines(t, idle)
}

// AGENTS.md / design 8: cancelling the assembly context stops the matchmaker
// even while Rooms are waiting, starting (song lookup held) and playing with
// sockets still open. Room Done does not wait on the held lookup forever: the
// provider observes cancellation. Afterwards live sockets are closed as soon as
// they send, and the process returns to the goroutine count it had before the
// server existed.
func TestIntegrationShutdownWithActiveRooms(t *testing.T) {
	before := runtime.NumGoroutine()
	t.Cleanup(func() { waitGoroutines(t, before) })
	s := startServer(t, serverConfig{hold: func(c music.Category) bool { return c == catSoloFemale }})

	w1, w2 := s.dial(t, "W1"), s.dial(t, "W2")
	pair(t, w1, w2, catAny)
	s1, s2 := s.dial(t, "S1"), s.dial(t, "S2")
	pair(t, s1, s2, catSoloFemale)
	s1.start()
	s.nextFetch(t) // never answered: only context cancellation releases it
	p1, p2 := s.dial(t, "P1"), s.dial(t, "P2")
	pair(t, p1, p2, catPopGroupMale)
	p1.start()
	expectGameStarted(t, p1, p2)
	lobby := s.dial(t, "L")
	id := lobby.request(catAllDecades)
	lobby.expectWaiting(id)

	s.cancel()
	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		t.Fatal("matchmaker did not stop with active rooms and a held song lookup")
	}
	for _, c := range []*wsClient{w1, w2, s1, s2, p1, p2, lobby} {
		// A routed command fails once the actors are gone, closing the socket.
		_ = c.conn.Write(context.Background(), websocket.MessageText, []byte(`{"type":"MATCH_CANCEL","payload":{"requestID":"`+uuid.NewString()+`"}}`))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		for {
			if _, _, err := c.conn.Read(ctx); err != nil {
				break
			}
		}
		timedOut := ctx.Err() != nil
		cancel()
		if timedOut {
			t.Fatalf("%s: socket stayed open after shutdown", c.name)
		}
		c.close()
	}
}
