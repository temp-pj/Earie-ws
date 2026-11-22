package main

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	message "temp-ws/internal/message/v1"
	music "temp-ws/internal/music/v1"
	sc "temp-ws/internal/sessioncontract"
)

func holdAll(music.Category) bool { return true }

// expectGameStarted reads GAME_STARTED followed by round 1 PRELOAD_SONG.
func expectGameStarted(t *testing.T, players ...*wsClient) *message.PreloadSongPayload {
	t.Helper()
	var preload *message.PreloadSongPayload
	for _, p := range players {
		p.expect(message.TypeGameStarted)
		preload = p.expect(message.TypePreloadSong).Payload.(*message.PreloadSongPayload)
		if preload.RoundNumber != 1 {
			t.Fatalf("%s: first preload round %d", p.name, preload.RoundNumber)
		}
	}
	return preload
}

// REQ-ROOM-02, REQ-ROOM-06, REQ-GAME-01: registration completed before the
// start is part of the game; a request arriving once START_GAME has closed
// admission (starting) is not placed in that room, gets no failure, and keeps
// matching. The song provider receives the room's authoritative category.
func TestIntegrationJoinBeforeAndAfterStart(t *testing.T) {
	t.Run("join completed before start is in the game", func(t *testing.T) {
		s := startServer(t, serverConfig{hold: holdAll})
		a, b, c := s.dial(t, "A"), s.dial(t, "B"), s.dial(t, "C")
		wa, _ := pair(t, a, b, catAny)
		join(t, c, catAny, a, b)
		a.start()
		call := s.nextFetch(t)
		if call.version != syntheticVersion || call.category != wa.Category {
			t.Fatalf("provider got %s/%+v, room is %+v", call.version, call.category, wa.Category)
		}
		call.reply <- nil
		expectGameStarted(t, a, b, c)
	})
	t.Run("request during starting waits and pairs elsewhere", func(t *testing.T) {
		s := startServer(t, serverConfig{hold: holdAll})
		a, b, c, d := s.dial(t, "A"), s.dial(t, "B"), s.dial(t, "C"), s.dial(t, "D")
		wa, _ := pair(t, a, b, catAny)
		a.start()
		call := s.nextFetch(t) // the Room is now in starting: admission closed
		rc := c.request(catAny)
		c.expectWaiting(rc)
		rd := d.request(catAny)
		wd := d.expectWelcome(rd)
		wc := c.expectWelcome(rc)
		if wc.RoomID == wa.RoomID || wc.RoomID != wd.RoomID || wc.HostID != wc.PlayerID {
			t.Fatalf("C entered the starting room or lost order: %+v %+v", wc, wd)
		}
		call.reply <- nil
		expectGameStarted(t, a, b)
		// C and D received nothing from the started room: their next frames
		// are answers to their own barriers.
		c.lobbyBarrier()
		d.lobbyBarrier()
	})
	t.Run("concurrent join and start", func(t *testing.T) {
		admitted, refused := 0, 0
		for iteration := range 10 {
			s := startServer(t, serverConfig{hold: holdAll})
			a, b, c := s.dial(t, "A"), s.dial(t, "B"), s.dial(t, "C")
			wa, wb := pair(t, a, b, catAny)
			rc := c.request(catAny)
			a.start()
			call := s.nextFetch(t)
			call.reply <- nil
			// A's first frame decides: PLAYER_JOINED(C) means C registered
			// before START closed admission, so C must be in the game.
			first := a.next()
			switch first.Type {
			case message.TypePlayerJoined:
				admitted++
				wc := c.expectWelcome(rc)
				if wc.RoomID != wa.RoomID || !reflect.DeepEqual(wc.Players, []string{wa.PlayerID, wb.PlayerID, wc.PlayerID}) {
					t.Fatalf("iteration %d: %+v", iteration, wc)
				}
				c.expectPlayer(message.TypePlayerJoined, wc.PlayerID)
				b.expectPlayer(message.TypePlayerJoined, wc.PlayerID)
				expectGameStarted(t, a, b, c)
			case message.TypeGameStarted:
				refused++
				a.expect(message.TypePreloadSong)
				expectGameStarted(t, b)
				c.expectWaiting(rc)
				// No failure alert: C keeps matching and pairs with D.
				d := s.dial(t, "D")
				rd := d.request(catAny)
				wd := d.expectWelcome(rd)
				if wc := c.expectWelcome(rc); wc.RoomID == wa.RoomID || wc.RoomID != wd.RoomID {
					t.Fatalf("iteration %d: %+v", iteration, wc)
				}
			default:
				t.Fatalf("iteration %d: unexpected %s", iteration, first.Type)
			}
		}
		t.Logf("concurrent join/start: admitted=%d refused=%d", admitted, refused)
	})
}

// design 7: duplicate START_GAME is INVALID_STATE without a second game,
// lookup failure returns to waiting with INTERNAL_ERROR (and the room accepts
// again), and a departure during starting cancels the start with
// NOT_ENOUGH_PLAYERS while the late lookup result is discarded.
func TestIntegrationStartFailures(t *testing.T) {
	t.Run("duplicate start", func(t *testing.T) {
		s := startServer(t, serverConfig{hold: holdAll})
		a, b := s.dial(t, "A"), s.dial(t, "B")
		pair(t, a, b, catAny)
		a.start()
		a.start()
		a.expectCommandFailed(message.InvalidState)
		call := s.nextFetch(t)
		s.noPendingFetch(t)
		call.reply <- nil
		expectGameStarted(t, a, b)
		a.roomBarrier(message.InvalidState)
		s.noPendingFetch(t)
	})
	t.Run("lookup failure", func(t *testing.T) {
		s := startServer(t, serverConfig{hold: holdAll})
		a, b, c := s.dial(t, "A"), s.dial(t, "B"), s.dial(t, "C")
		pair(t, a, b, catAny)
		a.start()
		s.nextFetch(t).reply <- errInjectedLookup
		a.expectCommandFailed(message.RoomInternalError)
		join(t, c, catAny, a, b)
		a.start()
		s.nextFetch(t).reply <- nil
		expectGameStarted(t, a, b, c)
	})
	t.Run("non-host and lone host", func(t *testing.T) {
		s := startServer(t, serverConfig{})
		a, b := s.dial(t, "A"), s.dial(t, "B")
		_, wb := pair(t, a, b, catAny)
		b.roomBarrier(message.NotHost)
		host := a.playerID
		a.leave()
		b.expectPlayer(message.TypePlayerLeft, host)
		b.expectPlayer(message.TypeHostChanged, wb.PlayerID)
		b.roomBarrier(message.NotEnoughPlayers)
	})
	t.Run("departure while starting", func(t *testing.T) {
		s := startServer(t, serverConfig{hold: holdAll})
		a, b, c := s.dial(t, "A"), s.dial(t, "B"), s.dial(t, "C")
		_, wb := pair(t, a, b, catAny)
		a.start()
		call := s.nextFetch(t)
		b.leave()
		a.expectPlayer(message.TypePlayerLeft, wb.PlayerID)
		a.expectCommandFailed(message.NotEnoughPlayers)
		call.reply <- nil // late result of the cancelled start
		// Back in waiting with one member: the room accepts again.
		join(t, c, catAny, a)
		a.start()
		s.nextFetch(t).reply <- nil
		expectGameStarted(t, a, c)
	})
}

// REQ-ROOM-03: waiting-room host succession by request arrival order; one
// member keeps the room (refillable, cannot start); zero members deletes it.
func TestIntegrationHostSuccessionAndEmptyRoom(t *testing.T) {
	s := startServer(t, serverConfig{})
	a, b, c, d := s.dial(t, "A"), s.dial(t, "B"), s.dial(t, "C"), s.dial(t, "D")
	wa, wb := pair(t, a, b, catAny)
	wc := join(t, c, catAny, a, b)

	// Repeated LEAVE_ROOM (REQ-CONN-04): each is acknowledged, the membership
	// is removed once, and members see a single departure.
	a.send(message.TypeLeaveRoom, message.RoomPayload{RoomID: wa.RoomID})
	a.leave()
	if p := a.expect(message.TypeRoomLeft).Payload.(*message.RoomPayload); p.RoomID != wa.RoomID {
		t.Fatalf("duplicate ROOM_LEFT %+v", p)
	}
	for _, m := range []*wsClient{b, c} {
		m.expectPlayer(message.TypePlayerLeft, wa.PlayerID)
		m.expectPlayer(message.TypeHostChanged, wb.PlayerID)
	}
	b.leave()
	c.expectPlayer(message.TypePlayerLeft, wb.PlayerID)
	c.expectPlayer(message.TypeHostChanged, wc.PlayerID)
	c.roomBarrier(message.NotEnoughPlayers)

	wd := join(t, d, catAny, c)
	if wd.RoomID != wa.RoomID || wd.HostID != wc.PlayerID || !reflect.DeepEqual(wd.Players, []string{wc.PlayerID, wd.PlayerID}) {
		t.Fatalf("one-member room must stay refillable: %+v", wd)
	}
	c.leave()
	d.expectPlayer(message.TypePlayerLeft, wc.PlayerID)
	d.expectPlayer(message.TypeHostChanged, wd.PlayerID)
	d.leave() // zero members: the room is deleted

	// The deleted room is not a candidate: A waits and pairs with B anew.
	ra := a.request(catAny)
	a.expectWaiting(ra)
	rb := b.request(catAny)
	nb := b.expectWelcome(rb)
	na := a.expectWelcome(ra)
	if na.RoomID == wa.RoomID || na.RoomID != nb.RoomID || na.PlayerID != wa.PlayerID {
		t.Fatalf("deleted room reused or connection not reused: %+v", na)
	}
}

// REQ-CONN-02: the server cleans up after a dropped connection in every
// state; nobody is ever paired with a disconnected user.
func TestIntegrationDisconnectCleanup(t *testing.T) {
	t.Run("waiting", func(t *testing.T) {
		s := startServer(t, serverConfig{})
		x, y, z := s.dial(t, "X"), s.dial(t, "Y"), s.dial(t, "Z")
		rx := x.request(catAny)
		x.expectWaiting(rx)
		x.close()
		s.awaitHandlerExit(t)
		ry := y.request(catAny)
		y.expectWaiting(ry)
		rz := z.request(catAny)
		wz := z.expectWelcome(rz)
		if wy := y.expectWelcome(ry); wy.RoomID != wz.RoomID || len(wy.Players) != 2 {
			t.Fatalf("paired with a disconnected user: %+v", wy)
		}
	})
	t.Run("during assignment", func(t *testing.T) {
		admitted, direct := 0, 0
		for iteration := range 10 {
			s := startServer(t, serverConfig{})
			a, b, c := s.dial(t, "A"), s.dial(t, "B"), s.dial(t, "C")
			ra := a.request(catAny)
			a.expectWaiting(ra)
			rb := b.request(catAny)
			if iteration%2 == 1 {
				// MATCH_WAITING is sent in the turn that schedules the pair,
				// so closing now always disconnects an assigned request.
				b.expectWaiting(rb)
			}
			b.close()
			s.awaitHandlerExit(t)
			rc := c.request(catAny)
			wa := a.expectWelcome(ra)
			wc := c.expectWelcome(rc)
			if wc.RoomID != wa.RoomID || wa.HostID != wa.PlayerID || len(wa.Players) != 2 || !reflect.DeepEqual(wc.Players, []string{wa.PlayerID, wc.PlayerID}) {
				t.Fatalf("iteration %d: %+v %+v", iteration, wa, wc)
			}
			if ghost := wa.Players[1]; ghost != wc.PlayerID {
				// B's request reached the matcher and B was paired with A
				// before its disconnect was handled: B is removed before the
				// room is published, then C joins. B is never welcomed again.
				admitted++
				a.expectPlayer(message.TypePlayerLeft, ghost)
				a.expectPlayer(message.TypePlayerJoined, wc.PlayerID)
				c.expectPlayer(message.TypePlayerJoined, wc.PlayerID)
			} else {
				if iteration%2 == 1 {
					t.Fatalf("iteration %d: B was assigned but A was not paired with B: %+v", iteration, wa)
				}
				direct++
			}
		}
		t.Logf("disconnect during assignment: paired-then-removed=%d never-paired=%d", admitted, direct)
	})
	t.Run("waiting room host", func(t *testing.T) {
		s := startServer(t, serverConfig{})
		a, b, c := s.dial(t, "A"), s.dial(t, "B"), s.dial(t, "C")
		wa, wb := pair(t, a, b, catAny)
		join(t, c, catAny, a, b)
		a.close()
		for _, m := range []*wsClient{b, c} {
			m.expectPlayer(message.TypePlayerLeft, wa.PlayerID)
			m.expectPlayer(message.TypeHostChanged, wb.PlayerID)
		}
	})
	t.Run("playing two players", func(t *testing.T) {
		s := startServer(t, serverConfig{})
		a, b, c := s.dial(t, "A"), s.dial(t, "B"), s.dial(t, "C")
		wa, wb := pair(t, a, b, catAny)
		a.start()
		expectGameStarted(t, a, b)
		a.close()
		b.expectPlayer(message.TypePlayerLeft, wa.PlayerID)
		b.expectPlayer(message.TypeHostChanged, wb.PlayerID)
		b.expectAborted(message.InsufficientPlayers)
		b.leave()
		// The surviving connection is reusable.
		pair(t, b, c, catAny)
	})
}

// REQ-ROOM-04, design 7: in playing, 2+ members continue with succession, a
// non-ready departure re-evaluates readiness once, 1 member aborts with
// INSUFFICIENT_PLAYERS into result (no restart, no refill), then the room
// closes when empty. Late commands for an old room are ignored.
func TestIntegrationDeparturesWhilePlaying(t *testing.T) {
	s := startServer(t, serverConfig{})
	answers := fixtureAnswers(t)
	a, b, c, d, e := s.dial(t, "A"), s.dial(t, "B"), s.dial(t, "C"), s.dial(t, "D"), s.dial(t, "E")
	wa, wb := pair(t, a, b, catAny)
	wc := join(t, c, catAny, a, b)
	wd := join(t, d, catAny, a, b, c)
	a.start()
	preload := expectGameStarted(t, a, b, c, d)

	// A, B and C are ready; the Room FIFO barrier proves each READY was
	// handled before D (not ready) leaves, so departure re-evaluates readiness.
	for _, p := range []*wsClient{a, b, c} {
		p.ready(1)
	}
	a.roomBarrier(message.InvalidState)
	b.roomBarrier(message.NotHost)
	c.roomBarrier(message.NotHost)
	d.leave()
	for _, p := range []*wsClient{a, b, c} {
		p.expectPlayer(message.TypePlayerLeft, wd.PlayerID)
		if r := p.expect(message.TypeRoundStarted).Payload.(*message.RoundStartPayload); r.RoundNumber != 1 || r.TotalRounds != 10 {
			t.Fatalf("round start %+v", r)
		}
	}

	// Host leaves mid-round: B succeeds and the game continues with B and C.
	a.leave()
	for _, p := range []*wsClient{b, c} {
		p.expectPlayer(message.TypePlayerLeft, wa.PlayerID)
		p.expectPlayer(message.TypeHostChanged, wb.PlayerID)
	}
	b.answer(answers[preload.ISRC])
	for _, p := range []*wsClient{b, c} {
		if r := p.expect(message.TypeRoundResult).Payload.(*message.RoundResultPayload); r.Winner != wb.PlayerID {
			t.Fatalf("round winner %+v", r)
		}
		if p2 := p.expect(message.TypePreloadSong).Payload.(*message.PreloadSongPayload); p2.RoundNumber != 2 {
			t.Fatalf("round 2 preload %+v", p2)
		}
	}

	// C leaves during preload: one member left, the game aborts into result.
	c.leave()
	b.expectPlayer(message.TypePlayerLeft, wc.PlayerID)
	b.expectAborted(message.InsufficientPlayers)
	b.roomBarrier(message.InvalidState) // result: no restart
	// A result room is not refilled: E waits and pairs with A in a new room.
	re := e.request(catAny)
	e.expectWaiting(re)
	ra := a.request(catAny)
	wa2 := a.expectWelcome(ra)
	if we := e.expectWelcome(re); we.RoomID == wb.RoomID || we.RoomID != wa2.RoomID {
		t.Fatalf("result room was refilled: %+v", we)
	}
	oldRoom := b.roomID
	b.leave()

	// Late game commands for a room the connection left are ignored.
	b.send(message.TypeReadyToPlay, message.ReadyToPlayPayload{RoomID: oldRoom, RoundNumber: 2})
	b.send(message.TypeSubmitAnswer, message.SubmitAnswerPayload{RoomID: oldRoom, Answer: "late"})
	b.send(message.TypeStartGame, message.RoomPayload{RoomID: oldRoom})
	b.lobbyBarrier()
	// And game commands with a roomID other than the current room are ignored.
	c.send(message.TypeStartGame, message.RoomPayload{RoomID: wa2.RoomID})
	c.lobbyBarrier()
	a.roomBarrier(message.NotHost)
}

// design 7 / REQ-FAIL-01: a connected member that never sends READY_TO_PLAY
// aborts the game with INTERNAL_ERROR exactly at the 30-second preload limit
// (fake clock), distinct from INSUFFICIENT_PLAYERS. Members can then leave
// and match again on the same connections.
func TestIntegrationPreloadTimeout(t *testing.T) {
	s := startServer(t, serverConfig{})
	a, b := s.dial(t, "A"), s.dial(t, "B")
	pair(t, a, b, catAny)
	a.start()
	expectGameStarted(t, a, b)
	a.ready(1)
	a.roomBarrier(message.InvalidState)

	s.clock.Advance(sc.PreloadTimeout - time.Millisecond)
	a.roomBarrier(message.InvalidState) // not aborted before the limit
	s.clock.Advance(time.Millisecond)
	a.expectAborted(message.AbortInternalError)
	b.expectAborted(message.AbortInternalError)

	a.leave()
	b.expectPlayer(message.TypePlayerLeft, a.playerID)
	b.expectPlayer(message.TypeHostChanged, b.playerID)
	b.leave()
	pair(t, a, b, catAny)
}

// REQ-ROOM-05, REQ-CONN-01, REQ-CONN-04, REQ-GAME-01~02: a full 10-round game
// on the fake clock with correct, wrong and timed-out rounds; GAME_OVER keeps
// the existing scoring; the result room refuses restart, refill and new
// requests; after LEAVE_ROOM the same connection matches again.
func TestIntegrationTenRoundGameAndRematch(t *testing.T) {
	s := startServer(t, serverConfig{})
	answers := fixtureAnswers(t)
	a, b, c, d := s.dial(t, "A"), s.dial(t, "B"), s.dial(t, "C"), s.dial(t, "D")
	wa, wb := pair(t, a, b, catAny)
	wc := join(t, c, catAny, a, b)
	players := []*wsClient{a, b, c}
	ids := []string{wa.PlayerID, wb.PlayerID, wc.PlayerID}
	// Winner index per round; -1 is a timeout. Round 8 has a wrong answer first.
	winners := []int{0, 1, 2, 0, -1, 0, 1, 2, 0, 1}
	scores := map[string]int{ids[0]: 0, ids[1]: 0, ids[2]: 0}

	// Legacy START_GAME fields are ignored; the room category, 10 songs and
	// the default time limit are authoritative.
	a.sendRaw(fmt.Sprintf(`{"type":"START_GAME","payload":{"roomID":%q,"category":{"genre":"x"},"trackCount":3,"timeLimit":5}}`, wa.RoomID))
	for _, p := range players {
		p.expect(message.TypeGameStarted)
	}
	seen := map[string]bool{}
	for round := 1; round <= 10; round++ {
		var preload *message.PreloadSongPayload
		for _, p := range players {
			preload = p.expect(message.TypePreloadSong).Payload.(*message.PreloadSongPayload)
			if preload.RoundNumber != round {
				t.Fatalf("round %d preload %+v", round, preload)
			}
		}
		if seen[preload.ISRC] || answers[preload.ISRC] == "" {
			t.Fatalf("round %d: repeated or non-pool ISRC %s", round, preload.ISRC)
		}
		seen[preload.ISRC] = true
		// A stale READY for the previous round does not count.
		if round > 1 {
			a.ready(round - 1)
		}
		for _, p := range players {
			p.ready(round)
		}
		for _, p := range players {
			r := p.expect(message.TypeRoundStarted).Payload.(*message.RoundStartPayload)
			if r.RoundNumber != round || r.TotalRounds != 10 || r.AnswerLength <= 0 || r.LetterCards == nil {
				t.Fatalf("round %d start %+v", round, r)
			}
		}
		winner := ""
		switch w := winners[round-1]; {
		case w < 0:
			s.clock.Advance(time.Second)
			for _, p := range players {
				if cd := p.expect(message.TypeCountDown).Payload.(*message.CountDownPayload); cd.Remaining != 29 {
					t.Fatalf("countdown %+v", cd)
				}
			}
			s.clock.Advance(29 * time.Second)
		default:
			if round == 8 {
				b.answer("definitely wrong")
				for _, p := range players {
					wrong := p.expect(message.TypeWrongAnswer).Payload.(*message.WrongAnswerPayload)
					if wrong.PlayerID != ids[1] || wrong.WrongAnswer != "definitely wrong" {
						t.Fatalf("wrong answer %+v", wrong)
					}
				}
			}
			winner = ids[w]
			players[w].answer(answers[preload.ISRC])
			scores[winner]++
		}
		for _, p := range players {
			r := p.expect(message.TypeRoundResult).Payload.(*message.RoundResultPayload)
			if r.Winner != winner || r.CorrectAnswer != answers[preload.ISRC] || !reflect.DeepEqual(r.Scores, scores) {
				t.Fatalf("round %d result %+v, want winner %q scores %v", round, r, winner, scores)
			}
		}
	}
	for _, p := range players {
		over := p.expect(message.TypeGameOver).Payload.(*message.GameOverPayload)
		if over.Winner != ids[0] || !reflect.DeepEqual(over.Scores, scores) {
			t.Fatalf("game over %+v", over)
		}
	}

	// Result room: no restart, no new request, no refill.
	a.roomBarrier(message.InvalidState)
	again := uuid.NewString()
	a.requestWith(again, syntheticVersion, catAny)
	a.expectFailed(again, message.AlreadyInRoom)
	rd := d.request(catAny)
	d.expectWaiting(rd)

	a.leave()
	for _, p := range []*wsClient{b, c} {
		p.expectPlayer(message.TypePlayerLeft, ids[0])
		p.expectPlayer(message.TypeHostChanged, ids[1])
	}
	// The left matched request is now a cancelled tombstone.
	a.requestWith(wa.RequestID, syntheticVersion, catAny)
	a.expectCancelled(wa.RequestID)
	// The same connection and player match again, with the waiting D (host).
	ra := a.request(catAny)
	na := a.expectWelcome(ra)
	nd := d.expectWelcome(rd)
	if na.PlayerID != ids[0] || na.RoomID == wa.RoomID || nd.RoomID != na.RoomID || na.HostID != nd.PlayerID {
		t.Fatalf("rematch: %+v %+v", na, nd)
	}
	b.leave()
	c.expectPlayer(message.TypePlayerLeft, ids[1])
	c.expectPlayer(message.TypeHostChanged, ids[2])
	c.leave()
}
