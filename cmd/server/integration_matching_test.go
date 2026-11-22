package main

import (
	"reflect"
	"testing"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	message "temp-ws/internal/message/v1"
	music "temp-ws/internal/music/v1"
)

// REQ-MATCH-01~03, REQ-CAT-03~05, REQ-ROOM-01, REQ-CANCEL-03.
// Incompatible requests stay waiting, a compatible newcomer pairs with the
// oldest compatible waiter, existing rooms win over new pairs, the oldest room
// wins among several, and a re-request receives a new arrival order.
func TestIntegrationCompatibilityAndPriority(t *testing.T) {
	s := startServer(t, serverConfig{})
	a, b, c := s.dial(t, "A"), s.dial(t, "B"), s.dial(t, "C")

	ra := a.request(catBallad2010s)
	a.expectWaiting(ra)
	rb := b.request(catPopGroupMale)
	b.expectWaiting(rb)
	// Same ID and content: no new order or registration, MATCH_WAITING again.
	a.requestWith(ra, syntheticVersion, catBallad2010s)
	a.expectWaiting(ra)

	// C (any) is compatible with both; A arrived first, so A+C pair, A hosts.
	rc := c.request(catAny)
	wc := c.expectWelcome(rc)
	wa := a.expectWelcome(ra)
	room1 := wa.RoomID
	if wc.RoomID != room1 || !reflect.DeepEqual(wa.Players, []string{wa.PlayerID, wc.PlayerID}) || wa.HostID != wa.PlayerID || wc.HostID != wa.PlayerID {
		t.Fatalf("A+C pair order/host: %+v %+v", wa, wc)
	}
	// any+specific resolves to the specific value, any+any to all (no any in rooms).
	if wa.Category != roomBallad2010s || wc.Category != roomBallad2010s {
		t.Fatalf("resolved category %+v", wa.Category)
	}

	// D wants all decades: all vs the 2010s room and vs B's 2020s both mismatch.
	d := s.dial(t, "D")
	rd := d.request(catAllDecades)
	d.expectWaiting(rd)

	// E fits room1 only, and joins it instead of waiting. Host stays A.
	e := s.dial(t, "E")
	we := join(t, e, catBallad2010s, a, c)
	if we.RoomID != room1 || we.HostID != wa.PlayerID || !reflect.DeepEqual(we.Players, []string{wa.PlayerID, wc.PlayerID, we.PlayerID}) {
		t.Fatalf("refill: %+v", we)
	}

	// F matches B's waiting request: a second room hosted by B (earlier arrival).
	f := s.dial(t, "F")
	rf := f.request(catPopGroupMale)
	wf := f.expectWelcome(rf)
	wb := b.expectWelcome(rb)
	room2 := wb.RoomID
	if room2 == room1 || wf.RoomID != room2 || wb.HostID != wb.PlayerID || wb.Category != roomPopGroupMale {
		t.Fatalf("B+F pair: %+v %+v", wb, wf)
	}

	// D cancels; then G (any) fits both rooms and joins the older one.
	d.cancel(rd)
	d.expectCancelled(rd)
	g := s.dial(t, "G")
	wg := join(t, g, catAny, a, c, e)
	if wg.RoomID != room1 || len(wg.Players) != 4 {
		t.Fatalf("oldest room priority: %+v", wg)
	}

	// D's new request on the same socket gets a fresh order and pairs with H.
	rd2 := d.request(catAllDecades)
	d.expectWaiting(rd2)
	h := s.dial(t, "H")
	rh := h.request(catAllDecades)
	wh := h.expectWelcome(rh)
	wd := d.expectWelcome(rd2)
	if wd.RoomID != wh.RoomID || wd.RoomID == room1 || wd.RoomID == room2 || wd.HostID != wd.PlayerID || wd.Category != roomAll {
		t.Fatalf("D re-request: %+v %+v", wd, wh)
	}
}

// REQ-ROOM-01, REQ-MATCH-01~02 with 11 connections: a room never exceeds 8,
// the 9th waits and pairs into a new room, and a freed seat goes to the
// oldest accepting room.
func TestIntegrationCapacityWithElevenConnections(t *testing.T) {
	s := startServer(t, serverConfig{})
	var room1 []*wsClient
	for i := range 8 {
		room1 = append(room1, s.dial(t, "P"+string(rune('1'+i))))
	}
	wa, _ := pair(t, room1[0], room1[1], catAny)
	for i := 2; i < 8; i++ {
		w := join(t, room1[i], catAny, room1[:i]...)
		if w.RoomID != wa.RoomID || len(w.Players) != i+1 || w.HostID != wa.PlayerID {
			t.Fatalf("join %d: %+v", i+1, w)
		}
	}
	ninth, tenth := s.dial(t, "P9"), s.dial(t, "P10")
	r9 := ninth.request(catAny)
	ninth.expectWaiting(r9)
	r10 := tenth.request(catAny)
	w10 := tenth.expectWelcome(r10)
	w9 := ninth.expectWelcome(r9)
	if w9.RoomID == wa.RoomID || w9.RoomID != w10.RoomID || w9.HostID != w9.PlayerID || len(w9.Players) != 2 {
		t.Fatalf("9th must open a new room with the 10th: %+v %+v", w9, w10)
	}

	leaver := room1[2]
	leaverID := leaver.playerID
	leaver.leave()
	for i, m := range room1 {
		if i != 2 {
			m.expectPlayer(message.TypePlayerLeft, leaverID)
		}
	}
	eleventh := s.dial(t, "P11")
	members := append(append([]*wsClient{}, room1[:2]...), room1[3:]...)
	w11 := join(t, eleventh, catAny, members...)
	if w11.RoomID != wa.RoomID || len(w11.Players) != 8 {
		t.Fatalf("freed seat must go to the oldest room: %+v", w11)
	}
}

// REQ-ROOM-01: two requests race for the last seat. Exactly one is admitted
// with 8 players; the other is not alerted and pairs with the next arrival.
func TestIntegrationLastSeatRace(t *testing.T) {
	for iteration := range 5 {
		s := startServer(t, serverConfig{})
		var members []*wsClient
		for i := range 7 {
			members = append(members, s.dial(t, "M"+string(rune('1'+i))))
		}
		wa, _ := pair(t, members[0], members[1], catAny)
		for i := 2; i < 7; i++ {
			join(t, members[i], catAny, members[:i]...)
		}
		x, y := s.dial(t, "X"), s.dial(t, "Y")
		rx, ry := uuid.NewString(), uuid.NewString()
		// Both requests are written before either response is read, so the
		// matchmaker may issue both registrations against the same snapshot.
		x.requestWith(rx, syntheticVersion, catAny)
		y.requestWith(ry, syntheticVersion, catAny)
		// Acceptance (MATCH_WAITING) of both precedes Z, so Z cannot overtake.
		x.expectWaiting(rx)
		y.expectWaiting(ry)
		z := s.dial(t, "Z")
		rz := z.request(catAny)
		wz := z.expectWelcome(rz)
		wx := x.expectWelcome(rx)
		wy := y.expectWelcome(ry)
		winner, won, lost := x, wx, wy
		if wy.RoomID == wa.RoomID {
			winner, won, lost = y, wy, wx
		}
		if won.RoomID != wa.RoomID || len(won.Players) != 8 || won.HostID != wa.PlayerID {
			t.Fatalf("iteration %d: last seat: %+v", iteration, won)
		}
		if lost.RoomID != wz.RoomID || wz.RoomID == wa.RoomID || !reflect.DeepEqual(wz.Players, []string{lost.PlayerID, wz.PlayerID}) || wz.HostID != lost.PlayerID {
			t.Fatalf("iteration %d: loser must open a new room with Z: %+v %+v", iteration, lost, wz)
		}
		winner.expectPlayer(message.TypePlayerJoined, winner.playerID)
		for _, m := range members {
			m.expectPlayer(message.TypePlayerJoined, winner.playerID)
		}
	}
}

// REQ-CAT-06, REQ-DATA-01, REQ-CONN-03 (server side): invalid criteria are
// MATCH_FAILED(INVALID_CRITERIA) for that requestID, replay is stable, and the
// connection stays usable for a valid request.
func TestIntegrationInvalidCriteriaAndReplay(t *testing.T) {
	s := startServer(t, serverConfig{})
	a, b := s.dial(t, "A"), s.dial(t, "B")
	cases := []struct {
		name     string
		version  string
		category music.Category
	}{
		{"catalog version mismatch", "test-v2", catAny},
		{"all on a request-only axis", syntheticVersion, music.Category{Decade: "any", ArtistType: "all", Gender: "any", Genre: "any"}},
		{"combination outside allowedRequests", syntheticVersion, music.Category{Decade: "2020s", ArtistType: "any", Gender: "any", Genre: "any"}},
		{"unknown genre tag", syntheticVersion, music.Category{Decade: "any", ArtistType: "any", Gender: "any", Genre: "trot"}},
		{"unknown decade", syntheticVersion, music.Category{Decade: "1990s", ArtistType: "any", Gender: "any", Genre: "any"}},
	}
	for _, tc := range cases {
		id := uuid.NewString()
		a.requestWith(id, tc.version, tc.category)
		a.expectFailed(id, message.InvalidCriteria)
		a.requestWith(id, tc.version, tc.category)
		a.expectFailed(id, message.InvalidCriteria)
	}
	pair(t, a, b, catAny)
}

// REQ-MATCH-04: one active request per user, one registration per request.
// Duplicates are answered by state without new order, other IDs are refused
// with ALREADY_MATCHING / ALREADY_IN_ROOM, and burst duplicates never admit
// the same player twice.
func TestIntegrationDuplicateRequests(t *testing.T) {
	s := startServer(t, serverConfig{})
	a, b, e, f := s.dial(t, "A"), s.dial(t, "B"), s.dial(t, "E"), s.dial(t, "F")

	ra := a.request(catAny)
	for range 4 {
		a.requestWith(ra, syntheticVersion, catAny)
	}
	other := uuid.NewString()
	a.requestWith(other, syntheticVersion, catAny)
	for range 5 {
		a.expectWaiting(ra)
	}
	a.expectFailed(other, message.AlreadyMatching)
	// The refused ID is a failed tombstone: replay is the same failure.
	a.requestWith(other, syntheticVersion, catAny)
	a.expectFailed(other, message.AlreadyMatching)

	rb := b.request(catAny)
	wb := b.expectWelcome(rb)
	wa := a.expectWelcome(ra)
	if !reflect.DeepEqual(wa.Players, []string{wa.PlayerID, wb.PlayerID}) {
		t.Fatalf("original request lost its order: %+v", wa)
	}
	// Matched duplicate: the stored WELCOME is replayed, no new membership.
	a.requestWith(ra, syntheticVersion, catAny)
	if replay := a.expectWelcome(ra); !reflect.DeepEqual(replay, wa) {
		t.Fatalf("WELCOME replay changed: %+v vs %+v", replay, wa)
	}
	inRoom := uuid.NewString()
	a.requestWith(inRoom, syntheticVersion, catAny)
	a.expectFailed(inRoom, message.AlreadyInRoom)

	// Burst: three identical frames written before reading. Each frame gets
	// exactly one reply (WAITING or WELCOME replay) plus one admission
	// WELCOME and one PLAYER_JOINED: five frames in total.
	re := uuid.NewString()
	for range 3 {
		e.requestWith(re, syntheticVersion, catAny)
	}
	joined, welcomes := 0, 0
	for range 5 {
		msg := e.next()
		switch p := msg.Payload.(type) {
		case *message.RequestPayload:
			if msg.Type != message.TypeMatchWaiting || p.RequestID != re {
				t.Fatalf("burst reply %s %+v", msg.Type, p)
			}
		case *message.WelcomePayload:
			e.checkWelcome(p, re)
			if p.RoomID != wa.RoomID || len(p.Players) != 3 {
				t.Fatalf("burst WELCOME %+v", p)
			}
			welcomes++
		case *message.PlayerPayload:
			if msg.Type != message.TypePlayerJoined || p.PlayerID != e.playerID {
				t.Fatalf("burst room event %s %+v", msg.Type, p)
			}
			joined++
		default:
			t.Fatalf("unexpected burst frame %s", msg.Type)
		}
	}
	if joined != 1 || welcomes < 1 {
		t.Fatalf("burst produced %d PLAYER_JOINED and %d WELCOME", joined, welcomes)
	}
	// Members saw exactly one join for E: the next event is F's join.
	a.expectPlayer(message.TypePlayerJoined, e.playerID)
	b.expectPlayer(message.TypePlayerJoined, e.playerID)
	wf := join(t, f, catAny, a, b, e)
	if len(wf.Players) != 4 {
		t.Fatalf("duplicate membership: %+v", wf)
	}
}

// REQ-CANCEL-01~03 and protocol tombstones: cancel in waiting and matched
// states, repeated cancel, cancelled IDs are never resurrected, unknown-ID
// cancel creates a tombstone, and an old LEAVE_ROOM after cancellation is
// acknowledged without touching anybody's current room.
func TestIntegrationCancelTombstones(t *testing.T) {
	s := startServer(t, serverConfig{})
	a, b, c := s.dial(t, "A"), s.dial(t, "B"), s.dial(t, "C")

	r1 := a.request(catAny)
	a.expectWaiting(r1)
	a.cancel(r1)
	a.expectCancelled(r1)
	a.cancel(r1)
	a.expectCancelled(r1)
	a.requestWith(r1, syntheticVersion, catAny)
	a.expectCancelled(r1)

	unknown := uuid.NewString()
	a.cancel(unknown)
	a.expectCancelled(unknown)
	a.requestWith(unknown, syntheticVersion, catAny)
	a.expectCancelled(unknown)

	// The cancelled request is gone: B pairs with A's NEW request, not a ghost.
	wa, wb := pair(t, a, b, catAny)
	join(t, c, catAny, a, b)

	// Cancelling a matched request removes the membership before replying.
	a.cancel(wa.RequestID)
	a.expectCancelled(wa.RequestID)
	for _, m := range []*wsClient{b, c} {
		m.expectPlayer(message.TypePlayerLeft, wa.PlayerID)
		m.expectPlayer(message.TypeHostChanged, wb.PlayerID)
	}
	a.requestWith(wa.RequestID, syntheticVersion, catAny)
	a.expectCancelled(wa.RequestID)
	// A stale LEAVE_ROOM for the old room is acknowledged and harmless.
	a.send(message.TypeLeaveRoom, message.RoomPayload{RoomID: wa.RoomID})
	if p := a.expect(message.TypeRoomLeft).Payload.(*message.RoomPayload); p.RoomID != wa.RoomID {
		t.Fatal("stale ROOM_LEFT roomID")
	}
	// The same connection rejoins the same (still waiting) room.
	w := join(t, a, catAny, b, c)
	if w.RoomID != wa.RoomID || w.HostID != wb.PlayerID || w.PlayerID != wa.PlayerID {
		t.Fatalf("rejoin after cancel: %+v", w)
	}
}

// REQ-CANCEL-02~03, protocol "등록 중 취소" and "취소 후 재요청": MATCH_REQUEST
// R1, MATCH_CANCEL R1 and MATCH_REQUEST R2 are written back-to-back while a
// partner waits, so R1 is cancelled while its registration may be in flight.
// Whatever the interleaving, a late R1 result never reaches the new request:
// R2 is accepted with MATCH_WAITING, MATCH_CANCELLED(R1) is sent only after R1
// is cleaned up, no WELCOME(R1) follows it, and WELCOME(R2) comes last.
func TestIntegrationCancelDuringAssignmentThenRequest(t *testing.T) {
	lateWelcome := 0
	for iteration := range 20 {
		s := startServer(t, serverConfig{})
		a, b := s.dial(t, "A"), s.dial(t, "B")
		ra := a.request(catAny)
		a.expectWaiting(ra)
		r1, r2 := uuid.NewString(), uuid.NewString()
		b.requestWith(r1, syntheticVersion, catAny)
		b.cancel(r1)
		b.requestWith(r2, syntheticVersion, catAny)

		b.expectWaiting(r1)
		cancelled, waiting2, welcomed1 := false, false, false
		var w2 *message.WelcomePayload
		for w2 == nil {
			msg := b.next()
			switch p := msg.Payload.(type) {
			case *message.RequestPayload:
				switch {
				case msg.Type == message.TypeMatchCancelled && p.RequestID == r1 && !cancelled:
					cancelled = true
				case msg.Type == message.TypeMatchWaiting && p.RequestID == r2 && !waiting2:
					waiting2 = true
				default:
					t.Fatalf("iteration %d: unexpected %s %+v", iteration, msg.Type, p)
				}
			case *message.WelcomePayload:
				switch {
				case p.RequestID == r1 && !cancelled && !welcomed1:
					welcomed1 = true
				case p.RequestID == r2 && cancelled && waiting2:
					b.checkWelcome(p, r2)
					w2 = p
				default:
					t.Fatalf("iteration %d: WELCOME %+v out of order (cancelled=%v waiting2=%v)", iteration, p, cancelled, waiting2)
				}
			case *message.PlayerPayload:
				// Only possible inside the R1 membership, before cancellation;
				// next() already rejects room events outside a membership.
				if !welcomed1 || cancelled {
					t.Fatalf("iteration %d: room event %s outside R1 membership", iteration, msg.Type)
				}
			default:
				t.Fatalf("iteration %d: unexpected %s", iteration, msg.Type)
			}
		}
		if welcomed1 {
			lateWelcome++
		}
		b.expectPlayer(message.TypePlayerJoined, w2.PlayerID)

		wa := a.expectWelcome(ra)
		if wa.RoomID != w2.RoomID || !reflect.DeepEqual(wa.Players, []string{wa.PlayerID, w2.PlayerID}) || wa.HostID != wa.PlayerID {
			t.Fatalf("iteration %d: A %+v, R2 %+v", iteration, wa, w2)
		}
		a.expectPlayer(message.TypePlayerLeft, w2.PlayerID)
		a.expectPlayer(message.TypePlayerJoined, w2.PlayerID)
		if !reflect.DeepEqual(w2.Players, wa.Players) || w2.HostID != wa.PlayerID {
			t.Fatalf("iteration %d: R2 membership %+v", iteration, w2)
		}
		// R1 stays a cancelled tombstone; R2 replays its WELCOME.
		b.requestWith(r1, syntheticVersion, catAny)
		b.expectCancelled(r1)
		b.requestWith(r2, syntheticVersion, catAny)
		if replay := b.expectWelcome(r2); replay.RoomID != w2.RoomID {
			t.Fatalf("iteration %d: replay %+v", iteration, replay)
		}
	}
	t.Logf("cancel during assignment: WELCOME(R1) delivered before cancel in %d/20", lateWelcome)
}

// protocol 멱등성: a connection may register at most 4096 distinct request IDs;
// the next new ID closes it with 1008. Replies are read as they arrive so the
// bounded outbound queue never decides the outcome.
func TestIntegrationRequestIDLimitClosesWithPolicyViolation(t *testing.T) {
	s := startServer(t, serverConfig{})
	c := s.dial(t, "C")
	for i := 0; i < message.MaxRequestIDs; i++ {
		id := uuid.NewString()
		c.cancel(id)
		c.expectCancelled(id)
	}
	c.cancel(uuid.NewString())
	_, _, err := c.conn.Read(c.ctx)
	if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatalf("want close 1008 after %d request IDs, got %v", message.MaxRequestIDs, err)
	}
}
