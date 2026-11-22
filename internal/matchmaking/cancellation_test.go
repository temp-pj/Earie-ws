package matchmaking

import (
	"testing"
	"time"

	"github.com/google/uuid"
	message "temp-ws/internal/message/v1"
	sc "temp-ws/internal/sessioncontract"
)

func cancelRequest(m *Matchmaker, r *request) {
	m.accept(input{connection: r.connection, cancelID: r.payload.RequestID})
	m.schedule()
}
func completeRemoval(m *Matchmaker, cmd sc.RemoveCommand) {
	b := advance(m, cmd.RoomID, sc.RemovalResult{Token: cmd.Token, PlayerID: cmd.PlayerID, RequestID: cmd.RequestID})
	b.Snapshot.PlayerCount--
	m.apply(b)
	m.schedule()
}
func completeRegistration(m *Matchmaker, cmd sc.RegisterCommand) sc.EventBatch {
	b := advance(m, cmd.RoomID, sc.RegistrationResult{Token: cmd.Token, Member: cmd.Member, Admission: sc.Admission{HostID: cmd.Member.PlayerID, Players: []string{cmd.Member.PlayerID}, State: sc.RoomWaiting}})
	m.apply(b)
	return b
}
func countMessages(out *transport, kind string) int {
	n := 0
	for _, msg := range out.messages {
		if msg.Type == kind {
			n++
		}
	}
	return n
}
func processInputs(m *Matchmaker) {
	m.mu.Lock()
	inputs := m.inputs
	m.inputs = nil
	m.mu.Unlock()
	for _, in := range inputs {
		m.accept(in)
		m.schedule()
	}
}

func TestCancelWaitingAndUnknownTombstones(t *testing.T) {
	m, g := newMatcher(t)
	c, out := connect(m)
	r := submit(m, c, anyCategory)
	cancelRequest(m, r)
	if out.last().Type != message.TypeMatchCancelled || m.requests[c.ID] != nil {
		t.Fatal("waiting cancellation incomplete")
	}
	m.accept(input{connection: c, payload: r.payload})
	if out.last().Type != message.TypeMatchCancelled || len(g.pairs) != 0 {
		t.Fatal("cancelled ID resurrected")
	}
	unknown := uuid.NewString()
	m.accept(input{connection: c, cancelID: unknown})
	p := message.MatchRequestPayload{RequestID: unknown, CatalogVersion: "test-v1", Category: anyCategory}
	m.accept(input{connection: c, payload: p})
	if out.last().Payload.(message.MatchCancelledPayload).RequestID != unknown || m.requests[c.ID] != nil {
		t.Fatal("unknown cancellation resurrected")
	}
	next := submit(m, c, anyCategory)
	if next.member.ArrivalSequence <= r.member.ArrivalSequence {
		t.Fatal("new request has no new order")
	}
	// The same external UUID on a different connection has independent ownership.
	other, otherOut := connect(m)
	m.accept(input{connection: other, cancelID: next.payload.RequestID})
	if m.requests[c.ID] != next || otherOut.last().Type != message.TypeMatchCancelled {
		t.Fatal("cross-connection cancellation")
	}
}

func TestCancelAssigningAndImmediateReplacement(t *testing.T) {
	for _, accepted := range []bool{true, false} {
		t.Run(map[bool]string{true: "late success", false: "late rejection"}[accepted], func(t *testing.T) {
			m, g := newMatcher(t)
			id := candidate(m, 1, resolved(m, anyCategory))
			c, out := connect(m)
			old := submit(m, c, anyCategory)
			cmd := g.registrations[0]
			cancelRequest(m, old)
			next := submit(m, c, anyCategory)
			cancelRequest(m, old)
			m.accept(input{connection: c, payload: old.payload})
			if len(g.registrations) != 1 || countMessages(out, message.TypeMatchCancelled) != 0 || next.member.ArrivalSequence <= old.member.ArrivalSequence {
				t.Fatal("cleanup barrier bypassed")
			}
			if accepted {
				completeRegistration(m, cmd)
				if len(g.removals) != 1 || countMessages(out, message.TypeWelcome) != 0 {
					t.Fatal("late success was welcomed or not removed")
				}
				// Wrong token/member cannot complete removal and release the replacement.
				remove := g.removals[0]
				m.apply(advance(m, id, sc.RemovalResult{Token: remove.Token + 1000, PlayerID: remove.PlayerID, RequestID: remove.RequestID}))
				m.schedule()
				if len(g.registrations) != 1 {
					t.Fatal("unrelated removal released barrier")
				}
				completeRemoval(m, remove)
			} else {
				m.apply(advance(m, id, sc.RegistrationResult{Token: cmd.Token, Member: cmd.Member, Rejection: sc.AdmissionInternalError}))
				m.schedule()
			}
			if m.requests[c.ID] != next || len(g.registrations) != 2 || countMessages(out, message.TypeMatchCancelled) != 1 || countMessages(out, message.TypeMatchFailed) != 0 {
				t.Fatal("old completion damaged replacement")
			}
			completeRegistration(m, g.registrations[1])
			before := len(out.messages)
			completeRegistration(m, cmd) // higher revision duplicate must still be ignored
			m.apply(advance(m, id, sc.RegistrationResult{Token: cmd.Token, Member: cmd.Member, Rejection: sc.AdmissionInternalError}))
			if len(g.removals) > 1 || len(out.messages) != before || m.requests[c.ID] != next || next.state != sc.RequestMatched {
				t.Fatal("stale result changed new membership")
			}
		})
	}
}

func TestCancelInitialPairOneOrBothBeforeResult(t *testing.T) {
	for _, both := range []bool{false, true} {
		t.Run(map[bool]string{false: "one", true: "both"}[both], func(t *testing.T) {
			m, g := newMatcher(t)
			a, oa := connect(m)
			b, ob := connect(m)
			ra := submit(m, a, anyCategory)
			rb := submit(m, b, anyCategory)
			cancelRequest(m, ra)
			if both {
				cancelRequest(m, rb)
			}
			replacement := submit(m, a, anyCategory)
			m.apply(pairBatch(g, 0, sc.AdmissionAccepted))
			want := 1
			if both {
				want = 2
			}
			if len(g.removals) != want || len(g.publications) != 0 || countMessages(oa, message.TypeWelcome) != 0 {
				t.Fatal("initial cancelled membership published")
			}
			removals := append([]sc.RemoveCommand(nil), g.removals...)
			for _, cmd := range removals {
				completeRemoval(m, cmd)
			}
			if m.requests[a.ID] != replacement {
				t.Fatal("initial cleanup erased replacement")
			}
			if both {
				if len(g.publications) != 0 || len(g.closes) != 1 || countMessages(ob, message.TypeMatchCancelled) != 1 {
					t.Fatal("empty initial room not closed")
				}
			} else {
				if len(g.publications) != 1 || countMessages(ob, message.TypeWelcome) != 1 {
					t.Fatal("survivor not published")
				}
				roomID := g.handles[0].RoomID
				batch := advance(m, roomID, sc.Published{Token: g.publications[0].Token})
				batch.Snapshot.Accepting = true
				m.apply(batch)
				m.schedule()
				if len(g.registrations) != 1 || g.registrations[0].Member != replacement.member {
					t.Fatal("replacement not dispatched after publication")
				}
			}
		})
	}
}

func TestCancelMatchedAndLeaveTombstones(t *testing.T) {
	for _, leave := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "leave"}[leave], func(t *testing.T) {
			m, g := newMatcher(t)
			id := candidate(m, 1, resolved(m, anyCategory))
			c, out := connect(m)
			old := submit(m, c, anyCategory)
			completeRegistration(m, g.registrations[0])
			if leave {
				if err := m.registry.Route(c.ID, message.Message{Type: message.TypeLeaveRoom, Payload: &message.LeaveRoomPayload{RoomID: id}}); err != nil {
					t.Fatal(err)
				}
				processInputs(m)
			} else {
				cancelRequest(m, old)
			}
			if len(g.removals) != 1 {
				t.Fatal("missing scoped cleanup")
			}
			completeRemoval(m, g.removals[0])
			next := submit(m, c, anyCategory)
			completeRegistration(m, g.registrations[1])
			n := countMessages(out, message.TypeWelcome)
			m.accept(input{connection: c, payload: old.payload})
			// Old removal/result cannot remove new membership even in the SAME room.
			completeRemoval(m, g.removals[0])
			completeRegistration(m, g.registrations[0])
			if m.requests[c.ID] != next || next.state != sc.RequestMatched || countMessages(out, message.TypeWelcome) != n || out.last().Type != message.TypeMatchCancelled {
				t.Fatal("left request replay resurrected or damaged membership")
			}
			if err := m.registry.Route(c.ID, message.Message{Type: message.TypeLeaveRoom, Payload: &message.LeaveRoomPayload{RoomID: id}}); err != nil {
				t.Fatal(err)
			}
			processInputs(m)
			if g.removals[len(g.removals)-1].RequestID != next.payload.RequestID {
				t.Fatal("registry lost new membership")
			}
		})
	}
}

func TestPayloadChangesCloseAndFailedReplay(t *testing.T) {
	for _, terminal := range []string{"waiting", "cancelled", "failed", "unknown"} {
		t.Run(terminal, func(t *testing.T) {
			m, _ := newMatcher(t)
			c, out := connect(m)
			p := message.MatchRequestPayload{RequestID: uuid.NewString(), CatalogVersion: "test-v1", Category: anyCategory}
			if terminal == "unknown" {
				m.accept(input{connection: c, cancelID: p.RequestID})
			}
			if terminal == "failed" {
				p.CatalogVersion = "invalid"
			}
			m.accept(input{connection: c, payload: p})
			if terminal == "cancelled" {
				cancelRequest(m, m.requests[c.ID])
			}
			original := out.last()
			m.accept(input{connection: c, payload: p})
			if out.last().Type != original.Type {
				t.Fatal("duplicate outcome changed")
			}
			p.Category = ballad
			m.accept(input{connection: c, payload: p})
			processInputs(m)
			if !out.stopped || m.registry.Connected(c.ID) || len(m.history[c.ID]) != 0 {
				t.Fatal("changed payload did not close and clean session")
			}
		})
	}
}

func TestRequestHistoryLimitAndDisconnectCleanup(t *testing.T) {
	for _, cancelOnly := range []bool{false, true} {
		m, _ := newMatcher(t)
		c, out := connect(m)
		var first input
		for i := 0; i < maxRequestIDs; i++ {
			in := input{connection: c, payload: message.MatchRequestPayload{RequestID: uuid.NewString(), CatalogVersion: "invalid", Category: anyCategory}}
			if cancelOnly {
				in.cancelID = in.payload.RequestID
			}
			if i == 0 {
				first = in
			}
			m.accept(in)
		}
		m.accept(first)
		if out.stopped || len(m.history[c.ID]) != maxRequestIDs {
			t.Fatal("duplicate consumed ID quota")
		}
		m.accept(input{connection: c, cancelID: uuid.NewString()})
		processInputs(m)
		if !out.stopped || len(m.history) != 0 || len(m.requests) != 0 {
			t.Fatal("new ID over quota was retained")
		}
	}
}

func TestDisconnectKeepsPendingCleanupUntilLateResult(t *testing.T) {
	m, g := newMatcher(t)
	candidate(m, 1, resolved(m, anyCategory))
	c, _ := connect(m)
	r := submit(m, c, anyCategory)
	m.registry.Disconnect(c.ID)
	processInputs(m)
	if m.history[c.ID][r.payload.RequestID] != r || m.requests[c.ID] != nil {
		t.Fatal("pending disconnect operation lost")
	}
	completeRegistration(m, g.registrations[0])
	completeRemoval(m, g.removals[0])
	if len(m.history) != 0 {
		t.Fatal("disconnected history leaked")
	}
}

func TestTimeoutQuarantineWaitsForDoneAndDrain(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "cancelled"}[cancelled], func(t *testing.T) {
			m, g := newMatcher(t)
			now := time.Unix(100, 0)
			m.now = func() time.Time { return now }
			id := candidate(m, 1, resolved(m, anyCategory))
			other := candidate(m, 2, resolved(m, pop))
			c, out := connect(m)
			old := submit(m, c, anyCategory)
			var next *request
			if cancelled {
				cancelRequest(m, old)
				next = submit(m, c, anyCategory)
			}
			now = now.Add(sc.OperationTimeout - time.Nanosecond)
			m.expire(now)
			if len(g.closes) != 0 {
				t.Fatal("early timeout")
			}
			now = now.Add(time.Nanosecond)
			m.expire(now)
			m.schedule()
			if len(g.closes) != 1 || !m.rooms[id].closing || len(g.registrations) != 1 || countMessages(out, message.TypeMatchFailed) != 0 || countMessages(out, message.TypeMatchCancelled) != 0 {
				t.Fatal("timeout pretended cleanup")
			}
			completeRegistration(m, g.registrations[0]) // late success during quarantine
			m.apply(advance(m, id, sc.Closed{}))
			m.schedule()
			if len(out.messages) > 3 || len(g.removals) != 0 || len(g.registrations) != 1 {
				t.Fatal("terminal result escaped quarantine")
			}
			// Independent connections continue through the central loop.
			d, _ := connect(m)
			submit(m, d, pop)
			if g.registrations[len(g.registrations)-1].RoomID != other {
				t.Fatal("quarantined room blocked independent work")
			}
			// Model the real Run ordering: Done notification, queued event drain, retire.
			events := make(chan sc.EventBatch, 1)
			events <- advance(m, id, sc.RegistrationResult{Token: g.registrations[0].Token, Member: old.member})
			m.events = events
			done := make(chan struct{})
			m.rooms[id].handle.Done = done
			close(done)
			m.drain()
			m.retire(id)
			m.schedule()
			if cancelled {
				if countMessages(out, message.TypeMatchCancelled) != 1 || m.requests[c.ID] != next || next.state != sc.RequestAssigning || out.stopped {
					t.Fatal("cancelled timeout harmed replacement")
				}
			} else {
				if out.last().Payload.(message.MatchFailedPayload).Code != message.MatchInternalError || m.requests[c.ID] != nil {
					t.Fatal("active timeout not failed")
				}
				m.accept(input{connection: c, payload: old.payload})
				if countMessages(out, message.TypeMatchFailed) != 2 {
					t.Fatal("failed timeout not replayed")
				}
			}
			if _, ok := m.rooms[id]; ok {
				t.Fatal("terminal room leaked")
			}
		})
	}
}

func TestRemovalFaultQuarantinesCancelledMembership(t *testing.T) {
	for _, enqueueFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "no response", true: "enqueue failure"}[enqueueFailure], func(t *testing.T) {
			m, g := newMatcher(t)
			now := time.Unix(200, 0)
			m.now = func() time.Time { return now }
			id := candidate(m, 1, resolved(m, anyCategory))
			c, out := connect(m)
			old := submit(m, c, anyCategory)
			completeRegistration(m, g.registrations[0])
			if enqueueFailure {
				g.removeError = sc.ErrBackpressure
			}
			cancelRequest(m, old)
			replacement := submit(m, c, anyCategory)
			if !enqueueFailure {
				now = now.Add(sc.OperationTimeout)
				m.expire(now)
			}
			if !m.rooms[id].closing || len(g.registrations) != 1 || countMessages(out, message.TypeMatchCancelled) != 0 {
				t.Fatal("remove failure released barrier")
			}
			// A late removal does not shorten the quarantine: only Done/drain can.
			completeRemoval(m, g.removals[0])
			if len(g.registrations) != 1 || countMessages(out, message.TypeMatchCancelled) != 0 {
				t.Fatal("late removal bypassed quarantine")
			}
			done := make(chan struct{})
			m.rooms[id].handle.Done = done
			close(done)
			m.drain()
			m.retire(id)
			m.schedule()
			if out.stopped || m.requests[c.ID] != replacement || m.blocked(c.ID) || countMessages(out, message.TypeMatchCancelled) != 1 {
				t.Fatal("cancelled membership could not reuse connection after fault")
			}
		})
	}
}

func TestInitialPairTimeoutAndStalledDone(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "cancelled"}[cancelled], func(t *testing.T) {
			m, g := newMatcher(t)
			now := time.Unix(300, 0)
			m.now = func() time.Time { return now }
			a, oa := connect(m)
			b, ob := connect(m)
			ra := submit(m, a, anyCategory)
			submit(m, b, anyCategory)
			if cancelled {
				cancelRequest(m, ra)
				submit(m, a, anyCategory)
			}
			now = now.Add(sc.OperationTimeout)
			m.expire(now)
			id := g.handles[0].RoomID
			if !m.rooms[id].closing || len(g.closes) != 1 {
				t.Fatal("initial operation did not time out")
			}
			now = now.Add(sc.OperationTimeout)
			m.expire(now)
			processInputs(m)
			if !oa.stopped || !ob.stopped || countMessages(oa, message.TypeMatchCancelled) != 0 || countMessages(oa, message.TypeMatchFailed) != 0 || len(m.rooms) != 1 {
				t.Fatal("stalled Done gave false completion or dropped quarantine")
			}
			m.apply(pairBatch(g, 0, sc.AdmissionAccepted))
			m.expire(now.Add(time.Hour))
			processInputs(m)
			if countMessages(oa, message.TypeWelcome) != 0 || len(g.pairs) != 1 || len(g.removals) != 0 {
				t.Fatal("stalled actor late result reactivated work")
			}
			done := make(chan struct{})
			m.rooms[id].handle.Done = done
			close(done)
			m.drain()
			m.retire(id)
			if len(m.history) != 0 || len(m.requests) != 0 || len(m.rooms) != 0 {
				t.Fatal("disconnected quarantine retained records after Done")
			}
		})
	}
}

func TestTerminalRoomIgnoresAllLateEvents(t *testing.T) {
	m, g := newMatcher(t)
	id := candidate(m, 1, resolved(m, anyCategory))
	c, out := connect(m)
	r := submit(m, c, anyCategory)
	m.apply(advance(m, id, sc.Closed{}))
	snapshot := m.rooms[id].snapshot
	completeRegistration(m, g.registrations[0])
	m.apply(advance(m, id, sc.WireEvent{Recipients: []sc.Member{r.member}, Message: message.Message{Type: message.TypePlayerJoined}}))
	if m.rooms[id].snapshot != snapshot || countMessages(out, message.TypeWelcome) != 0 || r.state != sc.RequestAssigning {
		t.Fatal("terminal room accepted a late event")
	}
	m.retire(id)
	if out.last().Payload.(message.MatchFailedPayload).Code != message.MatchInternalError {
		t.Fatal("closed operation not failed after retirement")
	}
	m.apply(sc.EventBatch{RoomID: id, Revision: 100, Snapshot: snapshot})
	if len(m.rooms) != 0 {
		t.Fatal("retired room resurrected")
	}
}

func TestMatchedRoomFailureStopsConnectionsAfterDone(t *testing.T) {
	m, g := newMatcher(t)
	now := time.Unix(400, 0)
	m.now = func() time.Time { return now }
	id := candidate(m, 1, resolved(m, anyCategory))
	a, oa := connect(m)
	submit(m, a, anyCategory)
	completeRegistration(m, g.registrations[0])
	b, ob := connect(m)
	submit(m, b, anyCategory)
	now = now.Add(sc.OperationTimeout)
	m.expire(now)
	m.retire(id)
	if !oa.stopped || ob.stopped || ob.last().Payload.(message.MatchFailedPayload).Code != message.MatchInternalError {
		t.Fatal("room fault mixed admitted and assigning outcomes")
	}
}

func TestLeaveRemovalIsTimedAndCancelledCompletionIsScoped(t *testing.T) {
	m, g := newMatcher(t)
	now := time.Unix(500, 0)
	m.now = func() time.Time { return now }
	id := candidate(m, 1, resolved(m, anyCategory))
	c, out := connect(m)
	r := submit(m, c, anyCategory)
	completeRegistration(m, g.registrations[0])
	for range 2 {
		if err := m.registry.Route(c.ID, message.Message{Type: message.TypeLeaveRoom, Payload: &message.LeaveRoomPayload{RoomID: id}}); err != nil {
			t.Fatal(err)
		}
	}
	processInputs(m)
	if len(g.removals) != 1 || len(m.rooms[id].deadlines) != 1 {
		t.Fatal("leave bypassed deadline tracking or duplicated removal")
	}
	cancelRequest(m, r)
	if len(g.removals) != 1 {
		t.Fatal("cancel duplicated an already pending removal")
	}
	completeRemoval(m, g.removals[0])
	if countMessages(out, message.TypeRoomLeft) != 2 || countMessages(out, message.TypeMatchCancelled) != 1 {
		t.Fatal("shared removal did not complete leave and cancellation")
	}
	if countMessages(out, message.TypeMatchCancelled) != 1 || len(m.rooms[id].deadlines) != 0 {
		t.Fatal("cleanup deadlines or cancellation retained")
	}
	now = now.Add(sc.OperationTimeout)
	m.expire(now)
	if len(g.closes) != 0 {
		t.Fatal("completed cleanup timed out")
	}
}

func TestQueuedOldLeaveCannotTimeoutReplacement(t *testing.T) {
	m, g := newMatcher(t)
	now := time.Unix(600, 0)
	m.now = func() time.Time { return now }
	id := candidate(m, 1, resolved(m, anyCategory))
	c, out := connect(m)
	old := submit(m, c, anyCategory)
	completeRegistration(m, g.registrations[0])
	// Queue a leave but deliver the independent cancellation completion first.
	if err := m.registry.Route(c.ID, message.Message{Type: message.TypeLeaveRoom, Payload: &message.LeaveRoomPayload{RoomID: id}}); err != nil {
		t.Fatal(err)
	}
	cancelRequest(m, old)
	completeRemoval(m, g.removals[0])
	next := submit(m, c, anyCategory)
	completeRegistration(m, g.registrations[1])
	processInputs(m)
	if len(g.removals) != 1 || len(m.rooms[id].deadlines) != 0 {
		t.Fatal("queued old leave reissued cleanup after proven removal")
	}
	if countMessages(out, message.TypeRoomLeft) != 1 {
		t.Fatal("queued leave not acknowledged")
	}
	now = now.Add(sc.OperationTimeout)
	m.expire(now)
	if len(g.closes) != 0 || out.stopped || m.requests[c.ID] != next {
		t.Fatal("old cleanup damaged replacement")
	}
	if err := m.registry.Route(c.ID, message.Message{Type: message.TypeLeaveRoom, Payload: &message.LeaveRoomPayload{RoomID: id}}); err != nil {
		t.Fatal(err)
	}
	processInputs(m)
	if g.removals[len(g.removals)-1].RequestID != next.payload.RequestID {
		t.Fatal("old cleanup cleared new registry membership")
	}
}

func TestCancelledDoneCompletesQueuedLeaveWithoutStoppingReplacement(t *testing.T) {
	m, g := newMatcher(t)
	id := candidate(m, 1, resolved(m, anyCategory))
	c, out := connect(m)
	old := submit(m, c, anyCategory)
	completeRegistration(m, g.registrations[0])
	if err := m.registry.Route(c.ID, message.Message{Type: message.TypeLeaveRoom, Payload: &message.LeaveRoomPayload{RoomID: id}}); err != nil {
		t.Fatal(err)
	}
	g.removeError = sc.ErrClosed
	cancelRequest(m, old)
	next := submit(m, c, anyCategory)
	done := make(chan struct{})
	m.rooms[id].handle.Done = done
	close(done)
	m.drain()
	m.retire(id)
	processInputs(m)
	if out.stopped || countMessages(out, message.TypeRoomLeft) != 1 || countMessages(out, message.TypeMatchCancelled) != 1 || m.requests[c.ID] != next {
		t.Fatal("old pending leave stopped replacement after Done")
	}
}

func TestChangedPayloadDuringAdmissionAndCleanup(t *testing.T) {
	for _, phase := range []string{"assigning", "matched", "removing"} {
		t.Run(phase, func(t *testing.T) {
			m, g := newMatcher(t)
			candidate(m, 1, resolved(m, anyCategory))
			c, out := connect(m)
			r := submit(m, c, anyCategory)
			if phase != "assigning" {
				completeRegistration(m, g.registrations[0])
			}
			if phase == "removing" {
				cancelRequest(m, r)
			}
			changed := r.payload
			changed.CatalogVersion = "changed"
			m.accept(input{connection: c, payload: changed})
			processInputs(m)
			if !out.stopped || m.registry.Connected(c.ID) {
				t.Fatal("changed payload kept session alive")
			}
			if phase == "assigning" {
				completeRegistration(m, g.registrations[0])
			}
			if len(g.removals) != 1 {
				t.Fatal("disconnect duplicated or lost pending removal")
			}
			completeRemoval(m, g.removals[0])
			if len(m.history) != 0 || len(m.requests) != 0 {
				t.Fatal("closed connection retained history")
			}
		})
	}
}

func TestZeroTokenCannotReopenCancelledAdmission(t *testing.T) {
	m, g := newMatcher(t)
	id := candidate(m, 1, resolved(m, anyCategory))
	c, out := connect(m)
	r := submit(m, c, anyCategory)
	cancelRequest(m, r)
	completeRegistration(m, g.registrations[0])
	remove := g.removals[0]
	deadline := m.rooms[id].deadlines[remove.Token]
	m.apply(advance(m, id, sc.RegistrationResult{Member: r.member}))
	if len(g.removals) != 1 || m.rooms[id].deadlines[remove.Token] != deadline || countMessages(out, message.TypeWelcome) != 0 {
		t.Fatal("zero token revived old admission")
	}
	completeRemoval(m, remove)
	if countMessages(out, message.TypeMatchCancelled) != 1 {
		t.Fatal("cancellation lost")
	}
}
