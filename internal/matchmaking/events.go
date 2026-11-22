package matchmaking

import (
	message "temp-ws/internal/message/v1"
	sc "temp-ws/internal/sessioncontract"
)

func (m *Matchmaker) apply(batch sc.EventBatch) {
	room := m.rooms[batch.RoomID]
	if room == nil || room.closing || batch.Revision <= room.snapshot.Revision {
		return
	}
	if room.public {
		clear(m.failedPairs)
	}
	room.snapshot = batch.Snapshot
	var approved []sc.Member
	var removed []*request
	forwarded := batch
	forwarded.Events = make([]sc.Event, 0, len(batch.Events))
	for _, event := range batch.Events {
		switch e := event.(type) {
		case sc.InitialPairResult:
			a, b := m.pending(batch.RoomID, e.Token, e.Members[0]), m.pending(batch.RoomID, e.Token, e.Members[1])
			if a == nil || b == nil {
				continue
			}
			delete(room.deadlines, e.Token)
			if e.Rejection != sc.AdmissionAccepted {
				m.failedPairs[pairKey(a, b)] = true
				for _, r := range []*request{a, b} {
					m.retry(r, batch)
				}
				m.closeRoom(room)
				continue
			}
			room.initialComplete = true
			for _, r := range []*request{a, b} {
				if m.admit(r, room, e.Admission) {
					approved = append(approved, r.member)
				}
			}
		case sc.RegistrationResult:
			r := m.pending(batch.RoomID, e.Token, e.Member)
			if r == nil {
				continue
			}
			delete(room.deadlines, e.Token)
			if e.Rejection != sc.AdmissionAccepted {
				m.retry(r, batch)
				continue
			}
			if m.admit(r, room, e.Admission) {
				approved = append(approved, r.member)
			}
		case sc.RemovalResult:
			cmd, ok := room.cleanup[e.Token]
			if !ok || cmd.PlayerID != e.PlayerID || cmd.RequestID != e.RequestID {
				continue
			}
			delete(room.cleanup, e.Token)
			delete(room.deadlines, e.Token)
			if r := m.memberRequest(cmd); r != nil && r.token == 0 {
				removed = append(removed, r)
			}
		case sc.Published:
			if e.Token == room.publishToken && e.Token != 0 {
				delete(room.deadlines, e.Token)
				room.public = true
				clear(m.failedPairs)
			}
		case sc.Closed:
			m.closeRoom(room)
			continue
		}
		forwarded.Events = append(forwarded.Events, event)
	}
	// Apply clears Registry membership before cancellation completion can release
	// the next request. Admissions denied above become scoped cleanup operations.
	for _, cmd := range m.registry.Apply(forwarded, approved) {
		m.remove(room, cmd)
	}
	// The Room already removed a kicked member in this batch, after PLAYER_KICKED
	// was queued above. Treat the matched request like a cancellation: a scoped
	// Remove confirms absence and blocks reassignment until that proof arrives,
	// while a new requestID from the same connection is accepted (MATCH_WAITING).
	for _, event := range batch.Events {
		if w, ok := event.(sc.WireEvent); ok && w.Message.Type == message.TypePlayerKicked {
			for _, target := range w.Recipients {
				r := m.memberRequest(sc.RemoveCommand{RoomID: batch.RoomID, PlayerID: target.PlayerID, RequestID: target.RequestID})
				if r != nil && r.state == sc.RequestMatched {
					m.cancel(r, false)
				}
			}
		}
	}
	for _, r := range removed {
		m.registry.ClearMembership(batch.RoomID, r.member)
		r.roomID, r.cleanupPending = "", false
		r.state = sc.RequestCancelled
		m.finishCancelled(r)
	}
	m.publish(room)
}

func (m *Matchmaker) admit(r *request, room *trackedRoom, admission sc.Admission) bool {
	r.token = 0
	select {
	case <-room.handle.Done:
		m.closeRoom(room)
	default:
	}
	if room.closing {
		r.cleanupPending = true
		return false
	}
	if r.disconnected || r.state == sc.RequestCancelled {
		r.state, r.cleanupPending = sc.RequestCancelled, true
		return false
	}
	r.state = sc.RequestMatched
	s := room.snapshot
	r.welcome = &message.WelcomePayload{RequestID: r.payload.RequestID, RoomID: s.RoomID, PlayerID: r.member.PlayerID, HostID: admission.HostID, Players: append([]string(nil), admission.Players...), RoomState: string(admission.State), MaxPlayers: s.MaxPlayers, Category: s.Category, CatalogVersion: s.CatalogVersion}
	return true
}

func (m *Matchmaker) retry(r *request, batch sc.EventBatch) {
	r.roomID, r.token, r.cleanupPending = "", 0, false
	if r.state == sc.RequestCancelled || r.disconnected {
		r.state = sc.RequestCancelled
		m.finishCancelled(r)
		return
	}
	r.rejected[batch.RoomID] = batch.Revision
	r.state = sc.RequestWaiting
}
