package matchmaking

import (
	message "temp-ws/internal/message/v1"
	sc "temp-ws/internal/sessioncontract"
)

func (m *Matchmaker) apply(batch sc.EventBatch) {
	room := m.rooms[batch.RoomID]
	if room == nil || batch.Revision <= room.snapshot.Revision {
		return
	}
	// A closing room still needs removal/Closed results consumed, but cannot
	// regain eligibility from a late snapshot or approve another admission.
	if room.public && !room.closing {
		clear(m.failedPairs)
	}
	room.snapshot = batch.Snapshot
	var approved []sc.Member
	forwarded := batch
	forwarded.Events = make([]sc.Event, 0, len(batch.Events))
	for _, event := range batch.Events {
		switch e := event.(type) {
		case sc.InitialPairResult:
			a, b := m.pending(batch.RoomID, e.Token, e.Members[0]), m.pending(batch.RoomID, e.Token, e.Members[1])
			if a == nil || b == nil {
				continue
			}
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
			if e.Rejection != sc.AdmissionAccepted {
				m.retry(r, batch)
				continue
			}
			if m.admit(r, room, e.Admission) {
				approved = append(approved, r.member)
			}
		case sc.RemovalResult:
			if cmd, ok := room.cleanup[e.Token]; ok && cmd.PlayerID == e.PlayerID && cmd.RequestID == e.RequestID {
				delete(room.cleanup, e.Token)
			}
			for cid, r := range m.requests {
				if r.roomID == batch.RoomID && r.member.PlayerID == e.PlayerID && r.member.RequestID == e.RequestID && r.state == sc.RequestMatched {
					delete(m.requests, cid)
				}
			}
		case sc.Published:
			if e.Token == room.publishToken && e.Token != 0 && !room.closing {
				room.public = true
				clear(m.failedPairs)
			}
		case sc.Closed:
			room.closing = true
		}
		forwarded.Events = append(forwarded.Events, event)
	}
	// Registry applies all WELCOME and wire messages together under its FIFO
	// lock. Failed sends/disconnected connections become scoped cleanup, and a
	// private room cannot publish until every such removal has completed.
	for _, cmd := range m.registry.Apply(forwarded, approved) {
		room.cleanup[cmd.Token] = cmd
		if err := m.gateway.Remove(m.ctx, cmd); err != nil {
			m.closeRoom(room)
		}
	}
	m.publish(room)
}

func (m *Matchmaker) admit(r *request, room *trackedRoom, admission sc.Admission) bool {
	if r.disconnected || room.closing {
		delete(m.requests, r.connection.ID)
		return false
	}
	r.state = sc.RequestMatched
	s := room.snapshot
	r.welcome = &message.WelcomePayload{RequestID: r.payload.RequestID, RoomID: s.RoomID, PlayerID: r.member.PlayerID, HostID: admission.HostID, Players: append([]string(nil), admission.Players...), RoomState: string(admission.State), MaxPlayers: s.MaxPlayers, Category: s.Category, CatalogVersion: s.CatalogVersion}
	return true
}

func (m *Matchmaker) retry(r *request, batch sc.EventBatch) {
	r.rejected[batch.RoomID] = batch.Revision
	r.state, r.roomID, r.token = sc.RequestWaiting, "", 0
	if r.disconnected {
		delete(m.requests, r.connection.ID)
	}
}
