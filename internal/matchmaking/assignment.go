package matchmaking

import (
	"sort"

	"temp-ws/internal/session"
	sc "temp-ws/internal/sessioncontract"
)

func pairKey(a, b *request) [2]sc.Sequence {
	x, y := a.member.ArrivalSequence, b.member.ArrivalSequence
	if x > y {
		x, y = y, x
	}
	return [2]sc.Sequence{x, y}
}

func (m *Matchmaker) schedule() {
	if m.ctx.Err() != nil {
		return
	}
	waiting := m.waitingRequests()
	var rooms []*trackedRoom
	for _, r := range m.rooms {
		if r.public && !r.closing && r.snapshot.Accepting && r.snapshot.PlayerCount < r.snapshot.MaxPlayers {
			rooms = append(rooms, r)
		}
	}
	sort.Slice(rooms, func(i, j int) bool { return rooms[i].snapshot.CreationSequence < rooms[j].snapshot.CreationSequence })
	for _, r := range waiting {
		if r.state != sc.RequestWaiting {
			continue
		}
		for _, room := range rooms {
			s := room.snapshot
			if r.payload.CatalogVersion != s.CatalogVersion || r.rejected[s.RoomID] >= s.Revision || !m.catalog.CanJoin(s.CatalogVersion, r.payload.Category, s.Category) {
				continue
			}
			token := session.NextOperationToken()
			if err := m.gateway.Register(m.ctx, sc.RegisterCommand{Token: token, RoomID: s.RoomID, Member: r.member}); err != nil {
				// No command was accepted: try the next candidate without losing
				// arrival order or repeatedly enqueueing at the same revision.
				r.rejected[s.RoomID] = s.Revision
				continue
			}
			r.state, r.token, r.roomID = sc.RequestAssigning, token, s.RoomID
			m.track(room, token)
			break
		}
		if r.state != sc.RequestWaiting {
			continue
		}
		for _, other := range waiting {
			if other == r || other.state != sc.RequestWaiting || other.payload.CatalogVersion != r.payload.CatalogVersion || m.failedPairs[pairKey(r, other)] {
				continue
			}
			category, compatible, err := m.catalog.ResolvePair(r.payload.CatalogVersion, r.payload.Category, other.payload.Category)
			if err != nil || !compatible {
				continue
			}
			token := session.NextOperationToken()
			members := [2]sc.Member{r.member, other.member}
			handle, err := m.gateway.CreateInitialPair(m.ctx, sc.CreateInitialPairCommand{Token: token, Category: category, CatalogVersion: r.payload.CatalogVersion, Members: members})
			if err != nil {
				m.failedPairs[pairKey(r, other)] = true
				continue
			}
			m.rooms[handle.RoomID] = &trackedRoom{handle: handle, cleanup: make(map[sc.OperationToken]sc.RemoveCommand)}
			for _, req := range []*request{r, other} {
				req.state, req.token, req.roomID = sc.RequestAssigning, token, handle.RoomID
			}
			m.track(m.rooms[handle.RoomID], token)
			m.watch(handle)
			break
		}
	}
}

func (m *Matchmaker) pending(roomID string, token sc.OperationToken, member sc.Member) *request {
	if token == 0 {
		return nil
	}
	for _, records := range m.history {
		for _, r := range records {
			if (r.state == sc.RequestAssigning || r.state == sc.RequestCancelled) && r.roomID == roomID && r.token == token && r.member == member {
				return r
			}
		}
	}
	return nil
}

func (m *Matchmaker) closeRoom(room *trackedRoom) {
	if room.closing {
		return
	}
	room.closing = true
	room.snapshot.Accepting = false
	room.closeDeadline = m.now().Add(sc.OperationTimeout)
	_, _ = m.gateway.Close(m.ctx, sc.CloseCommand{Token: session.NextOperationToken(), RoomID: room.handle.RoomID})
}

func (m *Matchmaker) publish(room *trackedRoom) {
	if !room.initialComplete || room.public || room.closing || room.publishToken != 0 || len(room.cleanup) != 0 {
		return
	}
	if room.snapshot.PlayerCount == 0 {
		m.closeRoom(room)
		return
	}
	room.publishToken = session.NextOperationToken()
	m.track(room, room.publishToken)
	if err := m.gateway.Publish(m.ctx, sc.PublishCommand{Token: room.publishToken, RoomID: room.handle.RoomID}); err != nil {
		m.closeRoom(room)
	}
}
