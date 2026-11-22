package matchmaking

import (
	"context"

	message "temp-ws/internal/message/v1"
	"temp-ws/internal/session"
	sc "temp-ws/internal/sessioncontract"
)

const maxRequestIDs = 4096

// Registry-originated leave/disconnect removals enter the same owner loop as
// cancellation. No accepted Remove can escape deadline and quarantine tracking.
type registryGateway struct {
	sc.RoomGateway
	owner *Matchmaker
}

func (g registryGateway) Remove(ctx context.Context, cmd sc.RemoveCommand) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return g.owner.enqueue(input{remove: &cmd})
}

func (m *Matchmaker) accept(in input) {
	if in.remove != nil {
		cmd := *in.remove
		// Registry only emits cleanup for a previously admitted membership.
		// Missing request ownership here means another completion already
		// proved it absent, even if this input was queued before that proof.
		if m.memberRequest(cmd) == nil {
			m.registry.ConfirmRemoval(cmd)
		} else if room := m.rooms[cmd.RoomID]; room != nil && !room.closing {
			m.remove(room, cmd)
		}
		return
	}
	if in.disconnected {
		for _, r := range m.history[in.connection.ID] {
			r.disconnected = true
			m.cancel(r, false)
		}
		return
	}
	// A queued command must not recreate history after connection teardown.
	if !m.registry.Connected(in.connection.ID) {
		return
	}
	id := in.payload.RequestID
	if in.cancelID != "" {
		id = in.cancelID
	}
	records := m.history[in.connection.ID]
	if r := records[id]; r != nil {
		if in.cancelID != "" {
			m.cancel(r, true)
			return
		}
		if r.hasPayload && r.payload != in.payload {
			m.registry.CloseConnection(in.connection.ID, true)
			return
		}
		// An unknown-ID cancel still binds the first subsequent payload, but
		// can never resurrect that request. Later content changes are errors.
		if !r.hasPayload {
			r.payload, r.hasPayload = in.payload, true
		}
		m.replay(r)
		return
	}
	if len(records) >= maxRequestIDs {
		m.registry.CloseConnection(in.connection.ID, true)
		return
	}
	if records == nil {
		records = make(map[string]*request)
		m.history[in.connection.ID] = records
	}
	r := &request{connection: in.connection, payload: in.payload, hasPayload: in.cancelID == ""}
	r.payload.RequestID = id
	records[id] = r
	if in.cancelID != "" {
		m.cancel(r, true)
		return
	}
	if active := m.requests[in.connection.ID]; active != nil {
		code := message.AlreadyMatching
		if active.state == sc.RequestMatched {
			code = message.AlreadyInRoom
		}
		m.fail(r, code)
		return
	}
	if m.catalog == nil || m.catalog.ValidateRequest(in.payload.CatalogVersion, in.payload.Category) != nil {
		m.fail(r, message.InvalidCriteria)
		return
	}
	r.state = sc.RequestWaiting
	r.member = sc.Member{PlayerID: in.connection.PlayerID, RequestID: id, ArrivalSequence: sc.Sequence(arrivalSequence.Add(1))}
	r.rejected = make(map[string]sc.Revision)
	m.requests[in.connection.ID] = r
	clear(m.failedPairs)
	m.waiting(r)
}

func (m *Matchmaker) replay(r *request) {
	switch r.state {
	case sc.RequestWaiting, sc.RequestAssigning:
		m.waiting(r)
	case sc.RequestMatched:
		m.send(r, message.TypeWelcome, *r.welcome)
	case sc.RequestFailed:
		m.send(r, message.TypeMatchFailed, message.MatchFailedPayload{RequestID: r.payload.RequestID, Code: r.failure})
	case sc.RequestCancelled:
		r.cancelReply = true
		m.finishCancelled(r)
	}
}

func (m *Matchmaker) fail(r *request, code message.MatchFailureCode) {
	r.state, r.failure = sc.RequestFailed, code
	m.release(r)
	if !r.disconnected {
		m.replay(r)
	}
}

func (m *Matchmaker) cancel(r *request, reply bool) {
	r.cancelReply = r.cancelReply || reply
	previous := r.state
	r.state = sc.RequestCancelled
	if m.requests[r.connection.ID] == r {
		delete(m.requests, r.connection.ID)
	}
	switch previous {
	case sc.RequestAssigning:
		// Keep the original immutable registration token until its result.
		r.cleanupPending = true
	case sc.RequestMatched:
		r.cleanupPending = true
		if room := m.rooms[r.roomID]; room != nil && !room.closing {
			m.remove(room, sc.RemoveCommand{Token: session.NextOperationToken(), RoomID: r.roomID, PlayerID: r.member.PlayerID, RequestID: r.member.RequestID})
		}
	}
	m.finishCancelled(r)
}

func (m *Matchmaker) finishCancelled(r *request) {
	if r.cleanupPending || r.token != 0 || r.roomID != "" {
		return
	}
	m.release(r)
	if r.cancelReply && !r.disconnected {
		r.cancelReply = false
		m.send(r, message.TypeMatchCancelled, message.MatchCancelledPayload{RequestID: r.payload.RequestID})
	}
}

// Compact terminal records retain only identity, original payload and replay
// outcome. Pointer comparison prevents an old completion clearing a new active ID.
func (m *Matchmaker) release(r *request) {
	if m.requests[r.connection.ID] == r {
		delete(m.requests, r.connection.ID)
	}
	r.rejected, r.welcome = nil, nil
	if r.disconnected && !r.cleanupPending && r.token == 0 && r.roomID == "" {
		delete(m.history[r.connection.ID], r.payload.RequestID)
		if len(m.history[r.connection.ID]) == 0 {
			delete(m.history, r.connection.ID)
		}
	}
}

func (m *Matchmaker) blocked(id sc.ConnectionID) bool {
	for _, r := range m.history[id] {
		if r.cleanupPending {
			return true
		}
	}
	return false
}

func (m *Matchmaker) memberRequest(cmd sc.RemoveCommand) *request {
	for _, records := range m.history {
		for _, r := range records {
			if r.roomID == cmd.RoomID && r.member.PlayerID == cmd.PlayerID && r.member.RequestID == cmd.RequestID {
				return r
			}
		}
	}
	return nil
}
