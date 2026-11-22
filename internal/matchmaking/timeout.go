package matchmaking

import (
	"log/slog"
	"time"

	sc "temp-ws/internal/sessioncontract"
)

func (m *Matchmaker) track(room *trackedRoom, token sc.OperationToken) {
	if room.deadlines == nil {
		room.deadlines = make(map[sc.OperationToken]time.Time)
	}
	room.deadlines[token] = m.now().Add(sc.OperationTimeout)
}

func (m *Matchmaker) remove(room *trackedRoom, cmd sc.RemoveCommand) {
	// Cancel, leave and disconnect share one pending membership removal. Its
	// proof completes all scoped leave acknowledgements without extending time.
	for _, pending := range room.cleanup {
		if pending.PlayerID == cmd.PlayerID && pending.RequestID == cmd.RequestID {
			return
		}
	}
	room.cleanup[cmd.Token] = cmd
	m.track(room, cmd.Token)
	if r := m.memberRequest(cmd); r != nil {
		r.cleanupPending = true
	}
	if err := m.gateway.Remove(m.ctx, cmd); err != nil {
		m.closeRoom(room)
	}
}

// One loop-owned timer covers all operations. Tests advance now and expire
// explicitly, so command/result/timeout ordering never depends on sleeps.
func (m *Matchmaker) nextTimeout() time.Duration {
	now := m.now()
	delay := time.Hour
	consider := func(deadline time.Time) {
		if d := deadline.Sub(now); d < delay {
			delay = max(d, 0)
		}
	}
	for _, room := range m.rooms {
		if room.closing {
			if !room.closeReported {
				consider(room.closeDeadline)
			}
			continue
		}
		for _, deadline := range room.deadlines {
			consider(deadline)
		}
	}
	return delay
}

func (m *Matchmaker) expire(now time.Time) {
	for _, room := range m.rooms {
		if !room.closing {
			for _, deadline := range room.deadlines {
				if !now.Before(deadline) {
					m.closeRoom(room)
					break
				}
			}
			continue
		}
		if room.closeReported || now.Before(room.closeDeadline) {
			continue
		}
		select {
		case <-room.handle.Done:
			// A queued Done notification must win over the shutdown watchdog.
			m.drain()
			m.retire(room.handle.RoomID)
			continue
		default:
		}
		room.closeReported = true
		slog.Error("matchmaking room did not stop after cancellation", "roomID", room.handle.RoomID)
		// Disconnect every affected connection, including a replacement waiting
		// behind the old request. Keep quarantine until the actual Done/drain.
		for cid, records := range m.history {
			for _, r := range records {
				if r.roomID == room.handle.RoomID {
					m.registry.CloseConnection(cid, false)
					break
				}
			}
		}
	}
}
