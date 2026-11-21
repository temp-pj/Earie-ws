// Package matchmaking owns request state and room snapshots in one event loop.
// T3 implements admission. Cancellation tombstones, cleanup timeouts and fault
// quarantine are deliberately left to T4; game commands remain T7's boundary.
package matchmaking

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	message "temp-ws/internal/message/v1"
	"temp-ws/internal/music"
	"temp-ws/internal/session"
	sc "temp-ws/internal/sessioncontract"
)

var arrivalSequence atomic.Uint64

type request struct {
	connection   session.Connection
	payload      message.MatchRequestPayload
	member       sc.Member
	state        sc.RequestState
	token        sc.OperationToken
	roomID       string
	disconnected bool
	rejected     map[string]sc.Revision
	welcome      *message.WelcomePayload
}

type trackedRoom struct {
	handle          sc.RoomHandle
	snapshot        sc.RoomSnapshot
	public          bool
	initialComplete bool
	publishToken    sc.OperationToken
	cleanup         map[sc.OperationToken]sc.RemoveCommand
	closing         bool
}

type input struct {
	connection   session.Connection
	payload      message.MatchRequestPayload
	disconnected bool
}

// Matchmaker methods enqueue only. Run is the sole owner of requests and rooms;
// the Registry separately owns connection references and each Room its members.
type Matchmaker struct {
	ctx         context.Context
	gateway     sc.RoomGateway
	catalog     *music.CatalogStore
	registry    *session.Registry
	events      <-chan sc.EventBatch
	wake        chan struct{}
	mu          sync.Mutex
	inputs      []input
	done        chan struct{}
	roomDone    chan string
	requests    map[sc.ConnectionID]*request
	rooms       map[string]*trackedRoom
	failedPairs map[[2]sc.Sequence]bool
}

func New(ctx context.Context, gateway sc.RoomGateway, catalog *music.CatalogStore, events <-chan sc.EventBatch) *Matchmaker {
	m := &Matchmaker{ctx: ctx, gateway: gateway, catalog: catalog, events: events,
		wake: make(chan struct{}, 1), done: make(chan struct{}), roomDone: make(chan string),
		requests: make(map[sc.ConnectionID]*request), rooms: make(map[string]*trackedRoom), failedPairs: make(map[[2]sc.Sequence]bool)}
	m.registry = session.NewRegistry(ctx, gateway, m, nil)
	return m
}

func (m *Matchmaker) Registry() *session.Registry { return m.registry }
func (m *Matchmaker) Done() <-chan struct{}       { return m.done }

func (m *Matchmaker) Match(ctx context.Context, c session.Connection, msg message.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, ok := msg.Payload.(*message.MatchRequestPayload)
	if msg.Type != message.TypeMatchRequest || !ok || payload == nil {
		// MATCH_CANCEL must not falsely acknowledge cleanup before T4 exists.
		return sc.ErrClosed
	}
	return m.enqueue(input{connection: c, payload: *payload})
}

func (m *Matchmaker) Disconnected(c session.Connection) {
	_ = m.enqueue(input{connection: c, disconnected: true})
}

func (m *Matchmaker) enqueue(in input) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ctx.Err() != nil {
		return sc.ErrClosed
	}
	// Disconnect notifications cannot be dropped when the request queue is full.
	if !in.disconnected && len(m.inputs) >= 256 {
		return sc.ErrBackpressure
	}
	m.inputs = append(m.inputs, in)
	select {
	case m.wake <- struct{}{}:
	default:
	}
	return nil
}

// Run must be called once. Room completions never block the central loop: each
// gateway call only enqueues, and ordered batches are consumed here in full.
func (m *Matchmaker) Run() {
	defer close(m.done)
	defer m.shutdown()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.wake:
			m.mu.Lock()
			inputs := m.inputs
			m.inputs = nil
			m.mu.Unlock()
			for _, in := range inputs {
				m.accept(in)
				m.schedule()
			}
		case batch := <-m.events:
			m.apply(batch)
			m.schedule()
		case id := <-m.roomDone:
			// Done guarantees no more publications. Drain the shared FIFO before
			// releasing this room, including other rooms' interleaved batches.
			m.drain()
			m.retire(id)
			m.schedule()
		}
	}
}

func (m *Matchmaker) drain() {
	for {
		select {
		case batch := <-m.events:
			m.apply(batch)
		default:
			return
		}
	}
}

func (m *Matchmaker) accept(in input) {
	r := m.requests[in.connection.ID]
	if in.disconnected {
		if r != nil {
			r.disconnected = true
			if r.state != sc.RequestAssigning {
				delete(m.requests, in.connection.ID)
			}
		}
		return
	}
	if r != nil {
		if r.payload.RequestID == in.payload.RequestID {
			// T4 adds terminal replay/history and changed-payload policy closure.
			if r.welcome != nil {
				m.send(r, message.TypeWelcome, *r.welcome)
			} else {
				m.waiting(r)
			}
			return
		}
		code := message.AlreadyMatching
		if r.state == sc.RequestMatched {
			code = message.AlreadyInRoom
		}
		m.sendFailure(in.connection.ID, in.payload.RequestID, code)
		return
	}
	if m.catalog == nil || m.catalog.ValidateRequest(in.payload.CatalogVersion, in.payload.Category) != nil {
		m.sendFailure(in.connection.ID, in.payload.RequestID, message.InvalidCriteria)
		return
	}
	r = &request{connection: in.connection, payload: in.payload, state: sc.RequestWaiting,
		member: sc.Member{PlayerID: in.connection.PlayerID, RequestID: in.payload.RequestID, ArrivalSequence: sc.Sequence(arrivalSequence.Add(1))}, rejected: make(map[string]sc.Revision)}
	m.requests[in.connection.ID] = r
	// An initial-pair rejection keeps arrival order, but is not retried in a
	// self-sustaining failure loop. New input or public room changes enable it.
	clear(m.failedPairs)
	m.waiting(r)
}

func (m *Matchmaker) send(r *request, kind string, payload any) {
	if err := m.sendTo(r.connection.ID, kind, payload); err != nil {
		r.disconnected = true
	}
}
func (m *Matchmaker) sendTo(id sc.ConnectionID, kind string, payload any) error {
	now := time.Now().UnixMilli()
	return m.registry.Send(id, message.Message{Type: kind, Payload: payload, Timestamp: &now})
}
func (m *Matchmaker) waiting(r *request) {
	m.send(r, message.TypeMatchWaiting, message.MatchWaitingPayload{RequestID: r.payload.RequestID})
}
func (m *Matchmaker) sendFailure(id sc.ConnectionID, requestID string, code message.MatchFailureCode) {
	_ = m.sendTo(id, message.TypeMatchFailed, message.MatchFailedPayload{RequestID: requestID, Code: code})
}

func (m *Matchmaker) waitingRequests() []*request {
	var waiting []*request
	for _, r := range m.requests {
		if r.state == sc.RequestWaiting && !r.disconnected {
			waiting = append(waiting, r)
		}
	}
	sort.Slice(waiting, func(i, j int) bool { return waiting[i].member.ArrivalSequence < waiting[j].member.ArrivalSequence })
	return waiting
}

func (m *Matchmaker) watch(handle sc.RoomHandle) {
	go func() {
		select {
		case <-handle.Done:
			select {
			case m.roomDone <- handle.RoomID:
			case <-m.ctx.Done():
			}
		case <-m.ctx.Done():
		}
	}()
}

func (m *Matchmaker) shutdown() {
	// Only shutdown waits for Done, after the matching loop has stopped. The
	// event queue never blocks producers, even when its context is cancelled.
	for _, r := range m.rooms {
		_, _ = m.gateway.Close(context.Background(), sc.CloseCommand{RoomID: r.handle.RoomID})
	}
	for _, r := range m.rooms {
		<-r.handle.Done
	}
	m.drain()
	for id := range m.rooms {
		m.retire(id)
	}
}

func (m *Matchmaker) retire(id string) {
	m.registry.Terminated(id)
	for cid, r := range m.requests {
		delete(r.rejected, id)
		if r.roomID == id {
			if r.state == sc.RequestAssigning && !r.disconnected {
				m.sendFailure(cid, r.payload.RequestID, message.MatchInternalError)
			}
			delete(m.requests, cid)
		}
	}
	delete(m.rooms, id)
	m.registry.ForgetRoom(id)
	if g, ok := m.gateway.(interface{ Forget(string) error }); ok {
		_ = g.Forget(id)
	}
}
