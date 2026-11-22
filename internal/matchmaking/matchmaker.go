// Package matchmaking owns request state and room snapshots in one event loop.
// Cancellation and fault cleanup share the admission loop; game commands remain
// T7's boundary.
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
	connection     session.Connection
	payload        message.MatchRequestPayload
	member         sc.Member
	state          sc.RequestState
	token          sc.OperationToken
	roomID         string
	disconnected   bool
	rejected       map[string]sc.Revision
	welcome        *message.WelcomePayload
	hasPayload     bool
	failure        message.MatchFailureCode
	cleanupPending bool
	cancelReply    bool
}

type trackedRoom struct {
	handle          sc.RoomHandle
	snapshot        sc.RoomSnapshot
	public          bool
	initialComplete bool
	publishToken    sc.OperationToken
	cleanup         map[sc.OperationToken]sc.RemoveCommand
	closing         bool
	deadlines       map[sc.OperationToken]time.Time
	closeDeadline   time.Time
	closeReported   bool
}

type input struct {
	connection   session.Connection
	payload      message.MatchRequestPayload
	disconnected bool
	cancelID     string
	remove       *sc.RemoveCommand
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
	history     map[sc.ConnectionID]map[string]*request
	now         func() time.Time
}

func New(ctx context.Context, gateway sc.RoomGateway, catalog *music.CatalogStore, events <-chan sc.EventBatch) *Matchmaker {
	m := &Matchmaker{ctx: ctx, gateway: gateway, catalog: catalog, events: events,
		history: make(map[sc.ConnectionID]map[string]*request), now: time.Now,
		wake: make(chan struct{}, 1), done: make(chan struct{}), roomDone: make(chan string),
		requests: make(map[sc.ConnectionID]*request), rooms: make(map[string]*trackedRoom), failedPairs: make(map[[2]sc.Sequence]bool)}
	m.registry = session.NewRegistry(ctx, registryGateway{RoomGateway: gateway, owner: m}, m, nil)
	return m
}

func (m *Matchmaker) Registry() *session.Registry { return m.registry }
func (m *Matchmaker) Done() <-chan struct{}       { return m.done }

func (m *Matchmaker) Match(ctx context.Context, c session.Connection, msg message.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch p := msg.Payload.(type) {
	case *message.MatchRequestPayload:
		if msg.Type == message.TypeMatchRequest && p != nil {
			return m.enqueue(input{connection: c, payload: *p})
		}
	case *message.MatchCancelPayload:
		if msg.Type == message.TypeMatchCancel && p != nil {
			return m.enqueue(input{connection: c, cancelID: p.RequestID})
		}
	}
	return sc.ErrClosed
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
	if !in.disconnected && in.remove == nil && len(m.inputs) >= 256 {
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
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		timer.Reset(m.nextTimeout())
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
		case <-timer.C:
			m.expire(m.now())
			m.schedule()
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

func (m *Matchmaker) send(r *request, kind string, payload any) {
	if err := m.sendTo(r.connection.ID, kind, payload); err != nil {
		r.disconnected = true
		m.registry.CloseConnection(r.connection.ID, false)
	}
}
func (m *Matchmaker) sendTo(id sc.ConnectionID, kind string, payload any) error {
	now := time.Now().UnixMilli()
	return m.registry.Send(id, message.Message{Type: kind, Payload: payload, Timestamp: &now})
}
func (m *Matchmaker) waiting(r *request) {
	m.send(r, message.TypeMatchWaiting, message.MatchWaitingPayload{RequestID: r.payload.RequestID})
}
func (m *Matchmaker) waitingRequests() []*request {
	var waiting []*request
	for _, r := range m.requests {
		if r.state == sc.RequestWaiting && !r.disconnected && !m.blocked(r.connection.ID) {
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
	// Done and drain prove cancelled memberships are absent; preserve their
	// reusable connections while stopping genuinely admitted fault victims.
	for _, records := range m.history {
		for _, r := range records {
			if r.roomID == id && r.state == sc.RequestCancelled {
				m.registry.ClearMembership(id, r.member)
			}
		}
	}
	m.registry.Terminated(id)
	for _, records := range m.history {
		for _, r := range records {
			delete(r.rejected, id)
			if r.roomID != id {
				continue
			}
			r.roomID, r.token, r.cleanupPending = "", 0, false
			switch r.state {
			case sc.RequestCancelled:
				m.finishCancelled(r)
			case sc.RequestAssigning:
				m.fail(r, message.MatchInternalError)
			case sc.RequestMatched:
				r.disconnected = true
				m.release(r)
			}
		}
	}
	delete(m.rooms, id)
	m.registry.ForgetRoom(id)
	if g, ok := m.gateway.(interface{ Forget(string) error }); ok {
		_ = g.Forget(id)
	}
}
