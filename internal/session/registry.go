package session

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	message "temp-ws/internal/message/v1"
	sc "temp-ws/internal/sessioncontract"
)

var connectionSequence atomic.Uint64
var operationSequence atomic.Uint64

// NextOperationToken is shared by session cleanup and the future matchmaker.
func NextOperationToken() sc.OperationToken { return sc.OperationToken(operationSequence.Add(1)) }

// Transport sends without waiting for queue space or network I/O.
type Transport interface {
	Send(message.Message) error
	Stop()
}
type Connection struct {
	ID       sc.ConnectionID
	PlayerID string
}

// Handler methods must enqueue without blocking. Implementations own request
// tombstones and cleanup barriers, including disconnect during pending admission.
type Handler interface {
	Match(context.Context, Connection, message.Message) error
	Disconnected(Connection)
}
type RoomCommander interface {
	Command(context.Context, string, sc.Member, message.Message) error
}
type membership struct {
	roomID string
	member sc.Member
}
type pendingLeave struct {
	connectionID sc.ConnectionID
	membership
	responses int
}
type entry struct {
	connection Connection
	transport  Transport
	membership
}
type Registry struct {
	ctx      context.Context
	gateway  sc.RoomGateway
	handler  Handler
	commands RoomCommander
	// mu serializes connection commands and whole event batches, including WELCOME
	// enqueue before subsequent room wire events. No method waits for another actor.
	mu          sync.Mutex
	connections map[sc.ConnectionID]*entry
	players     map[string]*entry
	leaves      map[sc.OperationToken]pendingLeave
	revisions   map[string]sc.Revision
	terminal    map[string]bool
}

func NewRegistry(ctx context.Context, gateway sc.RoomGateway, handler Handler, commands RoomCommander) *Registry {
	return &Registry{ctx: ctx, gateway: gateway, handler: handler, commands: commands,
		connections: make(map[sc.ConnectionID]*entry), players: make(map[string]*entry), leaves: make(map[sc.OperationToken]pendingLeave), revisions: make(map[string]sc.Revision), terminal: make(map[string]bool)}
}
func (r *Registry) Connect(playerID string, transport Transport) Connection {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := Connection{ID: sc.ConnectionID(connectionSequence.Add(1)), PlayerID: playerID}
	e := &entry{connection: c, transport: transport}
	r.connections[c.ID], r.players[playerID] = e, e
	return c
}
func (r *Registry) Disconnect(id sc.ConnectionID) {
	r.mu.Lock()
	e := r.connections[id]
	if e == nil {
		r.mu.Unlock()
		return
	}
	delete(r.connections, id)
	delete(r.players, e.connection.PlayerID)
	for token, leave := range r.leaves {
		if leave.connectionID == id {
			delete(r.leaves, token)
		}
	}
	if e.roomID != "" {
		_ = r.remove(e.membership, NextOperationToken())
	}
	r.mu.Unlock()
	if r.handler != nil {
		r.handler.Disconnected(e.connection)
	}
}
func (r *Registry) remove(m membership, token sc.OperationToken) error {
	err := r.gateway.Remove(r.ctx, sc.RemoveCommand{Token: token, RoomID: m.roomID, PlayerID: m.member.PlayerID, RequestID: m.member.RequestID})
	if err != nil {
		// Enqueue failure is not cleanup success. Cancel the room; the owner must
		// still observe Done/drain before allowing reassignment (T4).
		_, _ = r.gateway.Close(r.ctx, sc.CloseCommand{Token: NextOperationToken(), RoomID: m.roomID})
	}
	return err
}
func serverMessage(kind string, payload any) message.Message {
	now := time.Now().UnixMilli()
	return message.Message{Type: kind, Payload: payload, Timestamp: &now}
}
func (r *Registry) Route(id sc.ConnectionID, msg message.Message) error {
	r.mu.Lock()
	e := r.connections[id]
	if e == nil {
		r.mu.Unlock()
		return sc.ErrClosed
	}
	if msg.Type == message.TypeMatchRequest || msg.Type == message.TypeMatchCancel {
		connection := e.connection
		r.mu.Unlock()
		if r.handler == nil {
			return sc.ErrClosed
		} // wiring is intentionally deferred to T3
		return r.handler.Match(r.ctx, connection, msg)
	}
	defer r.mu.Unlock()
	if payload, ok := msg.Payload.(*message.LeaveRoomPayload); ok && msg.Type == message.TypeLeaveRoom {
		if payload.RoomID != e.roomID {
			return e.transport.Send(serverMessage(message.TypeRoomLeft, message.RoomLeftPayload{RoomID: payload.RoomID}))
		}
		for token, pending := range r.leaves {
			if pending.connectionID == id && pending.membership == e.membership {
				pending.responses++
				r.leaves[token] = pending
				return nil
			}
		}
		token := NextOperationToken()
		if err := r.remove(e.membership, token); err != nil {
			return err
		}
		r.leaves[token] = pendingLeave{connectionID: id, membership: e.membership, responses: 1}
		return nil
	}
	roomID := commandRoomID(msg)
	if roomID == "" || roomID != e.roomID {
		return nil
	}
	if r.commands == nil {
		return sc.ErrClosed
	} // RoomCommander is the room Gateway in cmd/server
	return r.commands.Command(r.ctx, roomID, e.member, msg)
}
func commandRoomID(msg message.Message) string {
	switch p := msg.Payload.(type) {
	case *message.RoomPayload:
		return p.RoomID
	case *message.ReadyToPlayPayload:
		return p.RoomID
	case *message.SubmitAnswerPayload:
		return p.RoomID
	case *message.KickPlayerPayload:
		return p.RoomID
	}
	return ""
}

// Send provides the match handler with the connection's FIFO (waiting/failure/
// cancellation replay). No transport or membership is exposed to the handler.
func (r *Registry) Send(id sc.ConnectionID, msg message.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.connections[id]
	if e == nil {
		return sc.ErrClosed
	}
	return e.transport.Send(msg)
}

// Apply consumes one indivisible batch after the request owner adjudicates its
// tokens/cancellations. approved lists ONLY successful, still-valid admissions.
// Rejected/stale admissions are returned as scoped cleanup commands, never
// welcomed. The consumer must enqueue these and await cleanup before Publish.
// The single consumer must call Apply in each room's FIFO order.
func (r *Registry) Apply(batch sc.EventBatch, approved []sc.Member) []sc.RemoveCommand {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminal[batch.RoomID] || batch.Revision <= r.revisions[batch.RoomID] {
		return nil
	}
	r.revisions[batch.RoomID] = batch.Revision
	var cleanup []sc.RemoveCommand
	admit := func(member sc.Member, admission sc.Admission) {
		allowed := false
		for _, m := range approved {
			if m == member {
				allowed = true
				break
			}
		}
		e := r.players[member.PlayerID]
		if allowed && e != nil && e.roomID == "" {
			welcome := message.WelcomePayload{RequestID: member.RequestID, RoomID: batch.RoomID, PlayerID: member.PlayerID, HostID: admission.HostID, Players: admission.Players, RoomState: string(admission.State), MaxPlayers: batch.Snapshot.MaxPlayers, Category: batch.Snapshot.Category, CatalogVersion: batch.Snapshot.CatalogVersion}
			if err := e.transport.Send(serverMessage(message.TypeWelcome, welcome)); err == nil {
				e.membership = membership{roomID: batch.RoomID, member: member}
				return
			}
			e.transport.Stop()
		}
		cleanup = append(cleanup, sc.RemoveCommand{Token: NextOperationToken(), RoomID: batch.RoomID, PlayerID: member.PlayerID, RequestID: member.RequestID})
	}
	for _, event := range batch.Events {
		switch event := event.(type) {
		case sc.InitialPairResult:
			if event.Rejection == sc.AdmissionAccepted {
				for _, m := range event.Members {
					admit(m, event.Admission)
				}
			}
		case sc.RegistrationResult:
			if event.Rejection == sc.AdmissionAccepted {
				admit(event.Member, event.Admission)
			}
		case sc.RemovalResult:
			r.removed(batch.RoomID, event)
		case sc.WireEvent:
			for _, member := range event.Recipients {
				if e := r.players[member.PlayerID]; e != nil && e.roomID == batch.RoomID && e.member == member {
					if err := e.transport.Send(event.Message); err != nil {
						e.transport.Stop()
					}
				}
			}
		case sc.Closed:
			r.terminated(batch.RoomID)
		}
	}
	return cleanup
}

// ForgetRoom is called only after Done and stream drain, alongside Gateway.Forget.
func (r *Registry) ForgetRoom(roomID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.revisions, roomID)
	delete(r.terminal, roomID)
}

// Terminated handles a failed event sink after the room owner has observed
// Done and drained any batches. Closed cannot be relied on when the sink fails.
func (r *Registry) Terminated(roomID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.terminated(roomID)
}
func (r *Registry) terminated(roomID string) {
	r.terminal[roomID] = true
	for _, e := range r.connections {
		if e.roomID == roomID {
			e.transport.Stop()
			e.membership = membership{}
		}
	}
	for token, leave := range r.leaves {
		if leave.roomID == roomID {
			if e := r.connections[leave.connectionID]; e != nil {
				e.transport.Stop()
			}
			delete(r.leaves, token)
		}
	}
}

// Connected prevents queued matcher inputs from recreating a closed session.
func (r *Registry) Connected(id sc.ConnectionID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.connections[id] != nil
}

// CloseConnection initiates transport shutdown without waiting on network I/O.
// Policy-aware transports send 1008 from their own writer worker.
func (r *Registry) CloseConnection(id sc.ConnectionID, policy bool) {
	r.mu.Lock()
	if e := r.connections[id]; e != nil {
		if p, ok := e.transport.(interface{ RejectPolicy() }); policy && ok {
			p.RejectPolicy()
		} else {
			e.transport.Stop()
		}
	}
	r.mu.Unlock()
	r.Disconnect(id)
}

// ConfirmRemoval completes an already-proven scoped cleanup, e.g. a queued
// leave overtaken by a cancellation's Remove result. It does not invent a room
// revision or issue another Remove that could later time out a replacement.
func (r *Registry) ConfirmRemoval(cmd sc.RemoveCommand) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removed(cmd.RoomID, sc.RemovalResult{Token: cmd.Token, PlayerID: cmd.PlayerID, RequestID: cmd.RequestID})
}

// ClearMembership requires a matching Remove result or Room Done plus drain.
// Complete every pending leave of this exact membership, including a leave
// overtaken by cancellation cleanup, before a replacement may be dispatched.
func (r *Registry) ClearMembership(roomID string, member sc.Member) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removed(roomID, sc.RemovalResult{PlayerID: member.PlayerID, RequestID: member.RequestID})
	for token, leave := range r.leaves {
		if leave.roomID == roomID && leave.member == member {
			r.removed(roomID, sc.RemovalResult{Token: token, PlayerID: member.PlayerID, RequestID: member.RequestID})
		}
	}
}

// Caller holds mu; neither membership nor leave records are keyed by player alone.
func (r *Registry) removed(roomID string, result sc.RemovalResult) {
	if e := r.players[result.PlayerID]; e != nil && e.roomID == roomID && e.member.RequestID == result.RequestID {
		e.membership = membership{}
	}
	leave, ok := r.leaves[result.Token]
	if !ok || leave.roomID != roomID || leave.member.PlayerID != result.PlayerID || leave.member.RequestID != result.RequestID {
		return
	}
	delete(r.leaves, result.Token)
	if current := r.connections[leave.connectionID]; current != nil {
		for i := 0; i < leave.responses; i++ {
			if err := current.transport.Send(serverMessage(message.TypeRoomLeft, message.RoomLeftPayload{RoomID: roomID})); err != nil {
				current.transport.Stop()
				break
			}
		}
	}
}
