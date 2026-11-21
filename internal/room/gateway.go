package room

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	message "temp-ws/internal/message/v1"
	music "temp-ws/internal/music/v1"
	sc "temp-ws/internal/sessioncontract"
)

const commandQueueSize = 256

var creationSequence atomic.Uint64

// Gateway owns the v1 room registry. The legacy Hub is retained only for the
// old game tests until T7 migrates game handling; the v1 endpoint never uses it.
// Each lifecycleRoom exclusively owns its membership/state in run.
type Gateway struct {
	ctx   context.Context
	sink  sc.EventSink
	mu    sync.Mutex
	rooms map[string]*lifecycleRoom
}

var _ sc.RoomGateway = (*Gateway)(nil)

func NewGateway(ctx context.Context, sink sc.EventSink) *Gateway {
	return &Gateway{ctx: ctx, sink: sink, rooms: make(map[string]*lifecycleRoom)}
}

func (g *Gateway) CreateInitialPair(ctx context.Context, cmd sc.CreateInitialPairCommand) (sc.RoomHandle, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return sc.RoomHandle{}, err
	}
	if g.ctx.Err() != nil {
		return sc.RoomHandle{}, sc.ErrClosed
	}
	roomCtx, cancel := context.WithCancel(g.ctx)
	r := &lifecycleRoom{ctx: roomCtx, cancel: cancel, sink: g.sink,
		handle: sc.RoomHandle{RoomID: uuid.NewString(), CreationSequence: sc.Sequence(creationSequence.Add(1))},
		done:   make(chan struct{}), commands: make(chan any, commandQueueSize),
		members: make(map[string]sc.Member), state: sc.RoomInitializing,
		snapshot: sc.RoomSnapshot{Category: cmd.Category, CatalogVersion: cmd.CatalogVersion, MaxPlayers: sc.MaxPlayers}}
	r.handle.Done = r.done
	r.snapshot.RoomID, r.snapshot.CreationSequence = r.handle.RoomID, r.handle.CreationSequence
	r.commands <- cmd
	g.rooms[r.handle.RoomID] = r
	go r.run()
	return r.handle, nil
}

func (g *Gateway) lookup(id string) (*lifecycleRoom, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	r := g.rooms[id]
	if r == nil {
		return nil, sc.ErrRoomNotFound
	}
	return r, nil
}
func (g *Gateway) enqueue(ctx context.Context, id string, cmd any) error {
	r, err := g.lookup(id)
	if err != nil {
		return err
	}
	return r.enqueue(ctx, cmd)
}
func (g *Gateway) Register(ctx context.Context, cmd sc.RegisterCommand) error {
	return g.enqueue(ctx, cmd.RoomID, cmd)
}
func (g *Gateway) Remove(ctx context.Context, cmd sc.RemoveCommand) error {
	return g.enqueue(ctx, cmd.RoomID, cmd)
}
func (g *Gateway) Publish(ctx context.Context, cmd sc.PublishCommand) error {
	return g.enqueue(ctx, cmd.RoomID, cmd)
}
func (g *Gateway) Close(ctx context.Context, cmd sc.CloseCommand) (<-chan struct{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r, err := g.lookup(cmd.RoomID)
	if err != nil {
		return nil, err
	}
	r.stop(cmd.Token)
	return r.done, nil
}

// Forget releases a closed handle only AFTER the consumer has drained all of
// this room's batches. Until then repeated Close returns the same Done barrier.
func (g *Gateway) Forget(roomID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	r := g.rooms[roomID]
	if r == nil {
		return nil
	}
	select {
	case <-r.done:
		delete(g.rooms, roomID)
		return nil
	default:
		return sc.ErrBackpressure
	}
}

// BeginStart is the admission barrier for T7 game initialization. It is queued
// in the SAME stream as Register. T7 must supply song loading/completion before
// exposing game commands; this method alone does not start or announce a game.
func (g *Gateway) BeginStart(ctx context.Context, roomID string, member sc.Member) error {
	return g.enqueue(ctx, roomID, beginStart{member})
}

type beginStart struct{ member sc.Member }

type lifecycleRoom struct {
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	handle   sc.RoomHandle
	commands chan any
	sink     sc.EventSink
	// gate protects command acceptance and cancellation, never actor state.
	gate       sync.Mutex
	closing    bool
	closeToken sc.OperationToken
	members    map[string]sc.Member
	host       string
	state      sc.RoomState
	public     bool
	snapshot   sc.RoomSnapshot
}

func (r *lifecycleRoom) enqueue(ctx context.Context, cmd any) error {
	r.gate.Lock()
	defer r.gate.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.closing || r.ctx.Err() != nil {
		return sc.ErrClosed
	}
	select {
	case r.commands <- cmd:
		return nil
	default:
		return sc.ErrBackpressure
	}
}
func (r *lifecycleRoom) stop(token sc.OperationToken) {
	r.gate.Lock()
	defer r.gate.Unlock()
	if !r.closing {
		r.closing = true
		r.closeToken = token
		r.cancel()
	}
}
func (r *lifecycleRoom) run() {
	defer close(r.done)
	defer r.cancel()
	defer func() {
		r.stop(0)
		r.state = sc.RoomClosed
		clear(r.members)
		r.gate.Lock()
		token := r.closeToken
		r.gate.Unlock()
		// A failed sink is terminal too. Done remains the reliable fault barrier.
		r.emit(sc.Closed{Token: token})
	}()
	for {
		if r.ctx.Err() != nil {
			return
		}
		select {
		case <-r.ctx.Done():
			return
		case cmd := <-r.commands:
			if r.ctx.Err() != nil {
				return
			}
			if !r.apply(cmd) {
				return
			}
		}
	}
}
func (r *lifecycleRoom) ordered() []sc.Member {
	result := make([]sc.Member, 0, len(r.members))
	for _, m := range r.members {
		result = append(result, m)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ArrivalSequence < result[j].ArrivalSequence })
	return result
}
func (r *lifecycleRoom) admission() sc.Admission {
	players := make([]string, 0, len(r.members))
	for _, m := range r.ordered() {
		players = append(players, m.PlayerID)
	}
	return sc.Admission{HostID: r.host, Players: players, State: r.state.PublicState()}
}
func validMember(m sc.Member) bool {
	return message.ValidID(m.PlayerID) && message.ValidID(m.RequestID) && m.ArrivalSequence > 0
}
func (r *lifecycleRoom) emit(events ...sc.Event) bool {
	r.snapshot.Revision++
	r.snapshot.PlayerCount = len(r.members)
	r.snapshot.Accepting = r.public && r.state == sc.RoomWaiting && len(r.members) < sc.MaxPlayers
	if r.sink == nil {
		return false
	}
	return r.sink.TryPublish(sc.EventBatch{RoomID: r.handle.RoomID, Revision: r.snapshot.Revision, Snapshot: r.snapshot, Events: events}) == nil
}
func (r *lifecycleRoom) wire(kind string, payload any, recipients []sc.Member) sc.WireEvent {
	now := time.Now().UnixMilli()
	return sc.WireEvent{Recipients: recipients, Message: message.Message{Type: kind, Payload: payload, Timestamp: &now}}
}
func (r *lifecycleRoom) apply(command any) bool {
	switch cmd := command.(type) {
	case sc.CreateInitialPairCommand:
		rejection := sc.AdmissionAccepted
		if !validMember(cmd.Members[0]) || !validMember(cmd.Members[1]) || cmd.Members[0].PlayerID == cmd.Members[1].PlayerID || cmd.Members[0].ArrivalSequence == cmd.Members[1].ArrivalSequence || cmd.Category.ValidateResolved() != nil || !music.ValidCatalogVersion(cmd.CatalogVersion) {
			rejection = sc.AdmissionInternalError
		}
		if rejection == sc.AdmissionAccepted {
			for _, m := range cmd.Members {
				r.members[m.PlayerID] = m
			}
			r.host = r.ordered()[0].PlayerID
			r.state = sc.RoomWaiting
		}
		ok := r.emit(sc.InitialPairResult{Token: cmd.Token, Members: cmd.Members, Rejection: rejection, Admission: r.admission()})
		return ok && rejection == sc.AdmissionAccepted
	case sc.RegisterCommand:
		rejection := sc.AdmissionAccepted
		switch {
		case r.state != sc.RoomWaiting:
			rejection = sc.AdmissionStarted
		case !r.public:
			rejection = sc.AdmissionClosed
		case !validMember(cmd.Member):
			rejection = sc.AdmissionInternalError
		case r.members[cmd.Member.PlayerID].PlayerID != "":
			rejection = sc.AdmissionDuplicate
		case len(r.members) >= sc.MaxPlayers:
			rejection = sc.AdmissionFull
		}
		events := []sc.Event{}
		if rejection == sc.AdmissionAccepted {
			r.members[cmd.Member.PlayerID] = cmd.Member
		}
		events = append(events, sc.RegistrationResult{Token: cmd.Token, Member: cmd.Member, Rejection: rejection, Admission: r.admission()})
		if rejection == sc.AdmissionAccepted {
			events = append(events, r.wire(message.TypePlayerJoined, message.PlayerJoinedPayload{RoomID: r.handle.RoomID, PlayerID: cmd.Member.PlayerID}, r.ordered()))
		}
		return r.emit(events...)
	case sc.RemoveCommand:
		events := []sc.Event{sc.RemovalResult{Token: cmd.Token, PlayerID: cmd.PlayerID, RequestID: cmd.RequestID}}
		m, exists := r.members[cmd.PlayerID]
		if exists && m.RequestID == cmd.RequestID {
			delete(r.members, cmd.PlayerID)
			events = append(events, r.wire(message.TypePlayerLeft, message.PlayerLeftPayload{RoomID: r.handle.RoomID, PlayerID: cmd.PlayerID}, r.ordered()))
			if r.host == cmd.PlayerID {
				r.host = ""
				if remaining := r.ordered(); len(remaining) > 0 {
					r.host = remaining[0].PlayerID
					events = append(events, r.wire(message.TypeHostChanged, message.HostChangedPayload{RoomID: r.handle.RoomID, PlayerID: r.host}, remaining))
				}
			}
		}
		ok := r.emit(events...)
		return ok && len(r.members) > 0
	case sc.PublishCommand:
		if len(r.members) == 0 {
			return false
		}
		r.public = true
		return r.emit(sc.Published{Token: cmd.Token})
	case beginStart:
		current, exists := r.members[cmd.member.PlayerID]
		if !exists || current.RequestID != cmd.member.RequestID {
			return true
		}
		code := message.RoomFailureCode("")
		switch {
		case cmd.member.PlayerID != r.host:
			code = message.NotHost
		case r.state != sc.RoomWaiting:
			code = message.InvalidState
		case len(r.members) < 2:
			code = message.NotEnoughPlayers
		}
		if code != "" {
			return r.emit(r.wire(message.TypeRoomCommandFailed, message.RoomCommandFailedPayload{RoomID: r.handle.RoomID, Command: message.TypeStartGame, Code: code}, []sc.Member{current}))
		}
		r.state = sc.RoomStarting
		return r.emit()
	}
	return true
}
