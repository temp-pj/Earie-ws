package room

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"temp-ws/internal/game"
	message "temp-ws/internal/message/v1"
	music "temp-ws/internal/music/v1"
	sc "temp-ws/internal/sessioncontract"
)

const commandQueueSize = 256

var creationSequence atomic.Uint64

// Gateway owns the v1 room registry. Each lifecycleRoom exclusively owns its
// membership, admission and game state in run; mu protects only the map.
type Gateway struct {
	ctx   context.Context
	sink  sc.EventSink
	songs SongProvider
	clock Clock
	mu    sync.Mutex
	rooms map[string]*lifecycleRoom
}

var _ sc.RoomGateway = (*Gateway)(nil)

// Option configures dependencies injected by cmd/server or tests.
type Option func(*Gateway)

// WithSongs supplies the fixed-pool song lookup used by START_GAME.
func WithSongs(p SongProvider) Option { return func(g *Gateway) { g.songs = p } }

// WithClock replaces wall-clock preload/round timers (tests only need this).
func WithClock(c Clock) Option { return func(g *Gateway) { g.clock = c } }

func NewGateway(ctx context.Context, sink sc.EventSink, options ...Option) *Gateway {
	g := &Gateway{ctx: ctx, sink: sink, clock: systemClock{}, rooms: make(map[string]*lifecycleRoom)}
	for _, option := range options {
		option(g)
	}
	return g
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
	r := &lifecycleRoom{ctx: roomCtx, cancel: cancel, sink: g.sink, songs: g.songs, clock: g.clock, results: make(chan startResult),
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

// Command implements session.RoomCommander. Game commands share the Room's
// FIFO with Register/Remove, so START_GAME is an admission barrier. A command
// for a closed/forgotten Room is a late command and is ignored, not an error.
func (g *Gateway) Command(ctx context.Context, roomID string, member sc.Member, msg message.Message) error {
	cmd, ok := gameCommand(member, msg)
	if !ok {
		return nil
	}
	err := g.enqueue(ctx, roomID, cmd)
	if errors.Is(err, sc.ErrClosed) || errors.Is(err, sc.ErrRoomNotFound) {
		return nil
	}
	return err
}

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
	// Game state below is owned by run, like members. workers tracks song
	// lookups; Done closes only after they exit. timer is the single deadline.
	songs           SongProvider
	clock           Clock
	results         chan startResult
	workers         sync.WaitGroup
	timer           Timer
	timerC          <-chan time.Time
	startGeneration uint64
	starter         sc.Member
	game            *game.Game
	ready           map[string]bool
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
		r.workers.Wait()
		r.disarm()
		r.game, r.ready = nil, nil
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
		case result := <-r.results:
			if r.ctx.Err() != nil || !r.startFinished(result) {
				return
			}
		case <-r.timerC:
			if r.ctx.Err() != nil || !r.timerFired() {
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
			if len(r.members) > 0 {
				events = append(events, r.departed(cmd.PlayerID)...)
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
	case startGame:
		return r.startGame(cmd)
	case readyToPlay:
		return r.readyToPlay(cmd)
	case submitAnswer:
		return r.submitAnswer(cmd)
	}
	return true
}
