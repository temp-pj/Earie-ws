// Package sessioncontract defines values and nonblocking actor boundaries.
// It imports only v1 value/DTO packages, never client/session/room/matchmaking
// implementations. Implementations and lifecycle enforcement belong to T1–T4.
package sessioncontract

import (
	"context"
	"errors"
	"time"

	message "temp-ws/internal/message/v1"
	music "temp-ws/internal/music/v1"
)

const (
	MaxPlayers        = message.MaxPlayers
	OperationTimeout  = 5 * time.Second
	PreloadTimeout    = 30 * time.Second
	PingInterval      = 20 * time.Second
	PongTimeout       = 10 * time.Second
	WriteTimeout      = 10 * time.Second
	MaxOutboundFrames = 256
	MaxOutboundBytes  = 1024 * 1024
)

// These counters are internal and never appear in the wire protocol. Arrival
// and creation sequences are process-wide monotonically increasing counters.
type ConnectionID uint64
type Sequence uint64
type Revision uint64

// OperationToken is assigned by the dispatcher and must not be reused while a
// previous operation/result can still be in flight. Zero is reserved for events
// not caused by a gateway command (for example spontaneous room shutdown).
type OperationToken uint64

type RequestKey struct {
	ConnectionID ConnectionID
	RequestID    string
}

// A membership is identified by (roomID, playerID, requestID), not player alone.
type Member struct {
	PlayerID        string
	RequestID       string
	ArrivalSequence Sequence
}

type RoomState string

const (
	RoomInitializing RoomState = "initializing"
	RoomWaiting      RoomState = "waiting"
	RoomStarting     RoomState = "starting"
	RoomPlaying      RoomState = "playing"
	RoomResult       RoomState = "result"
	RoomClosed       RoomState = "closed"
)

// PublicState maps internal admission-blocking starting to public playing.
func (s RoomState) PublicState() RoomState {
	if s == RoomStarting {
		return RoomPlaying
	}
	return s
}

type RequestState string

const (
	RequestWaiting   RequestState = "waiting"
	RequestAssigning RequestState = "assigning"
	RequestMatched   RequestState = "matched"
	RequestCancelled RequestState = "cancelled"
	RequestFailed    RequestState = "failed"
)

type RoomSnapshot struct {
	RoomID           string
	CreationSequence Sequence
	Category         music.Category
	CatalogVersion   string
	PlayerCount      int
	MaxPlayers       int
	Accepting        bool
	Revision         Revision
}

// RoomHandle is allocated by the Hub before an initial-pair command is accepted.
// The caller can quarantine/Close this room even if its first result never
// arrives. Done has the same lifetime guarantee as the channel returned by Close.
// Allocation is not admission success or publication of a matchmaking candidate.
type RoomHandle struct {
	RoomID           string
	CreationSequence Sequence
	Done             <-chan struct{}
}

type CreateInitialPairCommand struct {
	Token          OperationToken
	Category       music.Category
	CatalogVersion string
	Members        [2]Member
}
type RegisterCommand struct {
	Token  OperationToken
	RoomID string
	Member Member
}
type RemoveCommand struct {
	Token     OperationToken
	RoomID    string
	PlayerID  string
	RequestID string
}

// Publish is sent only after both initial results and any cancelled membership
// removals have been consumed. An empty initial room is closed, never published.
type PublishCommand struct {
	Token  OperationToken
	RoomID string
}
type CloseCommand struct {
	Token  OperationToken
	RoomID string
}

var (
	ErrClosed       = errors.New("actor closed")
	ErrBackpressure = errors.New("actor queue full")
	ErrRoomNotFound = errors.New("room not found")
)

// RoomGateway methods enqueue without waiting for another actor or queue space.
// nil means accepted, with completion in the ordered event sink; an error means
// no operation was accepted. Context cancellation before acceptance is an error;
// after acceptance it never proves registration did not happen: consume the
// result and issue scoped Remove. A token is immutable and echoed in completion,
// so late/duplicate callbacks cannot complete a newer operation.
// CreateInitialPair returns the reserved identity/lifetime handle immediately;
// registration success/failure is still delivered asynchronously in a batch.
//
// Close initiates cancellation and returns immediately with Done. Done closes
// only after commands, registration, workers and event publication have stopped.
// The consumer must ALSO drain that room's queued batches before releasing its
// quarantine/terminal record. No timeout alone proves cleanup is complete.
// A successful Close always returns a non-nil channel; repeated Close returns
// the same lifetime barrier. Implementations retain it until the stream drains.
type RoomGateway interface {
	CreateInitialPair(context.Context, CreateInitialPairCommand) (RoomHandle, error)
	Register(context.Context, RegisterCommand) error
	Remove(context.Context, RemoveCommand) error
	Publish(context.Context, PublishCommand) error
	Close(context.Context, CloseCommand) (<-chan struct{}, error)
}

type AdmissionRejection string

const (
	AdmissionAccepted      AdmissionRejection = ""
	AdmissionFull          AdmissionRejection = "full"
	AdmissionStarted       AdmissionRejection = "started"
	AdmissionClosed        AdmissionRejection = "closed"
	AdmissionDuplicate     AdmissionRejection = "duplicate"
	AdmissionInternalError AdmissionRejection = "internal_error"
)

// Admission contains the actor-turn state needed to build WELCOME centrally.
// Successful admission has State=RoomWaiting, including the private initial pair;
// private/public visibility is separate from this externally reported state.
// Players are ordered by arrival sequence. Slices/maps in all contract values
// transfer ownership to the consumer; publishers must copy before reusing them.
type Admission struct {
	HostID  string
	Players []string
	State   RoomState
}

type Event interface{ roomEvent() }

// RegistrationResult is terminal for one accepted Register command. A rejection
// includes the latest snapshot in its enclosing batch for retry suppression.
type RegistrationResult struct {
	Token     OperationToken
	Member    Member
	Rejection AdmissionRejection
	Admission Admission
}

func (RegistrationResult) roomEvent() {}

// InitialPairResult is a single atomic completion: both memberships succeed or
// both fail. The enclosing batch supplies the newly allocated roomID; the room
// remains private until Publish. Cancellation is handled by later scoped Remove.
type InitialPairResult struct {
	Token     OperationToken
	Members   [2]Member
	Rejection AdmissionRejection
	Admission Admission
}

func (InitialPairResult) roomEvent() {}

// RemovalResult confirms cleanup, including when that exact membership was
// already absent. Removal failure is not represented as successful cleanup;
// timeout/fault requires Close, Done and drain before another assignment.
type RemovalResult struct {
	Token     OperationToken
	PlayerID  string
	RequestID string
}

func (RemovalResult) roomEvent() {}

type Published struct{ Token OperationToken }

func (Published) roomEvent() {}

// WireEvent targets explicit memberships, preventing an old room/request from
// reaching a replacement membership. It never carries WELCOME: the dispatcher
// queues WELCOME from admission results before subsequent wire events.
type WireEvent struct {
	Recipients []Member
	Message    message.Message
}

func (WireEvent) roomEvent() {}

// Closed is terminal. Consumers ignore later snapshots for this roomID. It does
// not by itself prove Done has closed or the queued stream has been drained.
type Closed struct{ Token OperationToken }

func (Closed) roomEvent() {}

// EventBatch is one indivisible actor turn in the room's FIFO stream. RoomID and
// Revision scope every result and wire event; Snapshot has the same ID/revision.
// Revisions increase between batches. Snapshot-only batches use an empty Events
// slice. Consumers apply events in slice order and accept only newer snapshots.
// Private initial rooms must not enter matchmaking candidates before Published.
type EventBatch struct {
	RoomID   string
	Revision Revision
	Snapshot RoomSnapshot
	Events   []Event
}

// TryPublish atomically accepts the entire batch without blocking. On failure,
// no prefix may have escaped; the actor must fail/close rather than drop events.
// One serialized publisher owns each room stream; the consumer owns dispatch.
type EventSink interface{ TryPublish(EventBatch) error }
