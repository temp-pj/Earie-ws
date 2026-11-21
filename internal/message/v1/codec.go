package message

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"unicode/utf8"

	"github.com/google/uuid"
	music "temp-ws/internal/music/v1"
)

type Direction uint8

const (
	ClientToServer Direction = iota
	ServerToClient
)

// ErrInvalidMessage maps to WebSocket 1008. Frame kind/size limits and endpoint
// negotiation are transport responsibilities (T1), not implemented by this codec.
var ErrInvalidMessage = errors.New("invalid v1 message")

// Message.Payload is a pointer to one of the DTOs in payloads.go after Decode.
// A pointer timestamp preserves absent versus explicit zero on client messages.
type Message struct {
	Type      string `json:"type"`
	Payload   any    `json:"payload"`
	Timestamp *int64 `json:"timestamp,omitempty"`
}

func Decode(data []byte, direction Direction) (Message, error) {
	m, err := decode(data, direction)
	if err != nil {
		return Message{}, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	return m, nil
}

func decode(data []byte, direction Direction) (Message, error) {
	var m Message
	if !utf8.Valid(data) {
		return m, errors.New("frame is not UTF-8")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return m, err
	}
	if err := required(fields["type"], &m.Type); err != nil {
		return m, err
	}
	if raw, exists := fields["timestamp"]; exists {
		var timestamp int64
		if err := required(raw, &timestamp); err != nil {
			return m, err
		}
		m.Timestamp = &timestamp
	} else if direction == ServerToClient {
		return m, errors.New("timestamp is required")
	}
	payload, err := payloadFor(m.Type, direction)
	if err != nil {
		return m, err
	}
	if err := required(fields["payload"], payload); err != nil {
		return m, err
	}
	if err := validatePayload(payload); err != nil {
		return m, err
	}
	m.Payload = payload
	return m, nil
}

// Encode validates the DTO/type/direction combination and emits only known
// fields. The caller supplies its clock timestamp; this package owns no clock.
func Encode(m Message, direction Direction) ([]byte, error) {
	expected, err := payloadFor(m.Type, direction)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	actual := reflect.TypeOf(m.Payload)
	if actual != reflect.TypeOf(expected) && actual != reflect.TypeOf(expected).Elem() {
		return nil, fmt.Errorf("%w: payload does not match %s", ErrInvalidMessage, m.Type)
	}
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	decoded, err := Decode(data, direction)
	if err != nil {
		return nil, err
	}
	return json.Marshal(decoded)
}

func payloadFor(kind string, direction Direction) (any, error) {
	switch direction {
	case ClientToServer:
		switch kind {
		case TypeMatchRequest:
			return &MatchRequestPayload{}, nil
		case TypeMatchCancel:
			return &MatchCancelPayload{}, nil
		case TypeLeaveRoom, TypeStartGame:
			return &RoomPayload{}, nil
		case TypeReadyToPlay:
			return &ReadyToPlayPayload{}, nil
		case TypeSubmitAnswer:
			return &SubmitAnswerPayload{}, nil
		case TypeKickPlayer:
			return &KickPlayerPayload{}, nil
		}
	case ServerToClient:
		switch kind {
		case TypeMatchWaiting, TypeMatchCancelled:
			return &RequestPayload{}, nil
		case TypeMatchFailed:
			return &MatchFailedPayload{}, nil
		case TypeWelcome:
			return &WelcomePayload{}, nil
		case TypeRoomLeft, TypePlayerKicked, TypeGameStarted:
			return &RoomPayload{}, nil
		case TypeRoomCommandFailed:
			return &RoomCommandFailedPayload{}, nil
		case TypeGameAborted:
			return &GameAbortedPayload{}, nil
		case TypePlayerJoined, TypePlayerLeft, TypeHostChanged:
			return &PlayerPayload{}, nil
		case TypePreloadSong:
			return &PreloadSongPayload{}, nil
		case TypeRoundStarted:
			return &RoundStartPayload{}, nil
		case TypeCountDown:
			return &CountDownPayload{}, nil
		case TypeWrongAnswer:
			return &WrongAnswerPayload{}, nil
		case TypeRoundResult:
			return &RoundResultPayload{}, nil
		case TypeGameOver:
			return &GameOverPayload{}, nil
		}
	}
	return nil, fmt.Errorf("unknown type/direction: %s/%d", kind, direction)
}

// required decodes exact JSON field names, rejects missing/null recursively, and
// ignores unknown fields (including differently cased names). Ordinary Unmarshal
// alone would silently accept missing scalar fields and case-insensitive keys.
func required(raw json.RawMessage, target any) error {
	return decodeValue(raw, reflect.ValueOf(target).Elem())
}

func decodeValue(raw json.RawMessage, value reflect.Value) error {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errors.New("missing or null field")
	}
	switch value.Kind() {
	case reflect.Struct:
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return err
		}
		for i := 0; i < value.NumField(); i++ {
			name := value.Type().Field(i).Tag.Get("json")
			if err := decodeValue(fields[name], value.Field(i)); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}
	case reflect.Slice:
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return err
		}
		value.Set(reflect.MakeSlice(value.Type(), len(items), len(items)))
		for i, item := range items {
			if err := decodeValue(item, value.Index(i)); err != nil {
				return err
			}
		}
	case reflect.Map:
		var items map[string]json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return err
		}
		value.Set(reflect.MakeMap(value.Type()))
		for key, item := range items {
			element := reflect.New(value.Type().Elem()).Elem()
			if err := decodeValue(item, element); err != nil {
				return err
			}
			value.SetMapIndex(reflect.ValueOf(key), element)
		}
	default:
		return json.Unmarshal(raw, value.Addr().Interface())
	}
	return nil
}

// ValidID accepts only lowercase canonical RFC 4122 UUID v4 wire identities.
func ValidID(id string) bool {
	u, err := uuid.Parse(id)
	return err == nil && u.String() == id && u.Version() == 4 && u.Variant() == uuid.RFC4122
}

func validatePayload(payload any) error {
	v := reflect.ValueOf(payload).Elem()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		switch v.Type().Field(i).Name {
		case "RequestID", "RoomID", "PlayerID", "HostID", "TargetPlayerID":
			if !ValidID(field.String()) {
				return errors.New("invalid UUID v4")
			}
		case "Winner":
			if field.String() != "" && !ValidID(field.String()) {
				return errors.New("invalid winner UUID")
			}
		case "Players":
			for _, id := range field.Interface().([]string) {
				if !ValidID(id) {
					return errors.New("invalid player UUID")
				}
			}
		case "Scores":
			for id := range field.Interface().(map[string]int) {
				if !ValidID(id) {
					return errors.New("invalid score player UUID")
				}
			}
		}
	}
	switch p := payload.(type) {
	case *WelcomePayload:
		if p.RoomState != "waiting" || p.MaxPlayers != MaxPlayers || len(p.Players) < 1 || len(p.Players) > MaxPlayers || !slices.Contains(p.Players, p.PlayerID) || !slices.Contains(p.Players, p.HostID) {
			return errors.New("invalid welcome membership/state")
		}
		seen := make(map[string]bool, len(p.Players))
		for _, id := range p.Players {
			if seen[id] {
				return errors.New("duplicate welcome player")
			}
			seen[id] = true
		}
		if !music.ValidCatalogVersion(p.CatalogVersion) {
			return errors.New("invalid catalog version")
		}
		return p.Category.ValidateResolved()
	case *MatchFailedPayload:
		switch p.Code {
		case InvalidCriteria, MatchInternalError, AlreadyMatching, AlreadyInRoom:
		default:
			return errors.New("invalid match failure code")
		}
	case *RoomCommandFailedPayload:
		if p.Command != TypeStartGame {
			return errors.New("invalid failed command")
		}
		switch p.Code {
		case NotHost, NotEnoughPlayers, InvalidState, RoomInternalError:
		default:
			return errors.New("invalid room failure code")
		}
	case *GameAbortedPayload:
		if p.Reason != InsufficientPlayers && p.Reason != AbortInternalError {
			return errors.New("invalid abort reason")
		}
	case *ReadyToPlayPayload:
		if p.RoundNumber < 1 {
			return errors.New("invalid round number")
		}
	}
	// MATCH_REQUEST category values/version deliberately reach T2 validation.
	return nil
}
