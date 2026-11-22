// Package message contains the WebSocket v1 contract. The pre-v1 protocol
// package was removed with the legacy Room/Client runtime (T7).
package message

import music "temp-ws/internal/music/v1"

const (
	ProtocolVersion       = "1"
	MaxFrameBytes         = 64 * 1024
	MaxRequestIDs         = 4096
	MaxPlayers            = 8
	TypeMatchRequest      = "MATCH_REQUEST"
	TypeMatchWaiting      = "MATCH_WAITING"
	TypeMatchCancel       = "MATCH_CANCEL"
	TypeMatchCancelled    = "MATCH_CANCELLED"
	TypeMatchFailed       = "MATCH_FAILED"
	TypeWelcome           = "WELCOME"
	TypeLeaveRoom         = "LEAVE_ROOM"
	TypeRoomLeft          = "ROOM_LEFT"
	TypeStartGame         = "START_GAME"
	TypeRoomCommandFailed = "ROOM_COMMAND_FAILED"
	TypeGameAborted       = "GAME_ABORTED"
	TypePlayerJoined      = "PLAYER_JOINED"
	TypePlayerLeft        = "PLAYER_LEFT"
	TypeHostChanged       = "HOST_CHANGED"
	TypePlayerKicked      = "PLAYER_KICKED"
	TypeGameStarted       = "GAME_STARTED"
	TypePreloadSong       = "PRELOAD_SONG"
	TypeRoundStarted      = "ROUND_STARTED"
	TypeCountDown         = "COUNT_DOWN"
	TypeWrongAnswer       = "WRONG_ANSWER"
	TypeRoundResult       = "ROUND_RESULT"
	TypeGameOver          = "GAME_OVER"
	TypeReadyToPlay       = "READY_TO_PLAY"
	TypeSubmitAnswer      = "SUBMIT_ANSWER"
	TypeKickPlayer        = "KICK_PLAYER"
)

type MatchFailureCode string

const (
	InvalidCriteria    MatchFailureCode = "INVALID_CRITERIA"
	MatchInternalError MatchFailureCode = "INTERNAL_ERROR"
	AlreadyMatching    MatchFailureCode = "ALREADY_MATCHING"
	AlreadyInRoom      MatchFailureCode = "ALREADY_IN_ROOM"
)

type RoomFailureCode string

const (
	NotHost           RoomFailureCode = "NOT_HOST"
	NotEnoughPlayers  RoomFailureCode = "NOT_ENOUGH_PLAYERS"
	InvalidState      RoomFailureCode = "INVALID_STATE"
	RoomInternalError RoomFailureCode = "INTERNAL_ERROR"
)

type AbortReason string

const (
	InsufficientPlayers AbortReason = "INSUFFICIENT_PLAYERS"
	AbortInternalError  AbortReason = "INTERNAL_ERROR"
)

type MatchRequestPayload struct {
	RequestID      string         `json:"requestID"`
	CatalogVersion string         `json:"catalogVersion"`
	Category       music.Category `json:"category"`
}
type RequestPayload struct {
	RequestID string `json:"requestID"`
}
type MatchWaitingPayload = RequestPayload
type MatchCancelPayload = RequestPayload
type MatchCancelledPayload = RequestPayload
type MatchFailedPayload struct {
	RequestID string           `json:"requestID"`
	Code      MatchFailureCode `json:"code"`
}
type WelcomePayload struct {
	RequestID      string         `json:"requestID"`
	RoomID         string         `json:"roomID"`
	PlayerID       string         `json:"playerID"`
	HostID         string         `json:"hostID"`
	Players        []string       `json:"players"`
	RoomState      string         `json:"roomState"`
	MaxPlayers     int            `json:"maxPlayers"`
	Category       music.Category `json:"category"`
	CatalogVersion string         `json:"catalogVersion"`
}
type RoomPayload struct {
	RoomID string `json:"roomID"`
}
type LeaveRoomPayload = RoomPayload
type RoomLeftPayload = RoomPayload
type StartGamePayload = RoomPayload
type PlayerKickedPayload = RoomPayload
type GameStartedPayload = RoomPayload
type RoomCommandFailedPayload struct {
	RoomID  string          `json:"roomID"`
	Command string          `json:"command"`
	Code    RoomFailureCode `json:"code"`
}
type GameAbortedPayload struct {
	RoomID string      `json:"roomID"`
	Reason AbortReason `json:"reason"`
}
type PlayerPayload struct {
	RoomID   string `json:"roomID"`
	PlayerID string `json:"playerID"`
}
type PlayerJoinedPayload = PlayerPayload
type PlayerLeftPayload = PlayerPayload
type HostChangedPayload = PlayerPayload
type ReadyToPlayPayload struct {
	RoomID      string `json:"roomID"`
	RoundNumber int    `json:"roundNumber"`
}
type SubmitAnswerPayload struct {
	RoomID string `json:"roomID"`
	Answer string `json:"answer"`
}
type KickPlayerPayload struct {
	RoomID         string `json:"roomID"`
	TargetPlayerID string `json:"targetPlayerID"`
}
type PreloadSongPayload struct {
	RoomID      string `json:"roomID"`
	ISRC        string `json:"isrc"`
	StartTime   int    `json:"startTime"`
	RoundNumber int    `json:"roundNumber"`
}
type RoundStartPayload struct {
	RoomID       string   `json:"roomID"`
	RoundNumber  int      `json:"roundNumber"`
	TotalRounds  int      `json:"totalRounds"`
	AnswerLength int      `json:"answerLength"`
	LetterCards  []string `json:"letterCards"`
}
type CountDownPayload struct {
	RoomID    string `json:"roomID"`
	Remaining int    `json:"remaining"`
}
type WrongAnswerPayload struct {
	RoomID      string `json:"roomID"`
	PlayerID    string `json:"playerID"`
	WrongAnswer string `json:"wrongAnswer"`
}

// Winner may be empty when the existing game has no winner. Score keys and
// nonempty winners are player IDs; T0 does not change scoring/tie rules.
type RoundResultPayload struct {
	RoomID        string         `json:"roomID"`
	Winner        string         `json:"winner"`
	CorrectAnswer string         `json:"correctAnswer"`
	Scores        map[string]int `json:"scores"`
}
type GameOverPayload struct {
	RoomID string         `json:"roomID"`
	Winner string         `json:"winner"`
	Scores map[string]int `json:"scores"`
}
