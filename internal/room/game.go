package room

import (
	"context"
	"errors"
	"time"

	"temp-ws/internal/game"
	message "temp-ws/internal/message/v1"
	gamemusic "temp-ws/internal/music"
	music "temp-ws/internal/music/v1"
	sc "temp-ws/internal/sessioncontract"
)

// RoundSeconds is the existing default round limit (REQ-GAME-01). START_GAME
// never supplies category, track count or time; the Room is authoritative.
const RoundSeconds = 30

// SongProvider loads exactly the fixed room pool. It runs outside the actor and
// must return promptly once ctx is cancelled: Room Done waits for the worker.
type SongProvider interface {
	FetchSongs(ctx context.Context, catalogVersion string, category music.Category) ([]music.ManifestSong, error)
}

var errNotEnoughSongs = errors.New("song provider returned fewer than 10 distinct ISRCs")

type startGame struct{ member sc.Member }
type readyToPlay struct {
	member sc.Member
	round  int
}
type submitAnswer struct {
	member sc.Member
	answer string
}
type kickPlayer struct {
	member sc.Member
	target string
}

// startResult is tagged with the start generation. A departure that cancels
// the start increments the generation, so a late fetch result is discarded.
type startResult struct {
	generation uint64
	songs      []music.ManifestSong
	err        error
}

// gameCommand maps routed v1 commands to actor inputs.
func gameCommand(member sc.Member, msg message.Message) (any, bool) {
	switch p := msg.Payload.(type) {
	case *message.RoomPayload:
		if msg.Type == message.TypeStartGame {
			return startGame{member}, true
		}
	case *message.ReadyToPlayPayload:
		if msg.Type == message.TypeReadyToPlay {
			return readyToPlay{member, p.RoundNumber}, true
		}
	case *message.SubmitAnswerPayload:
		if msg.Type == message.TypeSubmitAnswer {
			return submitAnswer{member, p.Answer}, true
		}
	case *message.KickPlayerPayload:
		if msg.Type == message.TypeKickPlayer {
			return kickPlayer{member, p.TargetPlayerID}, true
		}
	}
	return nil, false
}

// current reports whether m is still this exact membership (not a stale request).
func (r *lifecycleRoom) current(m sc.Member) bool {
	existing, ok := r.members[m.PlayerID]
	return ok && existing == m
}

func (r *lifecycleRoom) broadcast(kind string, payload any) sc.WireEvent {
	return r.wire(kind, payload, r.ordered())
}

func (r *lifecycleRoom) commandFailed(to sc.Member, code message.RoomFailureCode) sc.WireEvent {
	return r.wire(message.TypeRoomCommandFailed, message.RoomCommandFailedPayload{RoomID: r.handle.RoomID, Command: message.TypeStartGame, Code: code}, []sc.Member{to})
}

func (r *lifecycleRoom) startGame(cmd startGame) bool {
	if !r.current(cmd.member) {
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
		return r.emit(r.commandFailed(cmd.member, code))
	}
	// starting closes admission in this same actor turn; later Register fails.
	r.state = sc.RoomStarting
	r.startGeneration++
	r.starter = cmd.member
	r.fetch(r.startGeneration)
	return r.emit()
}

// fetch runs song lookup outside the actor. Done waits for this worker, and its
// delivery selects on the Room context so it cannot block after shutdown.
func (r *lifecycleRoom) fetch(generation uint64) {
	provider, version, category := r.songs, r.snapshot.CatalogVersion, r.snapshot.Category
	r.workers.Add(1)
	go func() {
		defer r.workers.Done()
		result := startResult{generation: generation, err: sc.ErrClosed}
		if provider != nil {
			ctx, cancel := context.WithTimeout(r.ctx, sc.OperationTimeout)
			result.songs, result.err = provider.FetchSongs(ctx, version, category)
			cancel()
		}
		select {
		case r.results <- result:
		case <-r.ctx.Done():
		}
	}()
}

func (r *lifecycleRoom) startFinished(res startResult) bool {
	if res.generation != r.startGeneration || r.state != sc.RoomStarting {
		return true
	}
	songs, err := gameSongs(res.songs, res.err)
	if err != nil {
		r.state = sc.RoomWaiting
		var events []sc.Event
		if r.current(r.starter) {
			events = append(events, r.commandFailed(r.starter, message.RoomInternalError))
		}
		return r.emit(events...)
	}
	players := make([]string, 0, len(r.members))
	for _, m := range r.ordered() {
		players = append(players, m.PlayerID)
	}
	r.game = game.NewGame(players, songs, RoundSeconds)
	r.state = sc.RoomPlaying
	started := r.broadcast(message.TypeGameStarted, message.GameStartedPayload{RoomID: r.handle.RoomID})
	return r.emit(started, r.preload())
}

// gameSongs requires 10 distinct ISRCs from the fixed pool. It keeps the
// existing game start-time generator; manifest startTime is not substituted.
func gameSongs(tracks []music.ManifestSong, err error) ([]gamemusic.Song, error) {
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, music.TracksPerGame)
	songs := make([]gamemusic.Song, 0, music.TracksPerGame)
	for _, t := range tracks {
		if len(songs) == music.TracksPerGame {
			break
		}
		if t.ISRC == "" || seen[t.ISRC] {
			continue
		}
		seen[t.ISRC] = true
		released, _ := time.Parse(time.DateOnly, t.ReleaseDate)
		songs = append(songs, gamemusic.Song{MBID: t.MBID, Title: t.Title, ISRC: t.ISRC, Artist: t.Artist, ReleaseDate: released, Duration: t.Duration})
	}
	if len(songs) < music.TracksPerGame {
		return nil, errNotEnoughSongs
	}
	return songs, nil
}

// preload announces the current round and arms the 30-second preload guard.
func (r *lifecycleRoom) preload() sc.Event {
	r.ready = make(map[string]bool)
	r.arm(sc.PreloadTimeout)
	return r.broadcast(message.TypePreloadSong, message.PreloadSongPayload{RoomID: r.handle.RoomID, ISRC: r.game.CurrentISRC(), StartTime: r.game.CurrentStartTime(), RoundNumber: r.game.CurrentRound()})
}

func (r *lifecycleRoom) preloading() bool {
	return r.state == sc.RoomPlaying && r.game != nil && !r.game.IsPlaying()
}

func (r *lifecycleRoom) allReady() bool {
	for id := range r.members {
		if !r.ready[id] {
			return false
		}
	}
	return true
}

func (r *lifecycleRoom) readyToPlay(cmd readyToPlay) bool {
	if !r.current(cmd.member) || !r.preloading() || cmd.round != r.game.CurrentRound() {
		return true
	}
	r.ready[cmd.member.PlayerID] = true
	if !r.allReady() {
		return true
	}
	return r.emit(r.startRound())
}

// startRound runs once per round: preloading() becomes false in this turn, so
// duplicate READY or a re-evaluation after departure cannot start it again.
func (r *lifecycleRoom) startRound() sc.Event {
	r.ready = make(map[string]bool)
	now := r.clock.Now()
	r.game.StartRoundAt(now)
	r.roundDeadline = now.Add(RoundSeconds * time.Second)
	r.armTick(now)
	info := r.game.GetRoundStartInfo()
	cards := info.LetterCards
	if cards == nil {
		cards = []string{}
	}
	return r.broadcast(message.TypeRoundStarted, message.RoundStartPayload{RoomID: r.handle.RoomID, RoundNumber: info.RoundNumber, TotalRounds: info.TotalRounds, AnswerLength: info.AnswerLength, LetterCards: cards})
}

func (r *lifecycleRoom) submitAnswer(cmd submitAnswer) bool {
	if !r.current(cmd.member) || r.state != sc.RoomPlaying || r.game == nil || !r.game.IsPlaying() {
		return true
	}
	// An answer processed at/after the fixed deadline cannot score, even if
	// the expiry tick has not been serviced yet: the round ends as a timeout.
	if !r.clock.Now().Before(r.roundDeadline) {
		return r.emit(r.endRound("")...)
	}
	if !r.game.SubmitAnswer(cmd.answer) {
		return r.emit(r.broadcast(message.TypeWrongAnswer, message.WrongAnswerPayload{RoomID: r.handle.RoomID, PlayerID: cmd.member.PlayerID, WrongAnswer: cmd.answer}))
	}
	return r.emit(r.endRound(cmd.member.PlayerID)...)
}

// endRound keeps the existing scoring/winner rules. After the 10th round the
// Room enters result: no restart, no admission, members leave individually.
func (r *lifecycleRoom) endRound(winner string) []sc.Event {
	r.disarm()
	r.game.EndRound(winner)
	result := r.broadcast(message.TypeRoundResult, message.RoundResultPayload{RoomID: r.handle.RoomID, Winner: winner, CorrectAnswer: r.game.CurrentAnswer(), Scores: r.game.Scores()})
	if r.game.NextRound() {
		return []sc.Event{result, r.preload()}
	}
	over := r.broadcast(message.TypeGameOver, message.GameOverPayload{RoomID: r.handle.RoomID, Winner: r.game.Winner(), Scores: r.game.Scores()})
	r.game, r.ready, r.state = nil, nil, sc.RoomResult
	return []sc.Event{result, over}
}

// abort ends the game early without computing a new score or winner.
func (r *lifecycleRoom) abort(reason message.AbortReason) sc.Event {
	r.disarm()
	r.game, r.ready, r.state = nil, nil, sc.RoomResult
	return r.broadcast(message.TypeGameAborted, message.GameAbortedPayload{RoomID: r.handle.RoomID, Reason: reason})
}

// timerFired handles the single actor-owned deadline: a preload guard while
// preloading, otherwise the one-second countdown/round-limit tick.
func (r *lifecycleRoom) timerFired() bool {
	r.timer, r.timerC = nil, nil
	if r.state != sc.RoomPlaying || r.game == nil {
		return true
	}
	if !r.game.IsPlaying() {
		return r.emit(r.abort(message.AbortInternalError))
	}
	now := r.clock.Now()
	remaining := r.game.RemainingTimeAt(now)
	if remaining <= 0 || !now.Before(r.roundDeadline) {
		return r.emit(r.endRound("")...)
	}
	r.armTick(now)
	return r.emit(r.broadcast(message.TypeCountDown, message.CountDownPayload{RoomID: r.handle.RoomID, Remaining: remaining}))
}

// departed returns game events caused by a membership removal in this turn.
// At least one member remains; an empty Room is closed by the caller.
func (r *lifecycleRoom) departed(playerID string) []sc.Event {
	switch r.state {
	case sc.RoomStarting:
		if len(r.members) >= 2 {
			return nil
		}
		// Invalidate the in-flight lookup; its late result is discarded.
		r.startGeneration++
		r.state = sc.RoomWaiting
		if r.current(r.starter) {
			return []sc.Event{r.commandFailed(r.starter, message.NotEnoughPlayers)}
		}
	case sc.RoomPlaying:
		delete(r.ready, playerID)
		if len(r.members) < 2 {
			return []sc.Event{r.abort(message.InsufficientPlayers)}
		}
		if r.preloading() && r.allReady() {
			return []sc.Event{r.startRound()}
		}
	}
	return nil
}

// armTick schedules the next countdown tick, capped at the absolute round
// deadline so a delayed tick never extends the fixed 30-second limit.
func (r *lifecycleRoom) armTick(now time.Time) {
	r.arm(min(time.Second, r.roundDeadline.Sub(now)))
}

// kickPlayer keeps the v1 KICK_PLAYER operation (only a kick UI is out of
// scope). The host removes another current member in this actor turn: the
// target receives PLAYER_KICKED first, then the usual departure events. The
// request owner reacts to PLAYER_KICKED with a scoped Remove, which confirms
// the already-absent membership and releases the target's connection for a
// new request. A self-target is a no-op; LEAVE_ROOM is the self path.
func (r *lifecycleRoom) kickPlayer(cmd kickPlayer) bool {
	if !r.current(cmd.member) || cmd.member.PlayerID != r.host || cmd.target == r.host {
		return true
	}
	target, ok := r.members[cmd.target]
	if !ok {
		return true
	}
	kicked := r.wire(message.TypePlayerKicked, message.PlayerKickedPayload{RoomID: r.handle.RoomID}, []sc.Member{target})
	return r.emit(append([]sc.Event{kicked}, r.removeMember(cmd.target)...)...)
}

func (r *lifecycleRoom) arm(d time.Duration) {
	r.disarm()
	r.timer = r.clock.NewTimer(d)
	r.timerC = r.timer.C()
}

func (r *lifecycleRoom) disarm() {
	if r.timer != nil {
		r.timer.Stop()
	}
	r.timer, r.timerC = nil, nil
}
