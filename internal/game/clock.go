package game

import (
	"math"
	"time"
)

// StartRoundAt starts the current round on the caller's clock without creating
// a timer goroutine. The Room actor owns the round deadline and countdown, so
// stale timers cannot reach a later round and all timers stop with the Room.
func (g *Game) StartRoundAt(now time.Time) {
	g.rounds[g.currentRound].State = Playing
	g.roundStartedAt = now
	g.roundTimer = nil
}

// RemainingTimeAt reports whole seconds left in the current round at now,
// using the same rounding as RemainingTime.
func (g *Game) RemainingTimeAt(now time.Time) int {
	remaining := g.roundDuration - now.Sub(g.roundStartedAt)
	if remaining <= 0 {
		return 0
	}
	return int(math.Ceil(remaining.Seconds()))
}
