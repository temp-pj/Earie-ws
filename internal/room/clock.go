package room

import "time"

// Clock is injected so preload/round deadlines are tested with controlled time.
// Timers are owned by the Room actor and are never shared between Rooms.
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

// Timer delivers at most one value on C. Stop prevents an undelivered fire.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) NewTimer(d time.Duration) Timer {
	return systemTimer{time.NewTimer(d)}
}

type systemTimer struct{ t *time.Timer }

func (t systemTimer) C() <-chan time.Time { return t.t.C }
func (t systemTimer) Stop() bool          { return t.t.Stop() }
