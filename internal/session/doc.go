// Package session separates socket lifetime from room membership. Registry owns
// connection/membership references under a mutex; the room actor owns actual
// membership mutations. Transport and injected handlers must never wait for
// network I/O, queue space, or another actor while Registry calls them.
//
// T3 wiring uses an EventQueue as room.NewGateway's sink and one ordered consumer
// (normally the matchmaker loop). For each batch the request owner first checks
// operation tokens and cancellations, then calls Registry.Apply with only the
// still-valid admitted members. Apply enqueues WELCOME before that batch's wire
// events, and returns scoped Remove commands for discarded admissions. Those
// removals must complete before publishing an initial room. Match waiting,
// failure, cancellation and replay responses use Registry.Send. Request ordering,
// duplicate/tombstone policy and timeout quarantine remain T3/T4 responsibilities.
// Session cleanup and the matchmaker share NextOperationToken's process counter.
//
// On disconnect, Registry removes a known membership and notifies Handler so it
// can clean pending assignments too. Explicit leaves complete only on the exact
// RemovalResult. The request owner observes these same results to retire matched
// request tombstones; Registry does not duplicate request state.
//
// Keep each RoomHandle until Done AND event drain. If the sink failed and Closed
// could not publish, call Registry.Terminated after Done/drain to terminate its
// connections. Release tracking with Registry.ForgetRoom and Gateway.Forget only
// after this barrier. No timeout or enqueue failure is proof of cleanup.
//
// The room Gateway implements RoomCommander: START_GAME is an admission barrier
// in the Room FIFO, followed by game initialization/completion in that actor.
package session
