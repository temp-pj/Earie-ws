package session

import (
	"context"
	sc "temp-ws/internal/sessioncontract"
)

// EventQueue transfers entire batches without waiting for its single consumer.
// It is never closed by a publisher. Stop producers, await their Done barriers,
// then drain before discarding it. A full queue makes the publishing room fail.
type EventQueue struct {
	ctx     context.Context
	batches chan sc.EventBatch
}

func NewEventQueue(ctx context.Context, capacity int) *EventQueue {
	return &EventQueue{ctx: ctx, batches: make(chan sc.EventBatch, capacity)}
}
func (q *EventQueue) Batches() <-chan sc.EventBatch { return q.batches }
func (q *EventQueue) TryPublish(batch sc.EventBatch) error {
	if q.ctx.Err() != nil {
		return sc.ErrClosed
	}
	select {
	case q.batches <- batch:
		return nil
	default:
		return sc.ErrBackpressure
	}
}
