package client

import (
	"context"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	message "temp-ws/internal/message/v1"
	sc "temp-ws/internal/sessioncontract"
)

// Connection owns the socket, bounded FIFO, and its reader/writer/ping workers.
// Room membership never closes its queue or its context. Legacy Client remains
// available to old game tests until T7 removes the obsolete protocol.
type Connection struct {
	ID      string
	conn    socket
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	queue   [][]byte
	bytes   int
	wake    chan struct{}
	done    chan struct{}
	runOnce sync.Once
}

// socket keeps deadline/cancellation tests independent of network timing.
type socket interface {
	SetReadLimit(int64)
	Read(context.Context) (websocket.MessageType, []byte, error)
	Write(context.Context, websocket.MessageType, []byte) error
	Ping(context.Context) error
	Close(websocket.StatusCode, string) error
	CloseNow() error
}

func NewConnection(ctx context.Context, conn *websocket.Conn) *Connection {
	ctx, cancel := context.WithCancel(ctx)
	return &Connection{ID: uuid.NewString(), conn: conn, ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1), done: make(chan struct{})}
}
func (c *Connection) Context() context.Context { return c.ctx }
func (c *Connection) Done() <-chan struct{}    { return c.done }

// Send validates and copies the wire value before enqueueing; callers may reuse
// their DTO after return. Queue overflow terminates the connection immediately.
func (c *Connection) Send(msg message.Message) error {
	data, err := message.Encode(msg, message.ServerToClient)
	if err != nil {
		return err
	}
	return c.enqueue(data)
}
func (c *Connection) enqueue(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ctx.Err() != nil {
		return sc.ErrClosed
	}
	if len(c.queue) >= sc.MaxOutboundFrames || c.bytes+len(data) > sc.MaxOutboundBytes {
		c.cancel()
		return sc.ErrBackpressure
	}
	c.queue = append(c.queue, data)
	c.bytes += len(data)
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return nil
}
func (c *Connection) take() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.queue) == 0 {
		return nil
	}
	data := c.queue[0]
	c.queue[0] = nil
	c.queue = c.queue[1:]
	c.bytes -= len(data)
	return data
}
func (c *Connection) Stop() { c.cancel() }

// Run blocks until every connection worker has exited. Handler must enqueue
// without waiting for an actor; failure closes the connection for cleanup.
func (c *Connection) Run(handler func(message.Message) error) {
	c.runOnce.Do(func() { c.run(handler) })
}
func (c *Connection) run(handler func(message.Message) error) {
	defer close(c.done)
	defer c.cancel()
	c.conn.SetReadLimit(message.MaxFrameBytes)
	var workers sync.WaitGroup
	workers.Add(3)
	go func() { defer workers.Done(); c.writeLoop() }()
	go func() { defer workers.Done(); c.pingLoop() }()
	go func() {
		defer workers.Done()
		<-c.ctx.Done()
		_ = c.conn.CloseNow()
	}()
	for {
		kind, data, err := c.conn.Read(c.ctx)
		if err != nil {
			break
		}
		if kind != websocket.MessageText {
			_ = c.conn.Close(websocket.StatusUnsupportedData, "text frames required")
			break
		}
		msg, err := message.Decode(data, message.ClientToServer)
		if err != nil {
			_ = c.conn.Close(websocket.StatusPolicyViolation, "invalid message")
			break
		}
		if err := handler(msg); err != nil {
			break
		}
	}
	c.cancel()
	workers.Wait()
	c.mu.Lock()
	c.queue = nil
	c.bytes = 0
	c.mu.Unlock()
}
func (c *Connection) writeLoop() {
	defer c.cancel()
	for {
		if c.ctx.Err() != nil {
			return
		}
		if data := c.take(); data != nil {
			ctx, cancel := context.WithTimeout(c.ctx, sc.WriteTimeout)
			err := c.conn.Write(ctx, websocket.MessageText, data)
			cancel()
			if err != nil {
				return
			}
			continue
		}
		select {
		case <-c.ctx.Done():
			return
		case <-c.wake:
		}
	}
}
func (c *Connection) pingLoop() {
	ticker := time.NewTicker(sc.PingInterval)
	defer ticker.Stop()
	c.pingOn(ticker.C)
}
func (c *Connection) pingOn(ticks <-chan time.Time) {
	defer c.cancel()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticks:
			ctx, cancel := context.WithTimeout(c.ctx, sc.PongTimeout)
			err := c.conn.Ping(ctx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}
