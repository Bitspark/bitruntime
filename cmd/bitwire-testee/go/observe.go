package main

import (
	"strings"
	"sync"
	"time"

	"github.com/Bitspark/nightseam/runtime/go"
	"github.com/Bitspark/nightseam/tunnel/go"
)

// recorder is an observer that keeps what it is told, from whatever
// goroutine, for peer.observed to report in the driver's one shape. It keeps
// the events only where a peer was made with observe: true, and latches the
// close whether or not it was, since how a connection ended is what
// peer.await_close answers with.
type recorder struct {
	mu     sync.Mutex
	keep   bool
	events []runtime.ObserverEvent
	closed *runtime.ConnectionClosed
	ended  chan struct{}
}

func newRecorder() *recorder { return &recorder{ended: make(chan struct{})} }

func (r *recorder) Observe(event runtime.ObserverEvent) {
	r.mu.Lock()
	if r.keep {
		r.events = append(r.events, event)
	}
	if closed, ok := event.(runtime.ConnectionClosed); ok && r.closed == nil {
		r.closed = &closed
		close(r.ended)
	}
	r.mu.Unlock()
}

// whenClosed is the close the peer was told of, waited for up to within.
func (r *recorder) whenClosed(within time.Duration) (runtime.ConnectionClosed, bool) {
	select {
	case <-r.ended:
	case <-time.After(within):
		return runtime.ConnectionClosed{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return *r.closed, true
}

func (r *recorder) report(withTrace, drain bool) []map[string]any {
	r.mu.Lock()
	events := r.events
	if drain {
		r.events = nil
	}
	r.mu.Unlock()
	out := make([]map[string]any, 0, len(events))
	for _, event := range events {
		out = append(out, normalize(event, withTrace))
	}
	return out
}

// normalize renders one event as DRIVER.md says every language reports it:
// at stripped, sizes and durations as their presence, empty strings
// omitted, the trace split when asked for.
func normalize(event runtime.ObserverEvent, withTrace bool) map[string]any {
	m := map[string]any{}
	put := func(key string, value any) {
		switch v := value.(type) {
		case string:
			if v == "" {
				return
			}
		case int:
			m[key] = v
			return
		case int64:
			m[key] = v
			return
		case time.Duration:
			m[key] = v >= 0
			return
		}
		m[key] = value
	}
	bytes := func(n int) { m["bytes"] = n > 0 }
	trace := func(t runtime.Trace) {
		if !withTrace || t.Parent == "" {
			return
		}
		parts := strings.Split(t.Parent, "-")
		if len(parts) != 4 {
			m["trace"] = map[string]any{"traceparent": t.Parent}
			return
		}
		split := map[string]any{"trace_id": parts[1], "span_id": parts[2], "flags": parts[3]}
		if t.State != "" {
			split["state"] = t.State
		}
		m["trace"] = split
	}
	switch e := event.(type) {
	case runtime.ConnectionOpened:
		m["type"] = "connection.opened"
		put("role", string(e.Role))
	case runtime.ConnectionClosed:
		m["type"] = "connection.closed"
		put("code", e.Code)
		put("reason", e.Reason)
		m["local"] = e.Local
	case runtime.FrameSent:
		m["type"] = "frame.sent"
		put("kind", e.Kind)
		put("name", e.Name)
		bytes(e.Bytes)
		put("id", e.ID)
		put("family", e.Family)
		trace(e.Trace)
	case runtime.FrameReceived:
		m["type"] = "frame.received"
		put("kind", e.Kind)
		put("name", e.Name)
		bytes(e.Bytes)
		put("id", e.ID)
		put("family", e.Family)
		trace(e.Trace)
	case runtime.RequestStarted:
		m["type"] = "request.started"
		put("id", e.ID)
		put("method", e.Method)
		m["incoming"] = e.Incoming
		put("family", e.Family)
		trace(e.Trace)
	case runtime.RequestEnded:
		m["type"] = "request.ended"
		put("id", e.ID)
		put("method", e.Method)
		m["incoming"] = e.Incoming
		put("duration", e.Duration)
		put("outcome", e.Outcome.String())
		put("error_code", e.ErrorCode)
		put("family", e.Family)
		trace(e.Trace)
	case runtime.EventEmitted:
		m["type"] = "event.emitted"
		put("name", e.Name)
		bytes(e.Bytes)
		put("family", e.Family)
		trace(e.Trace)
	case runtime.EventDelivered:
		m["type"] = "event.delivered"
		put("name", e.Name)
		bytes(e.Bytes)
		put("family", e.Family)
		trace(e.Trace)
	case runtime.Backpressure:
		m["type"] = "backpressure"
		put("queued", e.Queued)
		m["stalled"] = e.Stalled
		put("deadline", e.Deadline)
	case runtime.HandlerPanic:
		m["type"] = "handler.panic"
		put("method", e.Method)
		put("value", e.Value)
		put("family", e.Family)
		trace(e.Trace)
	case tunnel.ChannelOpened:
		m["type"] = "channel.opened"
		put("family", e.Family)
		put("id", e.ID)
		m["opener"] = e.Opener
	case tunnel.ChannelAccepted:
		m["type"] = "channel.accepted"
		put("family", e.Family)
		put("id", e.ID)
	case tunnel.ChannelClosed:
		m["type"] = "channel.closed"
		put("family", e.Family)
		put("id", e.ID)
		put("code", e.Code)
		put("reason", e.Reason)
	case tunnel.CreditStall:
		m["type"] = "credit.stall"
		put("family", e.Family)
		put("id", e.ID)
		put("waiting", e.Waiting)
	case tunnel.OpenRefused:
		m["type"] = "open.refused"
		put("family", e.Family)
		put("reason", e.Reason)
	default:
		m["type"] = "unknown"
	}
	return m
}
