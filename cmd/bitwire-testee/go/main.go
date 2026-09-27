// Ported from nightseam v0.6.0 conformance/go/testee/main.go (5cc9723).

// Command bitwire-testee puts bitruntime's Go peer under the control of a
// bitwire/1 conformance runner, over driver 1 of bitwire's conformance
// contract (conformance/protocol/CONTRACT.md). It is a worked example of what
// a testee is: a loop reading one request per line, a table of handles, an
// inbox per handle for what arrived unasked, and nothing on stdout but
// answers.
//
// It claims the core: the seam (conn.*) and the peer (peer.*, call.*).
// bitruntime implements neither the tunnel nor live references yet and has no
// observer, so those ops, and the peer options that ask for an observer,
// answer unsupported.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

const driverVersion = 1

func main() {
	t := newTestee()
	in := bufio.NewReaderSize(os.Stdin, 1<<20)
	out := bufio.NewWriter(os.Stdout)
	for {
		line, err := in.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			out.Write(t.serve(line).encode())
			out.WriteByte('\n')
			out.Flush()
			if t.bye {
				t.reset()
				return
			}
		}
		if err != nil {
			t.reset()
			return
		}
	}
}

// request is one line the runner sent; args is everything but id and op.
type request struct {
	id   int
	op   string
	args map[string]json.RawMessage
}

type answer struct {
	ID    int      `json:"id"`
	OK    any      `json:"ok,omitempty"`
	Error *failure `json:"error,omitempty"`
}

// encode is the answer's line. An answer that cannot be encoded — a payload
// that is not JSON — is answered as the testee's own failure, since a line
// that is not an answer would leave the runner holding the testee dead.
func (a answer) encode() []byte {
	data, err := json.Marshal(a)
	if err != nil {
		data, _ = json.Marshal(answer{ID: a.ID, Error: fail("internal", "the answer could not be encoded: %v", err)})
	}
	return data
}

// failure is an error answer: the driver's codes, or the remote's.
type failure struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Members map[string]any `json:"-"`
}

func (f *failure) Error() string { return f.Code + ": " + f.Message }

func (f *failure) MarshalJSON() ([]byte, error) {
	object := map[string]any{"code": f.Code, "message": f.Message}
	for key, value := range f.Members {
		object[key] = value
	}
	return json.Marshal(object)
}

func fail(code, format string, args ...any) *failure {
	return &failure{Code: code, Message: fmt.Sprintf(format, args...)}
}

func unsupported(what string) *failure { return fail("unsupported", "%s", what) }

func invalid(format string, args ...any) *failure {
	return fail("invalid", format, args...)
}

// testee holds every object the runner made, by handle, and the ops.
type testee struct {
	mu      sync.Mutex
	next    int
	handles map[string]any
	table   map[string]func(request) (any, error)
	bye     bool
}

func newTestee() *testee {
	t := &testee{handles: map[string]any{}}
	t.table = t.ops()
	return t
}

func (t *testee) mint(prefix string, object any) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.next++
	handle := fmt.Sprintf("%s%d", prefix, t.next)
	t.handles[handle] = object
	return handle
}

func (t *testee) lookup(handle string) (any, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	object, ok := t.handles[handle]
	return object, ok
}

// object is what the request's on names, as the kind its op takes: an
// unknown handle is unknown_handle, and one of another kind is invalid.
func object[T any](t *testee, r request, kind string) (T, error) {
	var zero T
	handle, err := r.mustString("on")
	if err != nil {
		return zero, err
	}
	o, ok := t.lookup(handle)
	if !ok {
		return zero, fail("unknown_handle", "%s", handle)
	}
	typed, ok := o.(T)
	if !ok {
		return zero, invalid("%s is not a %s", handle, kind)
	}
	return typed, nil
}

// closer is what a handle's object does when the testee resets.
type closer interface{ shutdown() }

// reset forgets every handle and shuts every object down, all at once, so
// that no close handshake waits on another.
func (t *testee) reset() {
	t.mu.Lock()
	objects := t.handles
	t.handles = map[string]any{}
	t.mu.Unlock()
	var shut sync.WaitGroup
	for _, o := range objects {
		if c, ok := o.(closer); ok {
			shut.Go(c.shutdown)
		}
	}
	shut.Wait()
}

func (t *testee) serve(line []byte) answer {
	var raw map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		return answer{Error: invalid("not a request: %v", err)}
	}
	r := request{args: raw}
	if err := json.Unmarshal(raw["id"], &r.id); err != nil {
		return answer{Error: invalid("a request carries an integer id")}
	}
	if err := json.Unmarshal(raw["op"], &r.op); err != nil || r.op == "" {
		return answer{ID: r.id, Error: invalid("a request names its op")}
	}
	delete(raw, "id")
	delete(raw, "op")
	ok, err := t.dispatch(r)
	if err != nil {
		f, is := err.(*failure)
		if !is {
			f = fail("internal", "%v", err)
		}
		return answer{ID: r.id, Error: f}
	}
	if ok == nil {
		ok = map[string]any{}
	}
	return answer{ID: r.id, OK: ok}
}

func (t *testee) dispatch(r request) (any, error) {
	switch r.op {
	case "hello":
		return map[string]any{
			"driver":   driverVersion,
			"language": "go",
			"layers":   []string{"seam", "peer"},
			"features": []string{"listen", "pipe", "lazy", "propagator"},
		}, nil
	case "reset":
		t.reset()
		return nil, nil
	case "bye":
		t.bye = true
		return nil, nil
	}
	if handler, ok := t.table[r.op]; ok {
		return handler(r)
	}
	return nil, unsupported("no such op: " + r.op)
}

// The argument helpers: absent is the zero value; present and wrong is
// invalid.

func (r request) string(name string) (string, error) {
	raw, ok := r.args[name]
	if !ok {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", invalid("%s is a string", name)
	}
	return s, nil
}

func (r request) mustString(name string) (string, error) {
	s, err := r.string(name)
	if err == nil && s == "" {
		return "", invalid("%s is required", name)
	}
	return s, err
}

func (r request) int(name string, fallback int64) (int64, error) {
	raw, ok := r.args[name]
	if !ok {
		return fallback, nil
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil {
		return 0, invalid("%s is an integer", name)
	}
	return n, nil
}

func (r request) within() (time.Duration, error) {
	ms, err := r.int("within_ms", 5000)
	return time.Duration(ms) * time.Millisecond, err
}

// raw is a payload as the runner wrote it, handed on without decoding, or
// nil where the request has none.
func (r request) raw(name string) json.RawMessage {
	raw, ok := r.args[name]
	if !ok {
		return nil
	}
	return raw
}

func (r request) object(name string) (map[string]json.RawMessage, error) {
	raw, ok := r.args[name]
	if !ok {
		return nil, nil
	}
	var o map[string]json.RawMessage
	if err := json.Unmarshal(raw, &o); err != nil {
		return nil, invalid("%s is an object", name)
	}
	return o, nil
}

// payload is a JSON value as it travels in an answer: its bytes as they
// arrived, and null where there were none.
func payload(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("null")
	}
	return raw
}

// inbox holds what arrived unasked, in order, for await.
type inbox[T any] struct {
	mu    sync.Mutex
	cond  *sync.Cond
	items []T
	done  bool
}

func newInbox[T any]() *inbox[T] {
	b := &inbox[T]{}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *inbox[T]) put(item T) {
	b.mu.Lock()
	b.items = append(b.items, item)
	b.mu.Unlock()
	b.cond.Broadcast()
}

// close says nothing more arrives; an await then answers at once.
func (b *inbox[T]) close() {
	b.mu.Lock()
	b.done = true
	b.mu.Unlock()
	b.cond.Broadcast()
}

// await returns and removes the first item accept takes, waiting up to
// within for one; false when none came in time, or none will.
func (b *inbox[T]) await(within time.Duration, accept func(T) bool) (T, bool, bool) {
	deadline := time.Now().Add(within)
	// The deadline wakes the waiter under the lock, so that it cannot pass
	// between the waiter's look at the clock and its wait.
	timer := time.AfterFunc(within, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.cond.Broadcast()
	})
	defer timer.Stop()
	b.mu.Lock()
	defer b.mu.Unlock()
	for {
		for i, item := range b.items {
			if accept(item) {
				b.items = append(b.items[:i], b.items[i+1:]...)
				return item, true, false
			}
		}
		if b.done {
			var zero T
			return zero, false, true
		}
		if !time.Now().Before(deadline) {
			var zero T
			return zero, false, false
		}
		b.cond.Wait()
	}
}
