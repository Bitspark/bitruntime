// The Go testee: Nightseam's Go runtime and tunnel under the
// control of the conformance runner, over the protocol of
// conformance/DRIVER.md. It is the reference implementation the suite holds
// every other language to, and a worked example of what a testee is: a
// loop reading one request per line, a table of handles, an inbox per
// handle for what arrived unasked, and nothing on stdout but answers.
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
			answer := t.serve(line)
			data, _ := json.Marshal(answer)
			out.Write(data)
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
	ID    int   `json:"id"`
	OK    any   `json:"ok,omitempty"`
	Error error `json:"error,omitempty"`
}

// failure is an error answer: the protocol's codes, or the remote's.
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
	bye     bool
}

func newTestee() *testee { return &testee{handles: map[string]any{}} }

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

// closer is what a handle's object does when the testee resets.
type closer interface{ shutdown() }

func (t *testee) reset() {
	t.mu.Lock()
	objects := t.handles
	t.handles = map[string]any{}
	t.mu.Unlock()
	for _, object := range objects {
		if c, ok := object.(closer); ok {
			c.shutdown()
		}
	}
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
		var f *failure
		if e, is := err.(*failure); is {
			f = e
		} else {
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
			"layers":   []string{"seam", "peer", "tunnel", "live"},
			"features": []string{"listen", "pipe", "observer", "propagator", "lazy"},
		}, nil
	case "reset":
		t.reset()
		return nil, nil
	case "bye":
		t.bye = true
		return nil, nil
	}
	if handler, ok := t.ops()[r.op]; ok {
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

func (r request) bool(name string) (bool, error) {
	raw, ok := r.args[name]
	if !ok {
		return false, nil
	}
	var b bool
	if err := json.Unmarshal(raw, &b); err != nil {
		return false, invalid("%s is a boolean", name)
	}
	return b, nil
}

func (r request) within() (time.Duration, error) {
	ms, err := r.int("within_ms", 5000)
	return time.Duration(ms) * time.Millisecond, err
}

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
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, invalid("%s is an object", name)
	}
	return object, nil
}

// inbox holds what arrived unasked, in order, for await and drain.
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
	timer := time.AfterFunc(within, func() { b.cond.Broadcast() })
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

// drain returns everything held, and empties the inbox when asked.
func (b *inbox[T]) drain(empty bool) []T {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := append([]T(nil), b.items...)
	if empty {
		b.items = nil
	}
	return out
}
