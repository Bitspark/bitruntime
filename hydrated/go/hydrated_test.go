package hydrated

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	websocket "github.com/Bitspark/bitruntime/websocket/go"
	ontos "github.com/Bitspark/bitwire/ontos/go/core"
	wire "github.com/Bitspark/bitwire/wire/go"
)

func text(s string) ontos.Atom { return ontos.NewAtom([]byte(s)) }

// pending reads an endpoint's queue length under its lock.
func (e *Endpoint) pending() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.queue)
}

func tuple(t *testing.T, items ...Value) Value {
	t.Helper()
	v, err := NewTuple(items...)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func items(t *testing.T, v Value) []Value {
	t.Helper()
	xs, ok := Items(v)
	if !ok {
		t.Fatalf("not a tuple: %#v", v)
	}
	return xs
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// --- vectors (bitwire#83 revision 2, pinned by hash) ---

type jsonValue map[string]json.RawMessage

func ground(v jsonValue) ontos.Value {
	if raw, ok := v["atom"]; ok {
		var h string
		_ = json.Unmarshal(raw, &h)
		b, _ := hex.DecodeString(h)
		return ontos.NewAtom(b)
	}
	var xs []jsonValue
	_ = json.Unmarshal(v["tuple"], &xs)
	out := make([]ontos.Value, len(xs))
	for i, x := range xs {
		out[i] = ground(x)
	}
	return ontos.NewTuple(out...)
}

func wires(v Value) int {
	switch x := v.(type) {
	case Wire:
		return 1
	case Tuple:
		n := 0
		for _, item := range x.items {
			n += wires(item)
		}
		return n
	}
	return 0
}

func TestObservation1VectorsRoundTrip(t *testing.T) {
	raw, err := os.ReadFile("testdata/hydrated-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	if sum := sha256.Sum256(raw); hex.EncodeToString(sum[:]) != "ce6b9f0f45db9c64627bbc1ba007344a8b4dde44297cc771431a7bc122546e6f" {
		t.Fatal("vectors changed; repin to the reviewed bitwire record")
	}
	var v struct {
		Encode []struct {
			Name, Live, Hex string
			Body            jsonValue
		}
		RejectBody, RejectFrame []struct {
			Name  string
			Value jsonValue
		}
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	// A scope at a path no vector names: every reference imports as a proxy.
	s, _ := NewScope(NewNamespace("vectors"), wire.Path{text("vectors")}, nil, DefaultLimits)
	for _, c := range v.Encode {
		body := ground(c.Body)
		live, err := s.decode(body, 0, map[string]Wire{}, &budget{})
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		if got, want := wires(live), strings.Count(c.Live, "wire{"); got != want {
			t.Fatalf("%s: %d wire leaves, want %d", c.Name, got, want)
		}
		again, err := s.encode(live, 0, map[*Endpoint]ontos.Atom{}, &budget{})
		if err != nil || !again.Equal(body) {
			t.Fatalf("%s: re-encoding changed the body: %v", c.Name, err)
		}
		b, _ := wire.EncodeMessage(again, wire.DefaultMaxMessageBytes)
		if hex.EncodeToString(b) != c.Hex {
			t.Fatalf("%s: bytes", c.Name)
		}
	}
	for _, c := range v.RejectBody {
		if _, err := s.decode(ground(c.Value), 0, map[string]Wire{}, &budget{}); !errors.Is(err, MalformedFrame) {
			t.Fatalf("%s: %v", c.Name, err)
		}
	}
	// Frame rejection with a live target at the vectors' scope S and id I1.
	s.token = ontos.NewAtom([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
	i1, _ := hex.DecodeString("101112131415161718191a1b1c1d1e1f")
	target := NewEndpoint(8)
	s.byID[string(i1)], s.byEndpoint[target] = target, ontos.NewAtom(i1)
	for _, c := range v.RejectFrame {
		if err := s.Deliver(s.path, ground(c.Value), nil); !errors.Is(err, MalformedFrame) {
			t.Fatalf("%s: %v", c.Name, err)
		}
	}
	if target.pending() != 0 {
		t.Fatal("a rejected frame was delivered")
	}
}

// --- a namespace of participants around an opaque middle router ---

var paths = map[string]wire.Path{
	"client":   {text("client42")},
	"provider": {text("a"), text("b"), text("service2")},
	"third":    {text("c")},
}

type diagnostic struct {
	who string
	err error
}

type participant struct {
	name string
	ep   wire.AddressedEndpoint
	mu   sync.Mutex
	sc   *Scope
}

func (p *participant) scope() *Scope {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sc
}

type tree struct {
	ns     *Namespace
	mu     sync.Mutex
	routes map[string]wire.AddressedWire
	trace  []ontos.Value
	diags  []diagnostic
	parts  map[string]*participant
}

func key(p wire.Path) string { return Reference{Path: p}.key() }

func (tr *tree) diagnostics() []diagnostic {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]diagnostic(nil), tr.diags...)
}

func (tr *tree) frames() int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return len(tr.trace)
}

// links returns n pairs of (participant side, middle side) endpoints.
func links(t *testing.T, carrier string, n int) [][2]wire.Endpoint {
	out := make([][2]wire.Endpoint, n)
	if carrier == "pair" {
		for i := range out {
			a, b, err := core.NewPair(core.PairOptions{})
			if err != nil {
				t.Fatal(err)
			}
			out[i] = [2]wire.Endpoint{a, b}
			t.Cleanup(func() { _ = a.Close() })
		}
		return out
	}
	accepted := make(chan *websocket.Endpoint, n)
	server, err := websocket.Listen("127.0.0.1:0", websocket.Options{}, func(e *websocket.Endpoint, _ *http.Request) { accepted <- e })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	for i := range out {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		c, err := websocket.Dial(ctx, server.URL(), websocket.Options{})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		out[i] = [2]wire.Endpoint{c, <-accepted}
	}
	return out
}

func newTree(t *testing.T, carrier string, names ...string) *tree {
	tr := &tree{ns: NewNamespace("test"), routes: map[string]wire.AddressedWire{}, parts: map[string]*participant{}}
	ls := links(t, carrier, len(names))
	for i, name := range names {
		mine, mid := core.Addressed(ls[i][0]), core.Addressed(ls[i][1])
		tr.routes[key(paths[name])] = mid
		if _, err := mid.Receive(func(p wire.Path, v ontos.Value) {
			tr.mu.Lock()
			tr.trace = append(tr.trace, v)
			dest := tr.routes[key(p)]
			tr.mu.Unlock()
			if dest != nil { // the middle knows routes and ground values only
				_ = dest.Send(p, v)
			}
		}); err != nil {
			t.Fatal(err)
		}
		sc, err := NewScope(tr.ns, paths[name], mine, DefaultLimits)
		if err != nil {
			t.Fatal(err)
		}
		p := &participant{name: name, ep: mine, sc: sc}
		tr.parts[name] = p
		if _, err := mine.Receive(func(path wire.Path, v ontos.Value) {
			// The composition's received context: here, the link it arrived on.
			if err := p.scope().Deliver(path, v, name+"-link"); err != nil {
				tr.mu.Lock()
				tr.diags = append(tr.diags, diagnostic{name, err})
				tr.mu.Unlock()
			}
		}); err != nil {
			t.Fatal(err)
		}
	}
	return tr
}

func (tr *tree) replace(t *testing.T, name string) *Scope {
	p := tr.parts[name]
	sc, err := NewScope(tr.ns, paths[name], p.ep, DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	old := p.sc
	p.sc = sc
	p.mu.Unlock()
	old.Close()
	return sc
}

// serve exposes a domain endpoint at a participant: the composition bootstrap.
func serve(t *testing.T, s *Scope, handler func(Value, Context)) (*Endpoint, Reference) {
	e := NewEndpoint(64)
	if _, err := e.Receive(handler); err != nil {
		t.Fatal(err)
	}
	ref, err := s.Expose(e)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e, ref
}

type received struct {
	value   Value
	context Context
}

// call is a domain adapter: it sends (op, args..., reply) and closes its reply
// endpoint after the reply. It knows no reference, table or route.
func call(t *testing.T, target Wire, op string, args ...Value) received {
	t.Helper()
	reply := NewEndpoint(1)
	defer reply.Close()
	got := make(chan received, 1)
	if _, err := reply.Receive(func(v Value, c Context) { got <- received{v, c} }); err != nil {
		t.Fatal(err)
	}
	if err := target.Send(tuple(t, append(append([]Value{text(op)}, args...), reply)...)); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-got:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("no reply")
	}
	return received{}
}

// echo is a domain provider: (op, arg, reply). "echo" replies (arg); "continue"
// replies (arg, continuation), where the continuation forwards one value to the
// Wire it carries and then closes.
func echo(t *testing.T) func(Value, Context) {
	return func(v Value, _ Context) {
		xs, _ := Items(v)
		op, arg, reply := xs[0].(ontos.Atom), xs[1], xs[len(xs)-1].(Wire)
		switch string(op.Bytes()) {
		case "echo":
			_ = reply.Send(arg)
		case "continue":
			cont := NewEndpoint(1)
			_, _ = cont.Receive(func(next Value, _ Context) {
				ys, _ := Items(next)
				_ = ys[1].(Wire).Send(ys[0])
				_ = cont.Close()
			})
			r, _ := NewTuple(arg, cont)
			_ = reply.Send(r)
		}
	}
}

func carriers(t *testing.T, f func(t *testing.T, carrier string)) {
	for _, c := range []string{"pair", "websocket"} {
		t.Run(c, func(t *testing.T) { f(t, c) })
	}
}

func TestObservation2RecursionThroughAnOpaqueRouter(t *testing.T) {
	carriers(t, func(t *testing.T, carrier string) {
		tr := newTree(t, carrier, "client", "provider")
		_, svc := serve(t, tr.parts["provider"].scope(), echo(t))
		target := tr.parts["client"].scope().Connect(svc)
		r := call(t, target, "continue", text("hello"))
		xs := items(t, r.value)
		if !xs[0].(ontos.Atom).Equal(text("hello")) {
			t.Fatal("wrong reply")
		}
		third := NewEndpoint(1)
		got := make(chan Value, 1)
		_, _ = third.Receive(func(v Value, _ Context) { got <- v })
		if err := xs[1].(Wire).Send(tuple(t, text("third Wire"), third)); err != nil {
			t.Fatal(err)
		}
		select {
		case v := <-got:
			if !v.(ontos.Atom).Equal(text("third Wire")) {
				t.Fatal("wrong third value")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("third Wire not reached")
		}
		if n := tr.frames(); n != 4 {
			t.Fatalf("%d frames crossed the middle, want 4", n)
		}
		if d := tr.diagnostics(); len(d) != 0 {
			t.Fatal(d)
		}
	})
}

func TestObservation3LongLivedCallsStayBounded(t *testing.T) {
	carriers(t, func(t *testing.T, carrier string) {
		tr := newTree(t, carrier, "client", "provider")
		client, provider := tr.parts["client"].scope(), tr.parts["provider"].scope()
		_, svc := serve(t, provider, echo(t))
		target := client.Connect(svc)
		for i := 0; i < 200; i++ {
			r := call(t, target, "echo", text(fmt.Sprint(i)))
			if !r.value.(ontos.Atom).Equal(text(fmt.Sprint(i))) {
				t.Fatalf("call %d: wrong reply", i)
			}
			if client.Live() > 1 || provider.Live() != 1 {
				t.Fatalf("call %d: live exports client %d provider %d", i, client.Live(), provider.Live())
			}
		}
		eventually(t, "the last reply export did not end", func() bool { return client.Live() == 0 })
		if d := tr.diagnostics(); len(d) != 0 {
			t.Fatal(d)
		}
	})
}

func TestObservation4ASharedWireEndsOnlyWithItsOwner(t *testing.T) {
	carriers(t, func(t *testing.T, carrier string) {
		tr := newTree(t, carrier, "client", "provider", "third")
		a, b, c := tr.parts["client"].scope(), tr.parts["provider"].scope(), tr.parts["third"].scope()
		var mu sync.Mutex
		var atB, atC Wire
		_, refC := serve(t, c, func(v Value, _ Context) { mu.Lock(); atC = v.(Wire); mu.Unlock() })
		toC := b.Connect(refC)
		_, refB := serve(t, b, func(v Value, _ Context) {
			mu.Lock()
			atB = v.(Wire)
			mu.Unlock()
			_ = toC.Send(v) // B forwards the Wire it received
		})
		w := NewEndpoint(8)
		var seen []Context
		_, _ = w.Receive(func(_ Value, ctx Context) { mu.Lock(); seen = append(seen, ctx); mu.Unlock() })
		if err := a.Connect(refB).Send(w); err != nil {
			t.Fatal(err)
		}
		eventually(t, "C did not receive the Wire", func() bool { mu.Lock(); defer mu.Unlock(); return atC != nil })
		if b.Live() != 1 || c.Live() != 1 {
			t.Fatalf("forwarding registered state: B %d C %d", b.Live(), c.Live())
		}
		_ = atC.Send(text("from C"))
		eventually(t, "C's send did not reach W", func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == 1 })
		_ = w.Close()
		mu.Lock()
		fromB := atB
		mu.Unlock()
		_ = fromB.Send(text("late B"))
		_ = atC.Send(text("late C"))
		eventually(t, "late sends were not refused at A", func() bool {
			n := 0
			for _, d := range tr.diagnostics() {
				if d.who == "client" && errors.Is(d.err, UnknownExport) {
					n++
				}
			}
			return n == 2
		})
		mu.Lock()
		defer mu.Unlock()
		if len(seen) != 1 {
			t.Fatal("a closed Wire received")
		}
	})
}

func TestObservation5NoProxyOfAProxy(t *testing.T) {
	tr := newTree(t, "pair", "client", "provider", "third")
	a, b, c := tr.parts["client"].scope(), tr.parts["provider"].scope(), tr.parts["third"].scope()
	w := NewEndpoint(1)
	ref, _ := a.Expose(w)
	imported := b.Connect(ref)
	for _, s := range []*Scope{b, c} { // a second local participant encodes it too
		got, err := s.encode(imported, 0, map[*Endpoint]ontos.Atom{}, &budget{})
		if err != nil || !got.Equal(ref.value()) {
			t.Fatalf("%v: reference changed", err)
		}
	}
	if b.Live() != 0 || c.Live() != 0 {
		t.Fatal("a proxy was re-exported")
	}
}

type funcWire func(Value) error

func (f funcWire) Send(v Value) error { return f(v) }

func TestObservation6RefusalsBeforeAdmission(t *testing.T) {
	tr := newTree(t, "pair", "client", "provider")
	a, b := tr.parts["client"].scope(), tr.parts["provider"].scope()
	_, svc := serve(t, b, func(Value, Context) {})
	target := a.Connect(svc)
	kept := NewEndpoint(1)
	if err := target.Send(tuple(t, kept, funcWire(func(Value) error { return nil }))); !errors.Is(err, Unexportable) {
		t.Fatalf("arbitrary Wire: %v", err)
	}
	other, _ := NewScope(NewNamespace("other"), wire.Path{text("x")}, nil, DefaultLimits)
	foreign := other.Connect(Reference{wire.Path{text("y")}, ontos.NewAtom(make([]byte, 16)), text("1")})
	if err := target.Send(tuple(t, kept, foreign)); !errors.Is(err, ForeignNamespace) {
		t.Fatalf("foreign proxy: %v", err)
	}
	closed := NewEndpoint(1)
	_ = closed.Close()
	if err := target.Send(closed); !errors.Is(err, Unexportable) {
		t.Fatalf("closed endpoint: %v", err)
	}
	if a.Live() != 0 || tr.frames() != 0 {
		t.Fatalf("a refused send left state: %d exports, %d frames", a.Live(), tr.frames())
	}
}

func TestObservation7StaleScopeCannotReachTheReplacement(t *testing.T) {
	carriers(t, func(t *testing.T, carrier string) {
		tr := newTree(t, carrier, "client", "provider")
		_, svc := serve(t, tr.parts["provider"].scope(), func(Value, Context) {})
		old := tr.parts["client"].scope().Connect(svc)
		fresh := tr.replace(t, "provider")
		var mu sync.Mutex
		hit := false
		_, _ = serve(t, fresh, func(Value, Context) { mu.Lock(); hit = true; mu.Unlock() })
		_ = old.Send(text("stale"))
		eventually(t, "not refused stale-scope", func() bool {
			d := tr.diagnostics()
			return len(d) == 1 && d[0].who == "provider" && errors.Is(d[0].err, StaleScope)
		})
		mu.Lock()
		defer mu.Unlock()
		if hit {
			t.Fatal("the replacement received a stale frame")
		}
	})
}

func TestObservation9ReturningHome(t *testing.T) {
	tr := newTree(t, "pair", "client", "provider")
	a, b := tr.parts["client"].scope(), tr.parts["provider"].scope()
	var back Wire
	var mu sync.Mutex
	proceed := make(chan struct{})
	_, svc := serve(t, b, func(v Value, _ Context) {
		<-proceed
		xs, _ := Items(v)
		r, _ := NewTuple(text("data"), xs[0], xs[1]) // send both Wires home
		_ = xs[2].(Wire).Send(r)
	})
	home, gone := NewEndpoint(1), NewEndpoint(1)
	reply := NewEndpoint(1)
	got := make(chan Value, 1)
	_, _ = reply.Receive(func(v Value, _ Context) { got <- v })
	if err := a.Connect(svc).Send(tuple(t, home, gone, reply)); err != nil {
		t.Fatal(err)
	}
	// gone closes before its reference comes back.
	eventually(t, "export not registered", func() bool { return a.Live() == 3 })
	_ = gone.Close()
	close(proceed)
	select {
	case v := <-got:
		xs := items(t, v)
		mu.Lock()
		back = xs[1].(Wire)
		mu.Unlock()
		if !xs[0].(ontos.Atom).Equal(text("data")) {
			t.Fatal("the rest of the message was lost")
		}
		if back != home.Wire() {
			t.Fatal("own reference did not return to the original face")
		}
		if err := xs[2].(Wire).Send(text("x")); !errors.Is(err, UnknownExport) {
			t.Fatalf("a withdrawn own reference: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no reply")
	}
}

func TestObservation10ReceivedContextIsNotData(t *testing.T) {
	tr := newTree(t, "pair", "client", "provider")
	got := make(chan received, 1)
	_, svc := serve(t, tr.parts["provider"].scope(), func(v Value, c Context) { got <- received{v, c} })
	forged := ontos.NewTuple(text("context"), text("client-link"))
	if err := tr.parts["client"].scope().Connect(svc).Send(forged); err != nil {
		t.Fatal(err)
	}
	r := <-got
	if r.context != "provider-link" || !forged.Equal(r.value.(ontos.Value)) {
		t.Fatalf("context %v, value %v", r.context, r.value)
	}
}

func TestObservation11AtomicDecoding(t *testing.T) {
	tr := newTree(t, "pair", "client", "provider")
	b := tr.parts["provider"].scope()
	var mu sync.Mutex
	delivered := 0
	_, svc := serve(t, b, func(Value, Context) { mu.Lock(); delivered++; mu.Unlock() })
	w := NewEndpoint(1)
	ref, _ := tr.parts["client"].scope().Expose(w)
	bad := ontos.NewTuple(tupleTag, ontos.NewTuple(ref.value(), ontos.NewTuple(tupleTag)))
	if err := b.Deliver(svc.Path, ontos.NewTuple(header, svc.Scope, svc.ID, bad), nil); !errors.Is(err, MalformedFrame) {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if delivered != 0 {
		t.Fatal("a malformed frame was delivered")
	}
}

type refusingSender struct{}

func (refusingSender) Send(wire.Path, ontos.Value) error { return errors.New("carrier refused") }

func TestObservation12RegistrationBeforeAdmission(t *testing.T) {
	s, _ := NewScope(NewNamespace("x"), wire.Path{text("a")}, refusingSender{}, DefaultLimits)
	target := s.Connect(Reference{wire.Path{text("b")}, ontos.NewAtom(make([]byte, 16)), text("1")})
	w := NewEndpoint(1)
	if err := target.Send(tuple(t, w)); err == nil {
		t.Fatal("the carrier's refusal was hidden")
	}
	if s.Live() != 1 {
		t.Fatal("a refused admission withdrew or never registered the export")
	}
	if err := target.Send(tuple(t, w, w)); err == nil || s.Live() != 1 {
		t.Fatal("a second attempt added an export for the same face")
	}
	_ = w.Close()
	if s.Live() != 0 {
		t.Fatal("closing the endpoint did not end its export")
	}
}

func TestObservation14Ordering(t *testing.T) {
	carriers(t, func(t *testing.T, carrier string) {
		tr := newTree(t, carrier, "client", "provider")
		var mu sync.Mutex
		var seen []string
		_, svc := serve(t, tr.parts["provider"].scope(), func(v Value, _ Context) {
			mu.Lock()
			seen = append(seen, string(v.(ontos.Atom).Bytes()))
			mu.Unlock()
		})
		target := tr.parts["client"].scope().Connect(svc)
		for i := 0; i < 50; i++ {
			if err := target.Send(text(fmt.Sprint(i))); err != nil {
				t.Fatal(err)
			}
		}
		eventually(t, "not all delivered", func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == 50 })
		for i, s := range seen {
			if s != fmt.Sprint(i) {
				t.Fatalf("position %d holds %s", i, s)
			}
		}
	})
}

func TestObservation15Bounds(t *testing.T) {
	s, _ := NewScope(NewNamespace("x"), wire.Path{text("a")}, refusingSender{}, Limits{Exports: 2, Nodes: 8, Depth: 3, Bytes: 64})
	target := s.Connect(Reference{wire.Path{text("b")}, ontos.NewAtom(make([]byte, 16)), text("1")})
	deep := Value(text("x"))
	for i := 0; i < 5; i++ {
		deep = tuple(t, deep)
	}
	wide := make([]Value, 9)
	for i := range wide {
		wide[i] = text("y")
	}
	for name, v := range map[string]Value{
		"depth":   deep,
		"nodes":   tuple(t, wide...),
		"bytes":   ontos.NewAtom(make([]byte, 65)),
		"exports": tuple(t, NewEndpoint(1), NewEndpoint(1), NewEndpoint(1)),
	} {
		if err := target.Send(v); !errors.Is(err, Limit) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if s.Live() != 0 {
		t.Fatal("a refused send registered exports")
	}
	// A target that does not admit a delivery: queue of one, no receiver.
	tr := newTree(t, "pair", "client", "provider")
	full := NewEndpoint(1)
	svc, _ := tr.parts["provider"].scope().Expose(full)
	to := tr.parts["client"].scope().Connect(svc)
	_ = to.Send(text("1"))
	_ = to.Send(text("2"))
	eventually(t, "not refused target-refused", func() bool {
		d := tr.diagnostics()
		return len(d) == 1 && errors.Is(d[0].err, TargetRefused)
	})
}

// Observation 8: one reference grants its own Wire and no sibling, including an
// endpoint the owner exported for another holder.
func TestObservation8SiblingForgery(t *testing.T) {
	carriers(t, func(t *testing.T, carrier string) {
		tr := newTree(t, carrier, "client", "provider", "third")
		provider := tr.parts["provider"].scope()
		var mu sync.Mutex
		private := 0
		_, public := serve(t, provider, func(Value, Context) {})
		_, intended := serve(t, provider, func(Value, Context) { mu.Lock(); private++; mu.Unlock() })
		// The private endpoint really is held by another participant.
		if err := tr.parts["third"].scope().Connect(intended).Send(text("legitimate")); err != nil {
			t.Fatal(err)
		}
		eventually(t, "the intended holder was not served", func() bool { mu.Lock(); defer mu.Unlock(); return private == 1 })
		if public.ID.Len() != 16 || public.ID.Equal(intended.ID) {
			t.Fatalf("export ids %x and %x", public.ID.Bytes(), intended.ID.Bytes())
		}
		// The client holds only the public reference, and so knows the scope token.
		client := tr.parts["client"].scope()
		bump := append([]byte(nil), public.ID.Bytes()...)
		bump[15]++
		forged := []ontos.Atom{ontos.NewAtom(bump), ontos.NewAtom(make([]byte, 16)), text("2"), text("0000000000000002")}
		for _, id := range forged {
			_ = client.Connect(Reference{public.Path, public.Scope, id}).Send(text("forged"))
		}
		eventually(t, "forged references were not refused", func() bool { return len(tr.diagnostics()) == len(forged) })
		for _, d := range tr.diagnostics() {
			if d.who != "provider" || !(errors.Is(d.err, UnknownExport) || errors.Is(d.err, MalformedFrame)) {
				t.Fatalf("forged reference: %+v", d)
			}
		}
		mu.Lock()
		defer mu.Unlock()
		if private != 1 {
			t.Fatalf("the private endpoint received %d deliveries, want only the legitimate one", private)
		}
	})
}

type recordingSender struct {
	mu     sync.Mutex
	frames []ontos.Value
}

func (r *recordingSender) Send(_ wire.Path, v ontos.Value) error {
	r.mu.Lock()
	r.frames = append(r.frames, v)
	r.mu.Unlock()
	return nil
}

// Observation 13: concurrent sends agree on one export per face, and never pass
// the export bound.
func TestObservation13ConcurrentSends(t *testing.T) {
	run := func(n int, f func(i int)) {
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); <-start; f(i) }()
		}
		close(start)
		wg.Wait()
	}
	dest := Reference{wire.Path{text("b")}, ontos.NewAtom(make([]byte, 16)), ontos.NewAtom(make([]byte, 16))}
	rec := &recordingSender{}
	s, _ := NewScope(NewNamespace("x"), wire.Path{text("a")}, rec, DefaultLimits)
	target := s.Connect(dest)
	face := NewEndpoint(1)
	run(32, func(int) {
		if err := target.Send(tuple(t, face)); err != nil {
			t.Error(err)
		}
	})
	if s.Live() != 1 || len(rec.frames) != 32 {
		t.Fatalf("one face: %d exports, %d frames", s.Live(), len(rec.frames))
	}
	registered := s.byEndpoint[face]
	for _, f := range rec.frames {
		leaf := f.(ontos.Tuple).At(3).(ontos.Tuple).At(1).(ontos.Tuple).At(0) // body (01, ((02, ref)))
		ref, err := readReference(leaf.(ontos.Tuple).At(1), DefaultLimits.Depth)
		if err != nil || !ref.ID.Equal(registered) {
			t.Fatal("a frame names an id that was not registered")
		}
	}

	bounded, _ := NewScope(NewNamespace("x"), wire.Path{text("a")}, &recordingSender{}, Limits{Exports: 4, Nodes: 64, Depth: 8, Bytes: 1 << 16})
	to := bounded.Connect(dest)
	var mu sync.Mutex
	refused := 0
	run(8, func(int) {
		if err := to.Send(tuple(t, NewEndpoint(1))); errors.Is(err, Limit) {
			mu.Lock()
			refused++
			mu.Unlock()
		} else if err != nil {
			t.Error(err)
		}
	})
	if bounded.Live() != 4 || refused != 4 {
		t.Fatalf("bound 4: %d live, %d refused", bounded.Live(), refused)
	}
}

// Observation 15, incoming: a frame counts whole against the byte bound,
// reference material included, and nothing is delivered.
func TestObservation15IncomingFrameBytes(t *testing.T) {
	s, _ := NewScope(NewNamespace("x"), wire.Path{text("a")}, nil, Limits{Exports: 4, Nodes: 64, Depth: 8, Bytes: 128})
	target := NewEndpoint(4)
	ref, _ := s.Expose(target)
	huge := Reference{wire.Path{ontos.NewAtom(make([]byte, 4096))}, ontos.NewAtom(make([]byte, 16)), ontos.NewAtom(make([]byte, 16))}
	body := ontos.NewTuple(tupleTag, ontos.NewTuple(huge.value()))
	if err := s.Deliver(s.path, ontos.NewTuple(header, ref.Scope, ref.ID, body), nil); !errors.Is(err, Limit) {
		t.Fatalf("an oversized reference: %v", err)
	}
	if target.pending() != 0 {
		t.Fatal("an oversized frame was delivered")
	}
}

// Observation 17: the Endpoint laws hold on every path. A local send conveys only
// the sending face; a stale detach removes only its own receiver; a receiver's
// failure terminates its endpoint, which ends its export.
func TestObservation17EndpointLaws(t *testing.T) {
	got := make(chan Value, 2)
	local := NewEndpoint(4)
	if _, err := local.Receive(func(v Value, _ Context) { got <- v }); err != nil {
		t.Fatal(err)
	}
	private := NewEndpoint(1)
	_ = local.Wire().Send(private)
	_ = local.Wire().Send(tuple(t, private))
	if v := <-got; v != private.Wire() {
		t.Fatalf("a bare endpoint arrived as %T", v)
	}
	if v := <-got; items(t, v)[0] != private.Wire() {
		t.Fatal("an endpoint inside a tuple kept its receive and close authority")
	}

	e := NewEndpoint(4)
	stale, _ := e.Receive(func(Value, Context) {})
	stale()
	seen := make(chan Value, 1)
	if _, err := e.Receive(func(v Value, _ Context) { seen <- v }); err != nil {
		t.Fatal(err)
	}
	stale()
	_ = e.Send(text("after"))
	select {
	case <-seen:
	case <-time.After(2 * time.Second):
		t.Fatal("a stale detach removed the newer receiver")
	}

	s, _ := NewScope(NewNamespace("x"), wire.Path{text("a")}, nil, DefaultLimits)
	failing := NewEndpoint(4)
	if _, err := failing.Receive(func(Value, Context) { panic("receiver fault") }); err != nil {
		t.Fatal(err)
	}
	ref, _ := s.Expose(failing)
	if err := s.Deliver(s.path, ontos.NewTuple(header, ref.Scope, ref.ID, text("x")), nil); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the failing receiver did not terminate its endpoint", func() bool {
		return failing.Failure() != nil && s.Live() == 0
	})
	if err := s.Deliver(s.path, ontos.NewTuple(header, ref.Scope, ref.ID, text("y")), nil); !errors.Is(err, UnknownExport) {
		t.Fatalf("after termination: %v", err)
	}
}
