package dispatch_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// node is an own Wire that records what reaches it. A request is answered
// with the node's name unless the node holds requests or refuses.
type node struct {
	name    string
	hold    bool
	refusal error
	mu      sync.Mutex
	seen    []string
	arrived chan string
}

func newNode(name string) *node { return &node{name: name, arrived: make(chan string, 64)} }

func (n *node) Send(message wire.Message) error {
	entry := n.name + ":" + string(message.Frame.Kind)
	n.mu.Lock()
	n.seen = append(n.seen, entry)
	n.mu.Unlock()
	n.arrived <- entry
	if n.refusal != nil {
		return n.refusal
	}
	if message.Frame.Kind == wire.ProfileRequest && !n.hold {
		return core.Respond(message, json.RawMessage(fmt.Sprintf("%q", n.name)), nil)
	}
	return nil
}

func (n *node) observed() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.seen...)
}

func leaf(t *testing.T, own wire.Wire, children ...wire.Child[wire.Wire]) wire.WireTree {
	t.Helper()
	tree, err := core.Compose(own, children)
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

func localDispatcher(t *testing.T) (wire.Endpoint, *dispatch.Dispatcher) {
	t.Helper()
	near, far, err := core.NewPair(core.PairOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = near.Close(transports.CodeNormal, "") })
	d, err := dispatch.NewDispatcher(far)
	if err != nil {
		t.Fatal(err)
	}
	return near, d
}

func callName(access wire.AddressedWire, path ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var name string
	err := dispatch.Call(ctx, access, path, nil, &name)
	return name, err
}

func publicCode(err error) string {
	var public *core.PublicError
	if errors.As(err, &public) {
		return public.Code
	}
	return fmt.Sprint(err)
}

func answering(name string) wire.Receiver {
	return wire.Receiver{Message: func(_ []string, message wire.Message) {
		if message.Frame.Kind == wire.ProfileRequest {
			_ = core.Respond(message, json.RawMessage(fmt.Sprintf("%q", name)), nil)
		}
	}}
}

// A delivery racing a swap is routed by one set or the other. Swapping by
// detaching and then registering leaves a moment in which the path has no route
// and a request is refused method_not_found; this test finds that moment within
// its budget (checked by substituting the two-step swap, bitruntime#15).
func TestRouteSetSwapsAtomically(t *testing.T) {
	near, d := localDispatcher(t)
	routes := d.RouteSet()
	if err := routes.Set([]dispatch.Route{{Path: []string{"x"}, Receiver: answering("a")}}); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	swapped := make(chan int)
	go func() {
		count := 0
		for {
			select {
			case <-stop:
				swapped <- count
				return
			default:
			}
			name := []string{"a", "b"}[count%2]
			if err := routes.Set([]dispatch.Route{{Path: []string{"x"}, Receiver: answering(name)}}); err != nil {
				t.Error(err)
			}
			count++
		}
	}()
	var failures sync.Map
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			for range 300 {
				if name, err := callName(near, "x"); err != nil || (name != "a" && name != "b") {
					failures.Store(fmt.Sprintf("%q %v", name, publicCode(err)), true)
				}
			}
		}()
	}
	group.Wait()
	close(stop)
	if count := <-swapped; count < 100 {
		t.Fatalf("only %d swaps raced the calls", count)
	}
	failures.Range(func(key, _ any) bool {
		t.Errorf("a call during a swap failed: %s", key)
		return true
	})
}

func TestRouteSetRefusesWithoutChanging(t *testing.T) {
	near, d := localDispatcher(t)
	if _, err := d.Register([]string{"y"}, answering("outside")); err != nil {
		t.Fatal(err)
	}
	routes := d.RouteSet()
	if err := routes.Set([]dispatch.Route{{Path: []string{"x"}, Receiver: answering("x1")}}); err != nil {
		t.Fatal(err)
	}
	for _, refused := range []struct {
		routes []dispatch.Route
		want   error
	}{
		{[]dispatch.Route{{Path: []string{"x"}, Receiver: answering("x2")}, {Path: []string{"y"}, Receiver: answering("mine")}}, core.ErrReceiverExists},
		{[]dispatch.Route{{Path: []string{"z"}, Receiver: answering("z")}, {Path: []string{"z"}, Receiver: answering("z")}}, core.ErrReceiverExists},
		{[]dispatch.Route{{Path: []string{"x"}, Receiver: answering("x2")}, {Path: []string{"\xff"}, Receiver: answering("bad")}}, core.ErrInvalidPath},
	} {
		if err := routes.Set(refused.routes); !errors.Is(err, refused.want) {
			t.Fatalf("Set = %v, want %v", err, refused.want)
		}
		if name, err := callName(near, "x"); err != nil || name != "x1" {
			t.Fatalf("a refused Set changed the group: %q %v", name, err)
		}
		if name, err := callName(near, "y"); err != nil || name != "outside" {
			t.Fatalf("a refused Set changed a route outside the group: %q %v", name, err)
		}
	}
	// An exact and a prefix route at one path are different routes.
	if err := routes.Set([]dispatch.Route{{Path: []string{"p"}, Receiver: answering("exact")}, {Path: []string{"p"}, Prefix: true, Receiver: answering("prefix")}}); err != nil {
		t.Fatal(err)
	}
	if name, _ := callName(near, "p", "deeper"); name != "prefix" {
		t.Fatalf("the prefix route answered %q", name)
	}
	if _, err := callName(near, "x"); publicCode(err) != "method_not_found" {
		t.Fatalf("a route the new set omits still answers: %v", err)
	}
	routes.Close()
	if _, err := callName(near, "p"); publicCode(err) != "method_not_found" {
		t.Fatalf("a closed set still routes: %v", err)
	}
	if err := routes.Set(nil); !errors.Is(err, transports.ErrClosed) {
		t.Fatalf("Set after Close = %v", err)
	}
	if name, err := callName(near, "y"); err != nil || name != "outside" {
		t.Fatalf("closing a set removed a route outside it: %q %v", name, err)
	}
	other := d.RouteSet()
	_ = d.Close(transports.CodeNormal, "")
	if err := other.Set(nil); !errors.Is(err, transports.ErrClosed) {
		t.Fatalf("Set on a closed dispatcher = %v", err)
	}
}

// A request admitted before a swap is cancelled at the route that admitted it.
func TestRouteSetKeepsCapturedCancellation(t *testing.T) {
	near, d := localDispatcher(t)
	old, replacement := newNode("old"), newNode("new")
	old.hold = true
	routes := d.RouteSet()
	own := func(n *node) wire.Receiver {
		return wire.Receiver{Message: func(_ []string, message wire.Message) { _ = n.Send(message) }}
	}
	if err := routes.Set([]dispatch.Route{{Path: []string{"w"}, Receiver: own(old)}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- dispatch.Call(ctx, near, []string{"w"}, nil, nil) }()
	if got := receive(t, old.arrived); got != "old:request" {
		t.Fatalf("first arrival %q", got)
	}
	if err := routes.Set([]dispatch.Route{{Path: []string{"w"}, Receiver: own(replacement)}}); err != nil {
		t.Fatal(err)
	}
	cancel()
	if got := receive(t, old.arrived); got != "old:cancel" {
		t.Fatalf("the cancellation reached %q", got)
	}
	<-done
	if seen := replacement.observed(); len(seen) != 0 {
		t.Fatalf("the replacement saw %v", seen)
	}
}

func TestServeRoutesEachUTF8Position(t *testing.T) {
	near, d := localDispatcher(t)
	root, a, empty, unicode, binary, underBinary, shared := newNode("root"), newNode("a"), newNode("empty"), newNode("unicode"), newNode("binary"), newNode("under-binary"), newNode("shared")
	sharedTree := leaf(t, shared)
	tree := leaf(t, root,
		wire.Child[wire.Wire]{Key: []byte("a"), Tree: leaf(t, a,
			wire.Child[wire.Wire]{Key: []byte(""), Tree: leaf(t, empty)},
			wire.Child[wire.Wire]{Key: []byte("s"), Tree: sharedTree},
		)},
		wire.Child[wire.Wire]{Key: []byte("é/x"), Tree: leaf(t, unicode)},
		wire.Child[wire.Wire]{Key: []byte{0xff}, Tree: leaf(t, binary,
			wire.Child[wire.Wire]{Key: []byte("c"), Tree: leaf(t, underBinary)},
		)},
		wire.Child[wire.Wire]{Key: []byte("s"), Tree: sharedTree},
	)
	served, err := dispatch.Serve(d, []string{"t"}, tree)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		path []string
		want string
	}{
		{[]string{"t"}, "root"},
		{[]string{"t", "a"}, "a"},
		{[]string{"t", "a", ""}, "empty"},
		{[]string{"t", "é/x"}, "unicode"},
		{[]string{"t", "s"}, "shared"},
		{[]string{"t", "a", "s"}, "shared"},
	} {
		if name, err := callName(near, c.path...); err != nil || name != c.want {
			t.Fatalf("%q answered %q %v, want %q", c.path, name, err, c.want)
		}
	}
	// Nothing beneath a binary key is named, even where deeper keys are UTF-8.
	for _, path := range [][]string{{"t", "c"}, {"t", "é/x"}, {"t", "a", "missing"}} {
		if _, err := callName(near, path...); publicCode(err) != "method_not_found" {
			t.Fatalf("%q: %v", path, err)
		}
	}
	if len(binary.observed())+len(underBinary.observed()) != 0 {
		t.Fatal("a node beneath a binary key was reached")
	}
	if got, want := served.Unreachable(), []wire.TreePath{{{0xff}}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Unreachable = %v, want %v", got, want)
	}
}

// Refusals over a real WebSocket: a refused request is answered and a refused
// event is dropped, and neither ends the carrier.
func TestServeRefusalsKeepTheCarrier(t *testing.T) {
	client, server := newPair(t, engine.Options{}, engine.Options{})
	d, err := dispatch.NewDispatcher(server.Wire())
	if err != nil {
		t.Fatal(err)
	}
	plain, public, fine := newNode("plain"), newNode("public"), newNode("fine")
	plain.refusal = errors.New("not public")
	public.refusal = &core.PublicError{Code: "bad_request", Message: "refused on purpose"}
	tree := leaf(t, fine,
		wire.Child[wire.Wire]{Key: []byte("plain"), Tree: leaf(t, plain)},
		wire.Child[wire.Wire]{Key: []byte("public"), Tree: leaf(t, public)},
	)
	if _, err := dispatch.Serve(d, []string{"t"}, tree); err != nil {
		t.Fatal(err)
	}
	if _, err := callName(client.Wire(), "t", "plain"); publicCode(err) != "internal" {
		t.Fatalf("a non-public refusal: %v", err)
	}
	if _, err := callName(client.Wire(), "t", "public"); publicCode(err) != "bad_request" {
		t.Fatalf("a public refusal: %v", err)
	}
	if err := dispatch.Emit(context.Background(), client.Wire(), []string{"t", "plain"}, 1); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, plain.arrived); got != "plain:request" {
		t.Fatalf("first arrival %q", got)
	}
	if got := receive(t, plain.arrived); got != "plain:event" {
		t.Fatalf("second arrival %q", got)
	}
	if name, err := callName(client.Wire(), "t"); err != nil || name != "fine" {
		t.Fatalf("the carrier did not survive a refused event: %q %v", name, err)
	}
	if client.Err() != nil || server.Err() != nil {
		t.Fatalf("a refusal ended a peer: %v %v", client.Err(), server.Err())
	}
}

func TestServeRefusesWhatItCannotServe(t *testing.T) {
	near, d := localDispatcher(t)
	tree := leaf(t, newNode("root"))
	if _, err := dispatch.Serve(d, nil, tree); !errors.Is(err, core.ErrInvalidPath) {
		t.Fatalf("an empty prefix: %v", err)
	}
	if _, err := dispatch.Serve(d, []string{"t"}, nil); !errors.Is(err, core.ErrInvalidTree) {
		t.Fatalf("a nil tree: %v", err)
	}
	if _, err := dispatch.Serve(d, []string{"t"}, leaf(t, newNode("root"), wire.Child[wire.Wire]{Key: []byte("k"), Tree: nilOwn{}})); !errors.Is(err, core.ErrInvalidWire) {
		t.Fatalf("a foreign node without an own Wire: %v", err)
	}
	if _, err := d.Register([]string{"t", "taken"}, answering("outside")); err != nil {
		t.Fatal(err)
	}
	conflicting := leaf(t, newNode("root"), wire.Child[wire.Wire]{Key: []byte("taken"), Tree: leaf(t, newNode("mine"))})
	if _, err := dispatch.Serve(d, []string{"t"}, conflicting); !errors.Is(err, core.ErrReceiverExists) {
		t.Fatalf("a position routed elsewhere: %v", err)
	}
	if _, err := callName(near, "t"); publicCode(err) != "method_not_found" {
		t.Fatalf("a refused Serve left routes behind: %v", err)
	}
	if name, err := callName(near, "t", "taken"); err != nil || name != "outside" {
		t.Fatalf("a refused Serve disturbed the other route: %q %v", name, err)
	}
}

type nilOwn struct{}

func (nilOwn) Own() wire.Wire                                  { return nil }
func (nilOwn) Children() []wire.Child[wire.Wire]               { return nil }
func (nilOwn) Decompose() (wire.Wire, []wire.Child[wire.Wire]) { return nil, nil }
func (n nilOwn) At(path wire.TreePath) (wire.DeixisNode[wire.Wire], bool) {
	if len(path) == 0 {
		return n, true
	}
	return nil, false
}

func TestServeUpdateKeepsAdmittedRequestsAndCloseKeepsTheDispatcher(t *testing.T) {
	near, d := localDispatcher(t)
	old, replacement := newNode("old"), newNode("new")
	old.hold = true
	served, err := dispatch.Serve(d, []string{"t"}, leaf(t, old))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- dispatch.Call(ctx, near, []string{"t"}, nil, nil) }()
	if got := receive(t, old.arrived); got != "old:request" {
		t.Fatalf("first arrival %q", got)
	}
	if err := served.Update(leaf(t, replacement)); err != nil {
		t.Fatal(err)
	}
	cancel()
	if got := receive(t, old.arrived); got != "old:cancel" {
		t.Fatalf("the cancellation reached %q", got)
	}
	<-done
	if name, err := callName(near, "t"); err != nil || name != "new" {
		t.Fatalf("after Update: %q %v", name, err)
	}
	// A refused update leaves the served tree in place.
	if err := served.Update(leaf(t, newNode("x"), wire.Child[wire.Wire]{Key: []byte("k"), Tree: nilOwn{}})); !errors.Is(err, core.ErrInvalidWire) {
		t.Fatalf("a refused Update: %v", err)
	}
	if name, err := callName(near, "t"); err != nil || name != "new" {
		t.Fatalf("a refused Update changed the served tree: %q %v", name, err)
	}
	served.Close()
	if _, err := callName(near, "t"); publicCode(err) != "method_not_found" {
		t.Fatalf("Close left a route: %v", err)
	}
	if _, err := d.Register([]string{"t"}, answering("after")); err != nil {
		t.Fatalf("the dispatcher is not usable after Close: %v", err)
	}
	if name, err := callName(near, "t"); err != nil || name != "after" {
		t.Fatalf("the endpoint is not usable after Close: %q %v", name, err)
	}
}
