package runtime_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/Bitspark/nightseam/duplex/go"
	ws "github.com/Bitspark/nightseam/runtime/go"
)

// An endpoint has one owning attachment: a second is refused without replacing
// the first, detach is idempotent, and a later attachment is permitted.
func TestAnEndpointHasOneOwningAttachment(t *testing.T) {
	left, right, err := ws.NewWirePair(ws.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = left.Close(duplex.CodeNormal, "") })
	first := make(chan []string, 4)
	detach, err := right.Receive(duplex.Receiver{Message: func(path []string, _ duplex.Message) { first <- path }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := right.Receive(duplex.Receiver{}); !errors.Is(err, duplex.ErrReceiverExists) {
		t.Fatalf("a second attachment was accepted: %v", err)
	}
	if err := ws.EmitWire(t.Context(), left, []string{"one"}, nil); err != nil {
		t.Fatal(err)
	}
	if got := <-first; !slices.Equal(got, []string{"one"}) {
		t.Fatalf("first attachment saw %v", got)
	}
	detach()
	detach()
	second := make(chan []string, 4)
	if _, err := right.Receive(duplex.Receiver{Message: func(path []string, _ duplex.Message) { second <- path }}); err != nil {
		t.Fatalf("a later attachment was refused: %v", err)
	}
	if err := ws.EmitWire(t.Context(), left, []string{"two"}, nil); err != nil {
		t.Fatal(err)
	}
	if got := <-second; !slices.Equal(got, []string{"two"}) {
		t.Fatalf("second attachment saw %v", got)
	}
	select {
	case got := <-first:
		t.Fatalf("the detached attachment still received %v", got)
	default:
	}
}

// A dispatcher refuses a duplicate path and frees it when its registration
// detaches. Exact and prefix are separate spaces: one of each may hold a path.
func TestADispatcherRefusesADuplicatePath(t *testing.T) {
	endpoint := newInvocationEndpoint(ws.DefaultInvocationLimits())
	dispatch, err := ws.NewDispatcher(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	detach, err := dispatch.Register([]string{"a", "b"}, duplex.Receiver{Message: func([]string, duplex.Message) {}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatch.Register([]string{"a", "b"}, duplex.Receiver{}); !errors.Is(err, duplex.ErrReceiverExists) {
		t.Fatalf("a duplicate exact path was accepted: %v", err)
	}
	if _, err := dispatch.RegisterPrefix([]string{"a", "b"}, duplex.Receiver{}); err != nil {
		t.Fatalf("a prefix at an exact path was refused: %v", err)
	}
	if _, err := dispatch.RegisterPrefix([]string{"a", "b"}, duplex.Receiver{}); !errors.Is(err, duplex.ErrReceiverExists) {
		t.Fatalf("a duplicate prefix path was accepted: %v", err)
	}
	detach()
	if _, err := dispatch.Register([]string{"a", "b"}, duplex.Receiver{}); err != nil {
		t.Fatalf("a detached path was not freed: %v", err)
	}
}

// Overlapping prefixes: the longest match wins, and an exact registration wins
// over every prefix that would also have matched.
func TestOverlappingRoutesSelectTheLongestThenTheExact(t *testing.T) {
	endpoint := newInvocationEndpoint(ws.DefaultInvocationLimits())
	dispatch, err := ws.NewDispatcher(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	reached := make(chan string, 8)
	name := func(label string) duplex.Receiver {
		return duplex.Receiver{Message: func([]string, duplex.Message) { reached <- label }}
	}
	for _, route := range []struct {
		path  []string
		label string
	}{{nil, "root"}, {[]string{"a"}, "a"}, {[]string{"a", "b"}, "a/b"}} {
		if _, err := dispatch.RegisterPrefix(route.path, name(route.label)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := dispatch.Register([]string{"a", "b", "c"}, name("exact a/b/c")); err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		path  []string
		label string
	}{
		{[]string{"z"}, "root"},
		{[]string{"a", "z"}, "a"},
		{[]string{"a", "b", "z"}, "a/b"},
		{[]string{"a", "b", "c"}, "exact a/b/c"},
	} {
		endpoint.deliver(want.path, duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileEvent, Data: []byte("null")}})
		if got := <-reached; got != want.label {
			t.Fatalf("%v reached %q, want %q", want.path, got, want.label)
		}
	}
}

// A nested selection prepends its prefixes outgoing and strips them incoming,
// and every view is a view of the one root attachment.
func TestNestedSelectionPrependsAndStrips(t *testing.T) {
	left, right, err := ws.NewWirePair(ws.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = left.Close(duplex.CodeNormal, "") })
	dispatch, err := ws.NewDispatcher(right)
	if err != nil {
		t.Fatal(err)
	}
	inner := dispatch.Select([]string{"a"}).Select([]string{"b"})
	delivered := make(chan []string, 4)
	if _, err := inner.Receive(duplex.Receiver{Message: func(path []string, _ duplex.Message) { delivered <- path }}); err != nil {
		t.Fatal(err)
	}
	if err := ws.EmitWire(t.Context(), left, []string{"a", "b", "read"}, nil); err != nil {
		t.Fatal(err)
	}
	if got := <-delivered; !slices.Equal(got, []string{"read"}) {
		t.Fatalf("the nested view was delivered %v", got)
	}
	// And outgoing: what the view sends arrives at the root's full path.
	back := make(chan []string, 4)
	if _, err := left.Receive(duplex.Receiver{Message: func(path []string, _ duplex.Message) { back <- path }}); err != nil {
		t.Fatal(err)
	}
	if err := ws.EmitWire(t.Context(), inner, []string{"reply"}, nil); err != nil {
		t.Fatal(err)
	}
	if got := <-back; !slices.Equal(got, []string{"a", "b", "reply"}) {
		t.Fatalf("the nested view sent to %v", got)
	}
}

// Mounting chooses a child by one segment and restores it on delivery;
// closing the mount detaches its own attachments and leaves children usable.
func TestMountRoutesByOneSegmentAndBorrowsItsChildren(t *testing.T) {
	left, right, err := ws.NewWirePair(ws.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = left.Close(duplex.CodeNormal, "") })
	mount := duplex.Mount(map[string]duplex.Endpoint{"child": right})
	delivered := make(chan []string, 4)
	if _, err := mount.Receive(duplex.Receiver{Message: func(path []string, _ duplex.Message) { delivered <- path }}); err != nil {
		t.Fatal(err)
	}
	if err := ws.EmitWire(t.Context(), left, []string{"read"}, nil); err != nil {
		t.Fatal(err)
	}
	if got := <-delivered; !slices.Equal(got, []string{"child", "read"}) {
		t.Fatalf("the mount delivered %v", got)
	}
	// A mount has no destination at the empty path, and an unknown child has
	// no route at all.
	if err := mount.Send(nil, duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileEvent, Data: []byte("null")}}); !errors.Is(err, duplex.ErrNoRoute) {
		t.Fatalf("the mount had a destination at []: %v", err)
	}
	if err := mount.Send([]string{"absent"}, duplex.Message{Frame: duplex.ProfileFrame{Version: 1, Kind: duplex.ProfileEvent, Data: []byte("null")}}); !errors.Is(err, duplex.ErrNoRoute) {
		t.Fatalf("an unknown child had a route: %v", err)
	}
	if err := mount.Close(duplex.CodeNormal, ""); err != nil {
		t.Fatal(err)
	}
	after := make(chan []string, 4)
	if _, err := right.Receive(duplex.Receiver{Message: func(path []string, _ duplex.Message) { after <- path }}); err != nil {
		t.Fatalf("closing the mount closed its borrowed child: %v", err)
	}
	if err := ws.EmitWire(t.Context(), left, []string{"again"}, nil); err != nil {
		t.Fatal(err)
	}
	if got := <-after; !slices.Equal(got, []string{"again"}) {
		t.Fatalf("the borrowed child delivered %v", got)
	}
}
