package core_test

// Ported from Nightseam v0.6.0 duplex/go/wire_test.go (commit 5cc9723a): At,
// Mount and the canonical path encoding the addressed carriers share.

import (
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"

	core "github.com/Bitspark/bitruntime/core/go"
	"github.com/Bitspark/bitruntime/internal/profile/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

func TestPathEncodingIsCanonicalAndComposable(t *testing.T) {
	paths := [][]string{{}, {""}, {"a", "b"}, {"a.b"}, {"a", "b:c"}, {"é", "e\u0301", "😀", "\ufeff", "\x00"}}
	seen := map[string]bool{}
	for _, path := range paths {
		encoded, err := profile.EncodePath(path)
		if err != nil {
			t.Fatal(err)
		}
		if seen[encoded] {
			t.Fatalf("paths alias at %q", encoded)
		}
		seen[encoded] = true
		decoded, err := profile.DecodePath(encoded)
		if err != nil || !reflect.DeepEqual(decoded, path) {
			t.Fatalf("%q: %#v, %v", encoded, decoded, err)
		}
		for _, suffix := range paths {
			b, _ := profile.EncodePath(suffix)
			combined, _ := profile.EncodePath(append(append([]string{}, path...), suffix...))
			if combined != encoded+b {
				t.Fatal("prefixing did not compose by concatenation")
			}
		}
	}
	if got, _ := profile.EncodePath([]string{"a", "😀", ""}); got != "1:a4:😀0:" {
		t.Fatal(got)
	}
	for _, malformed := range []string{"01:a", "00:", "1", ":", "-1:a", "2:a", "1:é", "99999999999999999999999999999:x", "1:\xff"} {
		if _, err := profile.DecodePath(malformed); err == nil {
			t.Fatalf("accepted %q", malformed)
		}
	}
	if _, err := profile.EncodePath([]string{"\xff"}); err == nil {
		t.Fatal("accepted non-scalar UTF-8")
	}
}

// queuedRoot is a deterministic endpoint fixture. Only drain executes queued
// deliveries, so composition cannot pass the asynchronous check by timing luck.
type queuedRoot struct {
	mu      sync.Mutex
	queue   []queuedDelivery
	current *rootAttachment
	closed  bool
	closes  int
}
type rootAttachment struct{ receiver wire.Receiver }
type queuedDelivery struct {
	path    []string
	message wire.Message
}
type nonComparableRoot struct {
	*queuedRoot
	marker []int
}

func newRoot() *queuedRoot { return &queuedRoot{} }
func (r *queuedRoot) Send(path []string, message wire.Message) error {
	if _, err := profile.EncodePath(path); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return transports.ErrClosed
	}
	r.queue = append(r.queue, queuedDelivery{append([]string{}, path...), message})
	return nil
}
func (r *queuedRoot) Receive(receiver wire.Receiver) (func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, transports.ErrClosed
	}
	if r.current != nil {
		return nil, core.ErrReceiverExists
	}
	attachment := &rootAttachment{receiver}
	r.current = attachment
	return func() {
		r.mu.Lock()
		if r.current == attachment {
			r.current = nil
		}
		r.mu.Unlock()
	}, nil
}
func (r *queuedRoot) attached() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current != nil
}
func (r *queuedRoot) captured() wire.Receiver {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current.receiver
}
func (r *queuedRoot) Close(code wire.Code, reason string) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.closes++
	attachment := r.current
	r.current = nil
	r.mu.Unlock()
	if attachment != nil && attachment.receiver.Closed != nil {
		attachment.receiver.Closed(code, reason)
	}
	return nil
}
func (r *queuedRoot) drain() {
	for {
		r.mu.Lock()
		if len(r.queue) == 0 {
			r.mu.Unlock()
			return
		}
		d := r.queue[0]
		r.queue = r.queue[1:]
		attachment := r.current
		r.mu.Unlock()
		if attachment != nil && attachment.receiver.Message != nil {
			attachment.receiver.Message(d.path, d.message)
		}
	}
}

type sendOnly func([]string, wire.Message) error

func (s sendOnly) Send(path []string, message wire.Message) error { return s(path, message) }

func TestWireSelectionsGrantOnlySendAccess(t *testing.T) {
	root := newRoot()
	prefix := []string{"a.b"}
	selected := core.At(sendOnly(root.Send), prefix)
	prefix[0] = "changed"
	for _, view := range []wire.AddressedWire{selected, core.At(root, nil), core.At(selected, []string{"😀"})} {
		if _, ok := view.(interface {
			Receive(wire.Receiver) (func(), error)
		}); ok {
			t.Fatal("selection grants receive authority")
		}
		if _, ok := view.(interface {
			Close(wire.Code, string) error
		}); ok {
			t.Fatal("selection grants lifecycle authority")
		}
	}
	path := []string{"call"}
	if err := core.At(selected, []string{"😀"}).Send(path, wire.Message{}); err != nil {
		t.Fatal(err)
	}
	path[0] = "changed"
	if !reflect.DeepEqual(root.queue[0].path, []string{"a.b", "😀", "call"}) {
		t.Fatal(root.queue)
	}
}

func TestMountPreservesPathsFramesAndReturnCapability(t *testing.T) {
	left, right, reply := newRoot(), newRoot(), newRoot()
	children := map[string]wire.Endpoint{"left": left, "": right}
	mounted := core.Mount(children)
	children["left"] = reply
	address := &wire.ReturnAddress{Wire: nonComparableRoot{reply, []int{1}}}
	var paths [][]string
	var received []wire.Message
	_, err := mounted.Receive(wire.Receiver{Message: func(path []string, message wire.Message) {
		paths = append(paths, path)
		received = append(received, message)
	}})
	if err != nil {
		t.Fatal(err)
	}
	frames := []wire.ProfileFrame{
		{Version: 1, Kind: wire.ProfileRequest, ID: "c:1", Params: json.RawMessage(`{"n":9007199254740993}`), Meta: map[string]string{"tag": "value"}},
		{Version: 1, Kind: wire.ProfileResponse, ID: "c:1", Error: &wire.ProfileError{Code: "refused", Message: "No", Data: json.RawMessage(`{"why":"test"}`)}},
		{Version: 1, Kind: wire.ProfileEvent, Data: json.RawMessage(`null`)},
		{Version: 1, Kind: wire.ProfileCancel, ID: "c:1"},
	}
	view := core.At(core.At(mounted, []string{"left"}), []string{"😀"})
	for _, frame := range frames {
		if err := view.Send([]string{"call"}, wire.Message{Frame: frame, Return: address}); err != nil {
			t.Fatal(err)
		}
	}
	if len(received) != 0 || len(left.queue) != 4 || len(reply.queue) != 0 {
		t.Fatal("composition changed dispatch ownership")
	}
	for _, delivery := range left.queue {
		if !reflect.DeepEqual(delivery.path, []string{"😀", "call"}) {
			t.Fatal(delivery.path)
		}
	}
	left.drain()
	for i, frame := range frames {
		if !reflect.DeepEqual(paths[i], []string{"left", "😀", "call"}) || !reflect.DeepEqual(received[i].Frame, frame) || received[i].Return != address {
			t.Fatal(paths[i], received[i])
		}
	}
	if err := mounted.Send([]string{""}, wire.Message{}); err != nil {
		t.Fatal(err)
	}
	right.drain()
	if !reflect.DeepEqual(paths[4], []string{""}) {
		t.Fatal(paths[4])
	}
	for _, path := range [][]string{nil, {"missing"}} {
		if err := mounted.Send(path, wire.Message{}); !errors.Is(err, core.ErrMissingPath) {
			t.Fatal(err)
		}
	}
	if err := mounted.Send([]string{"left", "\xff"}, wire.Message{}); !errors.Is(err, core.ErrInvalidPath) {
		t.Fatal(err)
	}
}

func TestMountRefusesDuplicateAttachmentAndRebindsWithoutStealingCapturedDeliveries(t *testing.T) {
	root, reply := newRoot(), newRoot()
	mounted := core.Mount(map[string]wire.Endpoint{"service": root})
	address := &wire.ReturnAddress{Wire: core.At(reply, nil)}
	var old, fresh []wire.ProfileKind
	var oldPaths [][]string
	closed := 0
	detach, err := mounted.Receive(wire.Receiver{
		Message: func(path []string, message wire.Message) {
			oldPaths = append(oldPaths, path)
			old = append(old, message.Frame.Kind)
			if message.Return != address {
				t.Fatal("return capability changed")
			}
		},
		Closed: func(wire.Code, string) { closed++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	captured := root.captured()
	if _, err := mounted.Receive(wire.Receiver{}); !errors.Is(err, core.ErrReceiverExists) {
		t.Fatal(err)
	}
	captured.Message([]string{"wait"}, wire.Message{Frame: wire.ProfileFrame{Kind: wire.ProfileRequest}, Return: address})
	detach()
	detach()
	if root.attached() || root.closed {
		t.Fatal("detach retained ownership or closed borrowed root")
	}
	freshDetach, err := mounted.Receive(wire.Receiver{Message: func(_ []string, message wire.Message) { fresh = append(fresh, message.Frame.Kind) }})
	if err != nil {
		t.Fatal(err)
	}
	detach() // The old token must never remove the new attachment.
	captured.Closed(transports.CodeNormal, "stale close")
	captured.Message([]string{"wait"}, wire.Message{Frame: wire.ProfileFrame{Kind: wire.ProfileCancel}, Return: address})
	if err := address.Wire.Send(nil, wire.Message{Frame: wire.ProfileFrame{Kind: wire.ProfileResponse}}); err != nil {
		t.Fatal(err)
	}
	if err := mounted.Send([]string{"service", "new"}, wire.Message{Frame: wire.ProfileFrame{Kind: wire.ProfileEvent}}); err != nil {
		t.Fatal(err)
	}
	root.drain()
	if !reflect.DeepEqual(old, []wire.ProfileKind{wire.ProfileRequest, wire.ProfileCancel}) || !reflect.DeepEqual(fresh, []wire.ProfileKind{wire.ProfileEvent}) || closed != 0 || len(reply.queue) != 1 {
		t.Fatal(old, fresh, closed, reply.queue)
	}
	if !reflect.DeepEqual(oldPaths, [][]string{{"service", "wait"}, {"service", "wait"}}) {
		t.Fatal(oldPaths)
	}
	freshDetach()
	_ = mounted.Close(transports.CodeNormal, "done")
	if closed != 0 || root.closed {
		t.Fatal("detached owner or borrowed child was closed")
	}
}

func TestMountAttachmentFailureRollsBackOnlyItsBorrowedAttachments(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(map[bool]string{false: "occupied-child", true: "aliased-child"}[duplicate], func(t *testing.T) {
			first, occupied := newRoot(), newRoot()
			if duplicate {
				occupied = first
			} else {
				if _, err := occupied.Receive(wire.Receiver{}); err != nil {
					t.Fatal(err)
				}
			}
			mounted := core.Mount(map[string]wire.Endpoint{"a": first, "z": occupied})
			if _, err := mounted.Receive(wire.Receiver{}); !errors.Is(err, core.ErrReceiverExists) {
				t.Fatal(err)
			}
			if first.attached() || first.closed || occupied.closed {
				t.Fatal("failed acquisition leaked or closed a child")
			}
			if !duplicate && !occupied.attached() {
				t.Fatal("rollback removed another owner")
			}
			if _, err := first.Receive(wire.Receiver{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMountedChildEndKeepsHealthySiblingAndEndsOwnerOnce(t *testing.T) {
	left, right := newRoot(), newRoot()
	mounted := core.Mount(map[string]wire.Endpoint{"left": left, "right": right})
	closed, deliveries := 0, 0
	_, err := mounted.Receive(wire.Receiver{
		Message: func(path []string, _ wire.Message) {
			if !reflect.DeepEqual(path, []string{"right", "call"}) {
				t.Fatal(path)
			}
			deliveries++
		},
		Closed: func(code wire.Code, reason string) {
			closed++
			if code != transports.CodeNormal || reason != "last" {
				t.Fatal(code, reason)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	stale := left.captured()
	_ = left.Close(transports.CodeNormal, "first")
	stale.Closed(transports.CodeNormal, "duplicate")
	if closed != 0 {
		t.Fatal("one child ended the mount attachment")
	}
	if err := mounted.Send([]string{"right", "call"}, wire.Message{}); err != nil {
		t.Fatal(err)
	}
	right.drain()
	_ = right.Close(transports.CodeNormal, "last")
	if closed != 1 || deliveries != 1 {
		t.Fatal(closed, deliveries)
	}
	if err := mounted.Send(nil, wire.Message{}); !errors.Is(err, core.ErrMissingPath) {
		t.Fatal("child ending permanently closed mount", err)
	}
	if _, err := mounted.Receive(wire.Receiver{}); !errors.Is(err, transports.ErrClosed) {
		t.Fatal(err)
	}
	_ = mounted.Close(transports.CodeNormal, "mount")
	if closed != 1 {
		t.Fatal(closed)
	}
}

func TestMountCloseDetachesOwnAttachmentAndPreservesChildren(t *testing.T) {
	root := newRoot()
	mounted := core.Mount(map[string]wire.Endpoint{"": root})
	closed := 0
	_, err := mounted.Receive(wire.Receiver{Closed: func(code wire.Code, reason string) {
		closed++
		if code != transports.CodeNormal || reason != "mount ended" {
			t.Error(code, reason)
		}
		_ = mounted.Close(code, reason)
		if _, err := root.Receive(wire.Receiver{}); err != nil {
			t.Error("child was not released before closure callback", err)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	_ = mounted.Close(transports.CodeNormal, "mount ended")
	_ = mounted.Close(transports.CodeNormal, "again")
	if closed != 1 || root.closes != 0 || !root.attached() {
		t.Fatal(closed, root.closes, root.attached())
	}
	if err := mounted.Send([]string{""}, wire.Message{}); !errors.Is(err, transports.ErrClosed) {
		t.Fatal(err)
	}
	if _, err := mounted.Receive(wire.Receiver{}); !errors.Is(err, transports.ErrClosed) {
		t.Fatal(err)
	}
	if err := root.Send(nil, wire.Message{}); err != nil {
		t.Fatal(err)
	}
}

type registeringRoot struct {
	*queuedRoot
	registered chan struct{}
	resume     chan struct{}
}

func (r *registeringRoot) Receive(receiver wire.Receiver) (func(), error) {
	detach, err := r.queuedRoot.Receive(receiver)
	close(r.registered)
	<-r.resume
	return detach, err
}
func TestMountCloseDuringReceiveDisposesLateChildAttachment(t *testing.T) {
	root := &registeringRoot{newRoot(), make(chan struct{}), make(chan struct{})}
	mounted := core.Mount(map[string]wire.Endpoint{"x": root})
	finished := make(chan error, 1)
	closed := 0
	go func() {
		_, err := mounted.Receive(wire.Receiver{Closed: func(wire.Code, string) { closed++ }})
		finished <- err
	}()
	<-root.registered
	_ = mounted.Close(transports.CodeNormal, "done")
	close(root.resume)
	if err := <-finished; !errors.Is(err, transports.ErrClosed) {
		t.Fatal(err)
	}
	if root.attached() || root.closes != 0 || closed != 1 {
		t.Fatal(root.attached(), root.closes, closed)
	}
}

type endingRoot struct{ *queuedRoot }

func (r *endingRoot) Receive(receiver wire.Receiver) (func(), error) {
	detach, err := r.queuedRoot.Receive(receiver)
	if err == nil {
		_ = r.Close(transports.CodeNormal, "ended during acquisition")
	}
	return detach, err
}
func TestMountChildEndingDuringAcquisitionRollsBackHealthySibling(t *testing.T) {
	healthy := newRoot()
	ending := &endingRoot{newRoot()}
	mounted := core.Mount(map[string]wire.Endpoint{"a": healthy, "z": ending})
	if _, err := mounted.Receive(wire.Receiver{}); !errors.Is(err, transports.ErrClosed) {
		t.Fatal(err)
	}
	if healthy.attached() || ending.attached() || healthy.closed {
		t.Fatal("partial acquisition retained a child")
	}
	if _, err := healthy.Receive(wire.Receiver{}); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyMountStillOwnsOneDetachableAttachment(t *testing.T) {
	mounted := core.Mount(nil)
	detach, err := mounted.Receive(wire.Receiver{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mounted.Receive(wire.Receiver{}); !errors.Is(err, core.ErrReceiverExists) {
		t.Fatal(err)
	}
	detach()
	closed := 0
	_, err = mounted.Receive(wire.Receiver{Closed: func(wire.Code, string) { closed++ }})
	if err != nil {
		t.Fatal(err)
	}
	detach()
	_ = mounted.Close(transports.CodeNormal, "done")
	if closed != 1 {
		t.Fatal(closed)
	}
}
