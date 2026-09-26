package duplex_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/Bitspark/nightseam/duplex/go"
)

func TestPathEncodingIsCanonicalAndComposable(t *testing.T) {
	paths := [][]string{{}, {""}, {"a", "b"}, {"a.b"}, {"a", "b:c"}, {"é", "e\u0301", "😀", "\ufeff", "\x00"}}
	seen := map[string]bool{}
	for _, path := range paths {
		encoded, err := duplex.EncodePath(path)
		if err != nil {
			t.Fatal(err)
		}
		if seen[encoded] {
			t.Fatalf("paths alias at %q", encoded)
		}
		seen[encoded] = true
		decoded, err := duplex.DecodePath(encoded)
		if err != nil || !reflect.DeepEqual(decoded, path) {
			t.Fatalf("%q: %#v, %v", encoded, decoded, err)
		}
		for _, suffix := range paths {
			b, _ := duplex.EncodePath(suffix)
			combined, _ := duplex.EncodePath(append(append([]string{}, path...), suffix...))
			if combined != encoded+b {
				t.Fatal("prefixing did not compose by concatenation")
			}
		}
	}
	if got, _ := duplex.EncodePath([]string{"a", "😀", ""}); got != "1:a4:😀0:" {
		t.Fatal(got)
	}
	for _, malformed := range []string{"01:a", "00:", "1", ":", "-1:a", "2:a", "1:é", "99999999999999999999999999999:x", "1:\xff"} {
		if _, err := duplex.DecodePath(malformed); err == nil {
			t.Fatalf("accepted %q", malformed)
		}
	}
	if _, err := duplex.EncodePath([]string{"\xff"}); err == nil {
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
type rootAttachment struct{ receiver duplex.Receiver }
type queuedDelivery struct {
	path    []string
	message duplex.Message
}
type nonComparableRoot struct {
	*queuedRoot
	marker []int
}

func newRoot() *queuedRoot { return &queuedRoot{} }
func (r *queuedRoot) Send(path []string, message duplex.Message) error {
	if _, err := duplex.EncodePath(path); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return duplex.ErrClosed
	}
	r.queue = append(r.queue, queuedDelivery{append([]string{}, path...), message})
	return nil
}
func (r *queuedRoot) Receive(receiver duplex.Receiver) (func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, duplex.ErrClosed
	}
	if r.current != nil {
		return nil, duplex.ErrReceiverExists
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
func (r *queuedRoot) captured() duplex.Receiver {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current.receiver
}
func (r *queuedRoot) Close(code duplex.Code, reason string) error {
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

type sendOnly func([]string, duplex.Message) error

func (s sendOnly) Send(path []string, message duplex.Message) error { return s(path, message) }

func TestWireSelectionsGrantOnlySendAccess(t *testing.T) {
	root := newRoot()
	prefix := []string{"a.b"}
	selected := duplex.At(sendOnly(root.Send), prefix)
	prefix[0] = "changed"
	for _, view := range []duplex.Wire{selected, duplex.At(root, nil), duplex.At(selected, []string{"😀"})} {
		if _, ok := view.(interface {
			Receive(duplex.Receiver) (func(), error)
		}); ok {
			t.Fatal("selection grants receive authority")
		}
		if _, ok := view.(interface {
			Close(duplex.Code, string) error
		}); ok {
			t.Fatal("selection grants lifecycle authority")
		}
	}
	path := []string{"call"}
	if err := duplex.At(selected, []string{"😀"}).Send(path, duplex.Message{}); err != nil {
		t.Fatal(err)
	}
	path[0] = "changed"
	if !reflect.DeepEqual(root.queue[0].path, []string{"a.b", "😀", "call"}) {
		t.Fatal(root.queue)
	}
}

func TestMountPreservesPathsFramesAndReturnCapability(t *testing.T) {
	left, right, reply := newRoot(), newRoot(), newRoot()
	children := map[string]duplex.Endpoint{"left": left, "": right}
	mounted := duplex.Mount(children)
	children["left"] = reply
	address := &duplex.ReturnAddress{Wire: nonComparableRoot{reply, []int{1}}}
	var paths [][]string
	var received []duplex.Message
	_, err := mounted.Receive(duplex.Receiver{Message: func(path []string, message duplex.Message) {
		paths = append(paths, path)
		received = append(received, message)
	}})
	if err != nil {
		t.Fatal(err)
	}
	frames := []duplex.ProfileFrame{
		{Version: 1, Kind: duplex.ProfileRequest, ID: "c:1", Params: json.RawMessage(`{"n":9007199254740993}`), Meta: map[string]string{"tag": "value"}},
		{Version: 1, Kind: duplex.ProfileResponse, ID: "c:1", Error: &duplex.ProfileError{Code: "refused", Message: "No", Data: json.RawMessage(`{"why":"test"}`)}},
		{Version: 1, Kind: duplex.ProfileEvent, Data: json.RawMessage(`null`)},
		{Version: 1, Kind: duplex.ProfileCancel, ID: "c:1"},
	}
	view := duplex.At(duplex.At(mounted, []string{"left"}), []string{"😀"})
	for _, frame := range frames {
		if err := view.Send([]string{"call"}, duplex.Message{Frame: frame, Return: address}); err != nil {
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
	if err := mounted.Send([]string{""}, duplex.Message{}); err != nil {
		t.Fatal(err)
	}
	right.drain()
	if !reflect.DeepEqual(paths[4], []string{""}) {
		t.Fatal(paths[4])
	}
	for _, path := range [][]string{nil, {"missing"}} {
		if err := mounted.Send(path, duplex.Message{}); !errors.Is(err, duplex.ErrNoRoute) {
			t.Fatal(err)
		}
	}
	if err := mounted.Send([]string{"left", "\xff"}, duplex.Message{}); !errors.Is(err, duplex.ErrPath) {
		t.Fatal(err)
	}
}

func TestMountRefusesDuplicateAttachmentAndRebindsWithoutStealingCapturedDeliveries(t *testing.T) {
	root, reply := newRoot(), newRoot()
	mounted := duplex.Mount(map[string]duplex.Endpoint{"service": root})
	address := &duplex.ReturnAddress{Wire: duplex.At(reply, nil)}
	var old, fresh []duplex.ProfileKind
	var oldPaths [][]string
	closed := 0
	detach, err := mounted.Receive(duplex.Receiver{
		Message: func(path []string, message duplex.Message) {
			oldPaths = append(oldPaths, path)
			old = append(old, message.Frame.Kind)
			if message.Return != address {
				t.Fatal("return capability changed")
			}
		},
		Closed: func(duplex.Code, string) { closed++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	captured := root.captured()
	if _, err := mounted.Receive(duplex.Receiver{}); !errors.Is(err, duplex.ErrReceiverExists) {
		t.Fatal(err)
	}
	captured.Message([]string{"wait"}, duplex.Message{Frame: duplex.ProfileFrame{Kind: duplex.ProfileRequest}, Return: address})
	detach()
	detach()
	if root.attached() || root.closed {
		t.Fatal("detach retained ownership or closed borrowed root")
	}
	freshDetach, err := mounted.Receive(duplex.Receiver{Message: func(_ []string, message duplex.Message) { fresh = append(fresh, message.Frame.Kind) }})
	if err != nil {
		t.Fatal(err)
	}
	detach() // The old token must never remove the new attachment.
	captured.Closed(duplex.CodeNormal, "stale close")
	captured.Message([]string{"wait"}, duplex.Message{Frame: duplex.ProfileFrame{Kind: duplex.ProfileCancel}, Return: address})
	if err := address.Wire.Send(nil, duplex.Message{Frame: duplex.ProfileFrame{Kind: duplex.ProfileResponse}}); err != nil {
		t.Fatal(err)
	}
	if err := mounted.Send([]string{"service", "new"}, duplex.Message{Frame: duplex.ProfileFrame{Kind: duplex.ProfileEvent}}); err != nil {
		t.Fatal(err)
	}
	root.drain()
	if !reflect.DeepEqual(old, []duplex.ProfileKind{duplex.ProfileRequest, duplex.ProfileCancel}) || !reflect.DeepEqual(fresh, []duplex.ProfileKind{duplex.ProfileEvent}) || closed != 0 || len(reply.queue) != 1 {
		t.Fatal(old, fresh, closed, reply.queue)
	}
	if !reflect.DeepEqual(oldPaths, [][]string{{"service", "wait"}, {"service", "wait"}}) {
		t.Fatal(oldPaths)
	}
	freshDetach()
	_ = mounted.Close(duplex.CodeNormal, "done")
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
				if _, err := occupied.Receive(duplex.Receiver{}); err != nil {
					t.Fatal(err)
				}
			}
			mounted := duplex.Mount(map[string]duplex.Endpoint{"a": first, "z": occupied})
			if _, err := mounted.Receive(duplex.Receiver{}); !errors.Is(err, duplex.ErrReceiverExists) {
				t.Fatal(err)
			}
			if first.attached() || first.closed || occupied.closed {
				t.Fatal("failed acquisition leaked or closed a child")
			}
			if !duplicate && !occupied.attached() {
				t.Fatal("rollback removed another owner")
			}
			if _, err := first.Receive(duplex.Receiver{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMountedChildEndKeepsHealthySiblingAndEndsOwnerOnce(t *testing.T) {
	left, right := newRoot(), newRoot()
	mounted := duplex.Mount(map[string]duplex.Endpoint{"left": left, "right": right})
	closed, deliveries := 0, 0
	_, err := mounted.Receive(duplex.Receiver{
		Message: func(path []string, _ duplex.Message) {
			if !reflect.DeepEqual(path, []string{"right", "call"}) {
				t.Fatal(path)
			}
			deliveries++
		},
		Closed: func(code duplex.Code, reason string) {
			closed++
			if code != duplex.CodeNormal || reason != "last" {
				t.Fatal(code, reason)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	stale := left.captured()
	_ = left.Close(duplex.CodeNormal, "first")
	stale.Closed(duplex.CodeNormal, "duplicate")
	if closed != 0 {
		t.Fatal("one child ended the mount attachment")
	}
	if err := mounted.Send([]string{"right", "call"}, duplex.Message{}); err != nil {
		t.Fatal(err)
	}
	right.drain()
	_ = right.Close(duplex.CodeNormal, "last")
	if closed != 1 || deliveries != 1 {
		t.Fatal(closed, deliveries)
	}
	if err := mounted.Send(nil, duplex.Message{}); !errors.Is(err, duplex.ErrNoRoute) {
		t.Fatal("child ending permanently closed mount", err)
	}
	if _, err := mounted.Receive(duplex.Receiver{}); !errors.Is(err, duplex.ErrClosed) {
		t.Fatal(err)
	}
	_ = mounted.Close(duplex.CodeNormal, "mount")
	if closed != 1 {
		t.Fatal(closed)
	}
}

func TestMountCloseDetachesOwnAttachmentAndPreservesChildren(t *testing.T) {
	root := newRoot()
	mounted := duplex.Mount(map[string]duplex.Endpoint{"": root})
	closed := 0
	_, err := mounted.Receive(duplex.Receiver{Closed: func(code duplex.Code, reason string) {
		closed++
		if code != duplex.CodeNormal || reason != "mount ended" {
			t.Error(code, reason)
		}
		_ = mounted.Close(code, reason)
		if _, err := root.Receive(duplex.Receiver{}); err != nil {
			t.Error("child was not released before closure callback", err)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	_ = mounted.Close(duplex.CodeNormal, "mount ended")
	_ = mounted.Close(duplex.CodeNormal, "again")
	if closed != 1 || root.closes != 0 || !root.attached() {
		t.Fatal(closed, root.closes, root.attached())
	}
	if err := mounted.Send([]string{""}, duplex.Message{}); !errors.Is(err, duplex.ErrClosed) {
		t.Fatal(err)
	}
	if _, err := mounted.Receive(duplex.Receiver{}); !errors.Is(err, duplex.ErrClosed) {
		t.Fatal(err)
	}
	if err := root.Send(nil, duplex.Message{}); err != nil {
		t.Fatal(err)
	}
}

type registeringRoot struct {
	*queuedRoot
	registered chan struct{}
	resume     chan struct{}
}

func (r *registeringRoot) Receive(receiver duplex.Receiver) (func(), error) {
	detach, err := r.queuedRoot.Receive(receiver)
	close(r.registered)
	<-r.resume
	return detach, err
}
func TestMountCloseDuringReceiveDisposesLateChildAttachment(t *testing.T) {
	root := &registeringRoot{newRoot(), make(chan struct{}), make(chan struct{})}
	mounted := duplex.Mount(map[string]duplex.Endpoint{"x": root})
	finished := make(chan error, 1)
	closed := 0
	go func() {
		_, err := mounted.Receive(duplex.Receiver{Closed: func(duplex.Code, string) { closed++ }})
		finished <- err
	}()
	<-root.registered
	_ = mounted.Close(duplex.CodeNormal, "done")
	close(root.resume)
	if err := <-finished; !errors.Is(err, duplex.ErrClosed) {
		t.Fatal(err)
	}
	if root.attached() || root.closes != 0 || closed != 1 {
		t.Fatal(root.attached(), root.closes, closed)
	}
}

type endingRoot struct{ *queuedRoot }

func (r *endingRoot) Receive(receiver duplex.Receiver) (func(), error) {
	detach, err := r.queuedRoot.Receive(receiver)
	if err == nil {
		_ = r.Close(duplex.CodeNormal, "ended during acquisition")
	}
	return detach, err
}
func TestMountChildEndingDuringAcquisitionRollsBackHealthySibling(t *testing.T) {
	healthy := newRoot()
	ending := &endingRoot{newRoot()}
	mounted := duplex.Mount(map[string]duplex.Endpoint{"a": healthy, "z": ending})
	if _, err := mounted.Receive(duplex.Receiver{}); !errors.Is(err, duplex.ErrClosed) {
		t.Fatal(err)
	}
	if healthy.attached() || ending.attached() || healthy.closed {
		t.Fatal("partial acquisition retained a child")
	}
	if _, err := healthy.Receive(duplex.Receiver{}); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyMountStillOwnsOneDetachableAttachment(t *testing.T) {
	mounted := duplex.Mount(nil)
	detach, err := mounted.Receive(duplex.Receiver{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mounted.Receive(duplex.Receiver{}); !errors.Is(err, duplex.ErrReceiverExists) {
		t.Fatal(err)
	}
	detach()
	closed := 0
	_, err = mounted.Receive(duplex.Receiver{Closed: func(duplex.Code, string) { closed++ }})
	if err != nil {
		t.Fatal(err)
	}
	detach()
	_ = mounted.Close(duplex.CodeNormal, "done")
	if closed != 1 {
		t.Fatal(closed)
	}
}
