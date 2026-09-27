package core

import (
	"errors"
	"slices"
	"sort"
	"sync"

	"github.com/Bitspark/bitruntime/internal/profile/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// ErrReceiverExists refuses a second receive attachment to an endpoint.
var ErrReceiverExists = errors.New("bitruntime: endpoint already has a receiver")

type selectedWire struct {
	root   wire.AddressedWire
	prefix []string
}

// At binds a relative path prefix to addressed access without allocating a
// peer, channel or queue: at(w, []) ≃ w and at(at(w, a), b) ≃ at(w, a ++ b).
// The result grants only send access, even when path is empty. This is
// addressed prefix binding, not structural selection: it succeeds for every
// path, and whether the root admits what is sent through it is the root's
// to decide. Select is the structural operation on a WireTree.
func At(root wire.AddressedWire, path []string) wire.AddressedWire {
	return &selectedWire{root: root, prefix: slices.Clone(path)}
}

func (w *selectedWire) path(path []string) []string {
	return append(append([]string{}, w.prefix...), path...)
}
func (w *selectedWire) Send(path []string, message wire.Message) error {
	return w.root.Send(w.path(path), message)
}

type boundWire struct {
	access wire.AddressedWire
	path   []string
}

// Bind returns addressless access that sends at one fixed addressed path:
// Bind(access, path).Send(m) is access.Send(path, m), with the message and its
// return capability unchanged and a refusal returned as access returns it. The
// path is copied. The Wire grants only sending: no receive attachment, closure
// or structure.
//
// A carrier path names a position, not a node: addressed access cannot reveal
// that two far positions share one node. A tree whose own values live across a
// carrier therefore binds each position it names.
func Bind(access wire.AddressedWire, path []string) wire.Wire {
	return &boundWire{access: access, path: slices.Clone(path)}
}

func (b *boundWire) Send(message wire.Message) error {
	return b.access.Send(slices.Clone(b.path), message)
}

type mountedWire struct {
	children map[string]wire.Endpoint
	mu       sync.Mutex
	closed   bool
	current  *mountedReceiver
}
type mountedReceiver struct {
	receiver  wire.Receiver
	active    bool
	children  []*mountedChild
	remaining int
}
type mountedChild struct {
	detach func()
	ended  bool
}

// Mount consumes one path segment and delegates to that child. The map is
// copied. A mount has no leaf at []; [""] can select an empty-string key.
// Its single receive attachment borrows one attachment from each child.
// Closing a mount detaches those attachments and leaves every child usable:
// at(mount({k: w}), [k]) ≃ w while the mount is open.
func Mount(children map[string]wire.Endpoint) wire.Endpoint {
	w := &mountedWire{children: make(map[string]wire.Endpoint, len(children))}
	for key, child := range children {
		w.children[key] = child
	}
	return w
}

func (w *mountedWire) destination(path []string) (wire.Endpoint, error) {
	if w.closed {
		return nil, transports.ErrClosed
	}
	if len(path) == 0 {
		return nil, ErrMissingPath
	}
	if !profile.ValidPath(path) {
		return nil, ErrInvalidPath
	}
	child := w.children[path[0]]
	if child == nil {
		return nil, ErrMissingPath
	}
	return child, nil
}
func (w *mountedWire) Send(path []string, message wire.Message) error {
	w.mu.Lock()
	child, err := w.destination(path)
	w.mu.Unlock()
	if err != nil {
		return err
	}
	return child.Send(append([]string{}, path[1:]...), message)
}
func (w *mountedWire) Receive(receiver wire.Receiver) (func(), error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, transports.ErrClosed
	}
	if w.current != nil {
		w.mu.Unlock()
		return nil, ErrReceiverExists
	}
	keys := make([]string, 0, len(w.children))
	for key, child := range w.children {
		if child != nil {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	attachment := &mountedReceiver{receiver: receiver, active: true, remaining: len(keys)}
	for range keys {
		attachment.children = append(attachment.children, &mountedChild{})
	}
	w.current = attachment
	w.mu.Unlock()

	for i, key := range keys {
		slot := attachment.children[i]
		w.mu.Lock()
		active := attachment.active
		w.mu.Unlock()
		if !active {
			return nil, transports.ErrClosed
		}
		detach, err := w.children[key].Receive(wire.Receiver{
			Message: func(path []string, message wire.Message) {
				// The child owns capture of accepted invocations. A retained
				// delivery, including cancellation, keeps its original receiver.
				if receiver.Message != nil {
					receiver.Message(append([]string{key}, path...), message)
				}
			},
			Closed: func(code wire.Code, reason string) { w.childEnded(attachment, slot, code, reason) },
		})
		w.mu.Lock()
		active = attachment.active && !slot.ended
		if err == nil && active {
			slot.detach = detach
		}
		w.mu.Unlock()
		if err != nil || !active {
			// Close may happen while the child's Receive is returning. Its
			// late disposer is still ours, even after the attachment ended.
			if detach != nil {
				detach()
			}
			w.remove(attachment)
			if err != nil {
				return nil, err
			}
			return nil, transports.ErrClosed
		}
	}
	w.mu.Lock()
	active := attachment.active
	w.mu.Unlock()
	if !active {
		return nil, transports.ErrClosed
	}
	return func() { w.remove(attachment) }, nil
}

// releaseLocked retires only this attachment. Clear its ownership before
// invoking borrowed disposers or callbacks, which may reenter the mount.
func (w *mountedWire) releaseLocked(attachment *mountedReceiver) []func() {
	attachment.active = false
	if w.current == attachment {
		w.current = nil
	}
	var detaches []func()
	for _, child := range attachment.children {
		if child.detach != nil {
			detaches = append(detaches, child.detach)
			child.detach = nil
		}
	}
	return detaches
}

func (w *mountedWire) remove(attachment *mountedReceiver) {
	w.mu.Lock()
	if !attachment.active {
		w.mu.Unlock()
		return
	}
	detaches := w.releaseLocked(attachment)
	w.mu.Unlock()
	for _, detach := range detaches {
		detach()
	}
}

func (w *mountedWire) childEnded(attachment *mountedReceiver, child *mountedChild, code wire.Code, reason string) {
	w.mu.Lock()
	if !attachment.active || child.ended {
		w.mu.Unlock()
		return
	}
	child.ended = true
	attachment.remaining--
	last := attachment.remaining == 0
	var detaches []func()
	if last {
		detaches = w.releaseLocked(attachment)
	} else if child.detach != nil {
		detaches = append(detaches, child.detach)
		child.detach = nil
	}
	w.mu.Unlock()
	for _, detach := range detaches {
		detach()
	}
	if last && attachment.receiver.Closed != nil {
		attachment.receiver.Closed(code, reason)
	}
}

func (w *mountedWire) Close(code wire.Code, reason string) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	attachment := w.current
	var detaches []func()
	if attachment != nil {
		detaches = w.releaseLocked(attachment)
	}
	w.mu.Unlock()
	for _, detach := range detaches {
		detach()
	}
	if attachment != nil && attachment.receiver.Closed != nil {
		attachment.receiver.Closed(code, reason)
	}
	return nil
}
