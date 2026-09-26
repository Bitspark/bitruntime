package duplex

import (
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	bitwire "github.com/Bitspark/bitwire/wire/go"
)

// ProfileKind is one of the profile's four frame kinds. Correlation and
// validation remain the peer's; a wire only carries the frame.
type ProfileKind = bitwire.ProfileKind

const (
	ProfileRequest  = bitwire.ProfileRequest
	ProfileResponse = bitwire.ProfileResponse
	ProfileEvent    = bitwire.ProfileEvent
	ProfileCancel   = bitwire.ProfileCancel
)

// ProfileError is public error data, without a runtime error dependency.
type ProfileError = bitwire.ProfileError

// ProfileFrame carries a profile frame. The Send path is the request method or
// event name; keeping it outside this value prevents contradictory names.
// Payloads retain their JSON representation, including numeric precision.
type ProfileFrame = bitwire.ProfileFrame

// ReturnAddress is a local address with stable pointer identity, even when its
// Wire implementation is not comparable. It is never an envelope member.
type ReturnAddress = bitwire.ReturnAddress

// Message preserves a frame and its local return capability through routing.
type Message = bitwire.Message

// Receiver receives deliveries relative to its wire's origin, and an ending.
// A root owns asynchronous dispatch; composition does not invoke Message itself.
type Receiver = bitwire.Receiver

// Wire grants send access without receive attachment or lifecycle control.
type Wire = bitwire.Wire

// Endpoint owns one receive attachment and its lifecycle. Path dispatch and
// sharing among selected receiving views belong to an explicit dispatcher.
type Endpoint = bitwire.Endpoint

var (
	ErrPath           = errors.New("invalid wire path")
	ErrNoRoute        = errors.New("wire path has no destination")
	ErrReceiverExists = errors.New("endpoint already has a receiver")
)

// EncodePath concatenates UTF-8 byte-length-prefixed scalar-string segments.
// The empty path is "", while a single empty segment is "0:".
func EncodePath(path []string) (string, error) {
	var encoded strings.Builder
	for _, segment := range path {
		if !utf8.ValidString(segment) {
			return "", ErrPath
		}
		encoded.WriteString(strconv.Itoa(len(segment)))
		encoded.WriteByte(':')
		encoded.WriteString(segment)
	}
	return encoded.String(), nil
}

// DecodePath accepts only the canonical form of EncodePath, without Unicode
// normalization or interpretation of dots, slashes or empty segments.
func DecodePath(encoded string) ([]string, error) {
	path := []string{}
	for encoded != "" {
		colon := strings.IndexByte(encoded, ':')
		if colon <= 0 {
			return nil, ErrPath
		}
		digits := encoded[:colon]
		if len(digits) > 1 && digits[0] == '0' {
			return nil, ErrPath
		}
		for _, digit := range digits {
			if digit < '0' || digit > '9' {
				return nil, ErrPath
			}
		}
		length, err := strconv.ParseUint(digits, 10, 64)
		encoded = encoded[colon+1:]
		if err != nil || length > uint64(len(encoded)) {
			return nil, ErrPath
		}
		segment := encoded[:int(length)]
		if !utf8.ValidString(segment) {
			return nil, ErrPath
		}
		path = append(path, segment)
		encoded = encoded[int(length):]
	}
	return path, nil
}

type selectedWire struct {
	root   Wire
	prefix []string
}

// At selects a relative path without allocating a peer, channel or queue.
// The selection grants only send access, even when path is empty.
func At(root Wire, path []string) Wire {
	return &selectedWire{root: root, prefix: append([]string{}, path...)}
}

func (w *selectedWire) path(path []string) []string {
	return append(append([]string{}, w.prefix...), path...)
}
func (w *selectedWire) Send(path []string, message Message) error {
	return w.root.Send(w.path(path), message)
}

type mountedWire struct {
	children map[string]Endpoint
	mu       sync.Mutex
	closed   bool
	current  *mountedReceiver
}
type mountedReceiver struct {
	receiver  Receiver
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
// Closing a mount detaches those attachments and leaves every child usable.
func Mount(children map[string]Endpoint) Endpoint {
	w := &mountedWire{children: make(map[string]Endpoint, len(children))}
	for key, child := range children {
		w.children[key] = child
	}
	return w
}

func (w *mountedWire) destination(path []string) (Endpoint, error) {
	if w.closed {
		return nil, ErrClosed
	}
	if len(path) == 0 {
		return nil, ErrNoRoute
	}
	if _, err := EncodePath(path); err != nil {
		return nil, err
	}
	child := w.children[path[0]]
	if child == nil {
		return nil, ErrNoRoute
	}
	return child, nil
}
func (w *mountedWire) Send(path []string, message Message) error {
	w.mu.Lock()
	child, err := w.destination(path)
	w.mu.Unlock()
	if err != nil {
		return err
	}
	return child.Send(append([]string{}, path[1:]...), message)
}
func (w *mountedWire) Receive(receiver Receiver) (func(), error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, ErrClosed
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
			return nil, ErrClosed
		}
		detach, err := w.children[key].Receive(Receiver{
			Message: func(path []string, message Message) {
				// The child owns capture of accepted invocations. A retained
				// delivery, including cancellation, keeps its original receiver.
				if receiver.Message != nil {
					receiver.Message(append([]string{key}, path...), message)
				}
			},
			Closed: func(code Code, reason string) { w.childEnded(attachment, slot, code, reason) },
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
			return nil, ErrClosed
		}
	}
	w.mu.Lock()
	active := attachment.active
	w.mu.Unlock()
	if !active {
		return nil, ErrClosed
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

func (w *mountedWire) childEnded(attachment *mountedReceiver, child *mountedChild, code Code, reason string) {
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

func (w *mountedWire) Close(code Code, reason string) error {
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
