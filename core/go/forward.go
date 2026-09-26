package core

import (
	"errors"
	"sync"

	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// Forward joins two existing origins without allocating a peer or channel:
// what either endpoint delivers is sent through the other unchanged, with its
// return capability and context, in the order the source delivers it. The
// returned detach removes only the forwarding attachments; both endpoints stay
// owned by their callers, and each root remains responsible for ending its
// failed carrier.
//
// A message the destination refuses fails only that message: a refused
// request is answered through its return capability, and forwarding goes on.
// Forwarding ends when either endpoint ends or detach is called.
func Forward(inbound, outbound wire.Endpoint) (func(), error) {
	if inbound == nil || outbound == nil {
		return nil, errors.New("bitruntime: forwarding requires two origins")
	}
	var mu sync.Mutex
	var detaches []func()
	ended := false
	stop := func() {
		mu.Lock()
		if ended {
			mu.Unlock()
			return
		}
		ended = true
		owned := detaches
		detaches = nil
		mu.Unlock()
		for _, detach := range owned {
			detach()
		}
	}
	receiver := func(destination wire.AddressedWire) wire.Receiver {
		return wire.Receiver{Closed: func(wire.Code, string) { stop() }, Message: func(path []string, message wire.Message) {
			if err := destination.Send(path, message); err != nil && message.Frame.Kind == wire.ProfileRequest {
				Respond(message, nil, WithoutUnpublishedProof(err))
			}
		}}
	}
	for _, direction := range []struct{ source, destination wire.Endpoint }{{inbound, outbound}, {outbound, inbound}} {
		detach, err := direction.source.Receive(receiver(direction.destination))
		if err != nil {
			stop()
			return nil, err
		}
		mu.Lock()
		active := !ended
		if active {
			detaches = append(detaches, detach)
		}
		mu.Unlock()
		if !active {
			detach()
			return nil, transports.ErrClosed
		}
	}
	return stop, nil
}
