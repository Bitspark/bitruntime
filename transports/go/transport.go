// Package transports is the seam beneath every carrier: a frames duplex
// connection. It is ordered, message-framed, bidirectional and closed
// explicitly with a code and a reason, and it is nothing else — no JSON, no
// requests, no correlation, no events, no reconnection. The bitwire/1 protocol
// engine runs over it; beneath it the transport is an in-memory pipe, a
// WebSocket or a framed byte stream, and neither side of the seam knows which.
//
// Framing is the transport's: a WebSocket has message boundaries of its own,
// a byte stream needs a framing of its own, and either way a frame arrives
// whole or not at all. Close semantics travel: the codes are the WebSocket
// registry's numbers on every transport, so that a policy violation or an
// oversized frame is refused the same way everywhere.
//
// This package also holds the one classification of closed errors that every
// bitruntime carrier, operator and helper reports: ErrClosed.
package transports

import (
	"context"
	"errors"
	"fmt"

	wire "github.com/Bitspark/bitwire/wire/go"
)

// Kind is what a frame carries: text, which the protocol requires to be JSON,
// or bytes.
type Kind int

const (
	// Text is a frame of UTF-8 text, what the protocol's JSON envelopes travel as.
	Text Kind = iota + 1
	// Binary is a frame of bytes, opaque to the seam.
	Binary
)

// ErrNoKind is refused by every transport for a frame whose Kind is neither
// Text nor Binary.
var ErrNoKind = errors.New("bitruntime: transport frame of no kind")

func (k Kind) String() string {
	switch k {
	case Text:
		return "text"
	case Binary:
		return "binary"
	}
	return fmt.Sprintf("kind(%d)", int(k))
}

// Frame is one message: it is sent whole and received whole, in order.
type Frame struct {
	Kind Kind
	Data []byte
}

// Code is a close code, the bitwire contract's own type. The numbers are the
// WebSocket registry's, kept on every transport so that a close means the
// same thing whatever carried it.
type Code = wire.Code

const (
	// CodeNormal is a close both sides meant.
	CodeNormal Code = 1000
	// CodeGoingAway is a side shutting down.
	CodeGoingAway Code = 1001
	// CodeProtocolError is a frame the receiver could not take as the
	// protocol above the seam defines one.
	CodeProtocolError Code = 1002
	// CodeUnsupportedData is a frame of a kind the receiver does not speak,
	// such as a binary frame where JSON text was expected.
	CodeUnsupportedData Code = 1003
	// CodeNoStatus is what a side sees when the other closed with no code:
	// never sent, only observed.
	CodeNoStatus Code = 1005
	// CodeAbnormalClosure is what a side sees when the other ended with no
	// close at all: an abort, or a dropped transport. Never sent, only observed.
	CodeAbnormalClosure Code = 1006
	// CodePolicyViolation is a frame that parses and is refused anyway.
	CodePolicyViolation Code = 1008
	// CodeTooLarge is a frame over the receiver's limit.
	CodeTooLarge Code = 1009
	// CodeInternalError is a failure of the receiver's own.
	CodeInternalError Code = 1011
	// CodeTLSHandshake is what a side sees when a TLS handshake failed. Never
	// sent, only observed.
	CodeTLSHandshake Code = 1015
)

// Application codes are the range a protocol above the seam may use for its
// own reasons; bitwire/1 closes with CodeProtocol.
const (
	CodeApplicationFirst Code = 4000
	CodeApplicationLast  Code = 4999
	CodeProtocol         Code = 4011
)

// Sendable says whether a close code may be sent. Codes that only describe
// what a side observed — no status, an abnormal closure, a failed TLS
// handshake — are never transmitted; an abort is how a side ends without one.
// Codes outside the WebSocket registry's usable ranges are not sendable either.
func Sendable(code Code) bool {
	switch {
	case code == CodeNoStatus, code == CodeAbnormalClosure, code == CodeTLSHandshake, code == 1004:
		return false
	case code >= 1000 && code <= 1014:
		return true
	case code >= 3000 && code <= 4999:
		return true
	}
	return false
}

// ErrClosed is the one classification of a closed carrier. A transport's Send
// and Receive return it once the connection was closed or aborted on this
// side; every bitruntime endpoint, operator and helper reports a closed or
// ended carrier as an error for which errors.Is(err, ErrClosed) holds, whatever
// layer noticed it.
var ErrClosed = errors.New("bitruntime: closed")

// ErrUnsendableCode refuses a Close whose code may only be observed. Nothing
// is sent and the connection stays as it was; Abort ends it without a code.
var ErrUnsendableCode = errors.New("bitruntime: close code may only be observed")

// CloseError is what Receive returns once the remote side closed: the code
// and the reason it gave, which a protocol above may act on. It is also a
// closed carrier, so errors.Is(err, ErrClosed) holds for it.
type CloseError struct {
	Code   Code
	Reason string
}

func (e *CloseError) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("connection closed by the remote side (%d)", int(e.Code))
	}
	return fmt.Sprintf("connection closed by the remote side (%d): %s", int(e.Code), e.Reason)
}

// Is classifies a remote close as a closed carrier.
func (e *CloseError) Is(target error) bool { return target == ErrClosed }

// Conn is a frames duplex connection. A transport implements it; a protocol
// uses it and nothing beneath it. Send and Receive may be called
// concurrently with each other, and each in turn from one goroutine at a
// time. Once Close or Abort was called, every Send and Receive returns
// ErrClosed; once the remote closed, Receive returns a *CloseError and Send
// fails.
type Conn interface {
	// Send writes one frame after every frame sent before it. It blocks while
	// the transport cannot take more, until ctx ends: backpressure is the
	// caller's to wait out or to give up on, never hidden in a buffer that
	// grows.
	Send(ctx context.Context, frame Frame) error
	// Receive returns the next frame in the order it was sent. A frame larger
	// than the connection's receive limit is not delivered: Receive returns
	// an error and the connection ends with CodeTooLarge, because a limit that
	// could be exceeded first is not a limit.
	Receive(ctx context.Context) (Frame, error)
	// Close ends the connection with a code and a reason the remote side
	// will see, waiting for its acknowledgement until ctx ends where the
	// transport has one. A code that is not Sendable is refused with
	// ErrUnsendableCode and nothing is sent.
	Close(ctx context.Context, code Code, reason string) error
	// Abort ends the connection at once, with no handshake and nothing sent,
	// and releases every blocked Send and Receive. The remote side observes
	// CodeAbnormalClosure. It is what a protocol does when the remote side
	// has misbehaved or the consumer has stalled.
	Abort() error
}
