package core

import (
	"context"
	"encoding/json"
	"errors"

	transports "github.com/Bitspark/bitruntime/transports/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// ErrBackpressure is why a carrier ended when a bounded queue stayed full: a
// consumer that does not drain is disconnected rather than allowed to hold the
// carrier up. A carrier that ended this way is closed, so errors.Is(err,
// transports.ErrClosed) also holds for the error it reports.
var ErrBackpressure = errors.New("bitruntime: consumer is stalled")

// PublicError is safe to send to the remote caller. Other handler errors are
// replaced by a generic internal error; their messages are not disclosed.
type PublicError struct {
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Error is the code and the message, as a log line shows them.
func (e *PublicError) Error() string { return e.Code + ": " + e.Message }

// Ended classifies the terminal cause of a carrier as a closed carrier while
// keeping the cause itself: errors.Is(Ended(cause), transports.ErrClosed)
// holds, and so does errors.Is(Ended(cause), cause).
func Ended(cause error) error {
	if cause == nil || errors.Is(cause, transports.ErrClosed) {
		if cause == nil {
			return transports.ErrClosed
		}
		return cause
	}
	return &endedError{cause: cause}
}

type endedError struct{ cause error }

func (e *endedError) Error() string { return "bitruntime: closed: " + e.cause.Error() }
func (e *endedError) Unwrap() []error {
	return []error{transports.ErrClosed, e.cause}
}

// Respond answers a request through its return capability, normalizing err to
// the public error the protocol carries: a PublicError as it is, a
// cancellation as cancelled, a closed carrier as disconnected, and anything
// else as internal. An oversized or unencodable result is answered with a
// bounded internal error instead, so the caller is never left waiting.
//
// It returns the outcome it reported: nil for a delivered success, the
// normalized refusal, or the return capability's own refusal.
func Respond(request wire.Message, result json.RawMessage, err error) error {
	if request.Return == nil || request.Return.Wire == nil {
		return transports.ErrClosed
	}
	f := wire.ProfileFrame{Version: 1, Kind: wire.ProfileResponse, ID: request.Frame.ID, Result: result, Traceparent: request.Frame.Traceparent, Tracestate: request.Frame.Tracestate}
	if err != nil {
		var public *PublicError
		switch {
		case errors.As(err, &public) && public != nil && public.Code != "" && public.Message != "":
			f.Error = &wire.ProfileError{Code: public.Code, Message: public.Message, Data: public.Data}
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			f.Error = &wire.ProfileError{Code: "cancelled", Message: "Request cancelled"}
		case errors.Is(err, transports.ErrClosed):
			f.Error = &wire.ProfileError{Code: "disconnected", Message: "Connection ended; outcome may be unknown"}
		default:
			f.Error = &wire.ProfileError{Code: "internal", Message: "Internal error"}
		}
		f.Result = nil
		// Preserve the local cancellation cause, but otherwise observe exactly
		// the normalized public error selected for this response.
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			err = &PublicError{Code: f.Error.Code, Message: f.Error.Message, Data: f.Error.Data}
		}
	}
	if sendErr := request.Return.Wire.Send(nil, wire.Message{Frame: f}); sendErr != nil {
		// A malformed or oversized public result must settle as a bounded
		// refusal, just as the protocol engine's own response does.
		f.Result = nil
		f.Error = &wire.ProfileError{Code: "internal", Message: "Response could not be encoded"}
		if fallbackErr := request.Return.Wire.Send(nil, wire.Message{Frame: f}); fallbackErr == nil {
			return &PublicError{Code: f.Error.Code, Message: f.Error.Message}
		}
		// A caller that already withdrew cannot receive either response. Its
		// selected refusal remains that refusal; a failed success is no success.
		if err == nil {
			return WithoutUnpublishedProof(sendErr)
		}
	}
	return err
}
