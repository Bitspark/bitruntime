package runtime

import "errors"

// UnpublishedError reports a local refusal before a frame entered the peer's
// outbound queue or a local implementation dispatched. Its underlying cause
// retains the ordinary public error or cancellation identity. A queued write failure or a remote response never
// supplies this proof, even if it has the same error code or message.
//
// The proof belongs to this send attempt. A handler must not treat a nested
// call's refusal as proof that its own already-dispatched request was unsent.
type UnpublishedError struct {
	cause error
}

func (e *UnpublishedError) Error() string { return e.cause.Error() }

// Unwrap preserves errors.Is and errors.As for the original refusal.
func (e *UnpublishedError) Unwrap() error { return e.cause }

// Unpublished marks an error only at a boundary that can prove its own payload
// was neither queued nor dispatched locally. It preserves a nil error. Never
// use it to classify a received error code or an uncertain transport outcome.
func Unpublished(err error) error {
	if err == nil {
		return nil
	}
	return &UnpublishedError{cause: err}
}

// WithoutUnpublishedProof preserves the ordinary cause of an error crossing a
// dispatch boundary, but removes evidence that belongs to a nested send attempt.
// Transport adapters and local implementations can return another call's error;
// that error cannot prove that the surrounding request was never published.
func WithoutUnpublishedProof(err error) error {
	var proof *UnpublishedError
	if errors.As(err, &proof) {
		return dispatchedError{cause: err}
	}
	return err
}

type dispatchedError struct{ cause error }

func (e dispatchedError) Error() string { return e.cause.Error() }
func (e dispatchedError) Is(target error) bool {
	if _, proof := target.(*UnpublishedError); proof {
		return false
	}
	return errors.Is(e.cause, target)
}
func (e dispatchedError) As(target any) bool {
	if _, proof := target.(**UnpublishedError); proof {
		return false
	}
	return errors.As(e.cause, target)
}
