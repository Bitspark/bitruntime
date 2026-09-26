package core

import (
	"context"

	"github.com/Bitspark/bitruntime/internal/delivery/go"
)

// Meta is what a frame carries about a call rather than of it: a flat map of
// strings — a tenant, an idempotency key, a credential that is per request —
// which the profile carries verbatim and reads nothing into.
type Meta = map[string]string

// A carriage travels one way at a time. The meta a frame arrived with and the
// meta the next frame sent from this context will carry are separate values,
// so that a handler's outgoing call carries the caller's credential only where
// the handler said to: WithMeta(ctx, MetaFrom(ctx)) is how a handler forwards
// what it received, and nothing forwards it silently.

// WithMeta says what the requests and events sent from ctx carry. The map is
// copied, so a later write to the caller's does not reach a frame already
// sent; a nil or empty map carries nothing. Keys under the reserved prefix are
// the protocol's and are dropped rather than sent, since the peer at the far
// end refuses a frame carrying one.
func WithMeta(ctx context.Context, meta Meta) context.Context {
	return delivery.WithOutgoingMeta(ctx, meta)
}

// MetaFrom is the meta of the frame whose handler ctx runs under, and nil
// where the frame carried none or ctx is no handler's. The map is a copy: a
// handler may read it, and what it writes reaches no frame.
func MetaFrom(ctx context.Context) Meta { return delivery.IncomingMeta(ctx) }
