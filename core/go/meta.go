package runtime

import (
	"context"
	"encoding/json"
	"maps"
	"strings"
)

// metaReserved prefixes the meta keys the profile and its components keep for
// themselves — a deadline, a cause — so that a consumer's key and one defined
// later never collide. This version defines none, so every key under it is
// refused, on the way out as on the way in.
const metaReserved = "nightseam."

// Meta is what a frame carries about a call rather than of it: a flat map of
// strings — a tenant, an idempotency key, a credential that is per request —
// which the profile carries verbatim and reads nothing into.
type Meta = map[string]string

// A carriage travels one way at a time. The meta a frame arrived with and the
// meta the next frame sent from this context will carry are separate values
// under separate keys, so that a handler's outgoing call carries the caller's
// credential only where the handler said to: WithMeta(ctx, MetaFrom(ctx)) is
// how a handler forwards what it received, and nothing forwards it silently.
type outgoingMetaKey struct{}
type incomingMetaKey struct{}

// WithMeta says what the requests and events sent from ctx carry. The map is
// copied, so a later write to the caller's does not reach a frame already
// sent; a nil or empty map carries nothing. Keys under the reserved prefix are
// the profile's and are dropped rather than sent, since the peer at the far
// end refuses a frame carrying one.
func WithMeta(ctx context.Context, meta Meta) context.Context {
	carried := make(Meta, len(meta))
	for key, value := range meta {
		if !strings.HasPrefix(key, metaReserved) {
			carried[key] = value
		}
	}
	if len(carried) == 0 {
		return context.WithValue(ctx, outgoingMetaKey{}, Meta(nil))
	}
	return context.WithValue(ctx, outgoingMetaKey{}, carried)
}

// MetaFrom is the meta of the frame whose handler ctx runs under, and nil
// where the frame carried none or ctx is no handler's. The map is a copy: a
// handler may read it, and what it writes reaches no frame.
func MetaFrom(ctx context.Context) Meta {
	meta, _ := ctx.Value(incomingMetaKey{}).(Meta)
	if len(meta) == 0 {
		return nil
	}
	return maps.Clone(meta)
}

// withIncomingMeta places what a frame carried on the context its handler runs
// under. An absent member and an empty carriage are alike to a handler, which
// reads nil for both.
func withIncomingMeta(ctx context.Context, meta Meta) context.Context {
	if len(meta) == 0 {
		return ctx
	}
	return context.WithValue(ctx, incomingMetaKey{}, meta)
}

// outgoingMeta is what a frame sent from ctx carries, and nil where nothing
// said. It is read once per frame, so that a context changed between two calls
// is obeyed by each.
func outgoingMeta(ctx context.Context) Meta {
	meta, _ := ctx.Value(outgoingMetaKey{}).(Meta)
	if len(meta) == 0 {
		return nil
	}
	return meta
}

// validMeta holds meta to what a carriage is, reading the member as it was
// spelled rather than as the frame holds it: an object, since a member spelled
// null is not an absent one; every value a string, which map[string]string
// cannot tell from a null it would read as the empty one; and no key of the
// reserved prefix.
func validMeta(raw json.RawMessage) bool {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil || values == nil {
		return false
	}
	for key, value := range values {
		if strings.HasPrefix(key, metaReserved) || len(value) == 0 || value[0] != '"' {
			return false
		}
	}
	return true
}
