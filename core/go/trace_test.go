package core_test

// Ported from Nightseam v0.6.0 runtime/go/trace_test.go (commit 5cc9723a).
// Handlers are attached through the peer's root Endpoint (research R20), so a
// method or event name on the wire is the canonical encoding of its path:
// "far" travels as "3:far", "outer" as "5:outer", and so on.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/coder/websocket"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	transports "github.com/Bitspark/bitruntime/transports/go"
)

// The trace a test sends and expects to see continued: one traceparent of the
// W3C form and a tracestate the runtime never reads, only forwards.
var carried = core.Trace{Parent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", State: "congo=t61rcWkgMzE"}

const carriedMembers = `"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","tracestate":"congo=t61rcWkgMzE"`

func frameMember(t *testing.T, members map[string]json.RawMessage, name string) string {
	t.Helper()
	raw, present := members[name]
	if !present {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("member %s = %s: %v", name, raw, err)
	}
	return value
}

// frameTrace is what one frame carries, read off the wire rather than from the
// peer that wrote it.
func frameTrace(t *testing.T, members map[string]json.RawMessage) core.Trace {
	t.Helper()
	return core.Trace{Parent: frameMember(t, members, "traceparent"), State: frameMember(t, members, "tracestate")}
}

// childOf holds a trace to what a child of parent is: the same trace id and
// flags, a span id of its own, and the tracestate it inherited verbatim.
func childOf(t *testing.T, parent, child core.Trace) {
	t.Helper()
	if len(child.Parent) != 55 {
		t.Fatalf("child traceparent %q is not of the W3C form", child.Parent)
	}
	if child.Parent[:36] != parent.Parent[:36] || child.Parent[52:] != parent.Parent[52:] {
		t.Fatalf("child %q is not of the trace %q", child.Parent, parent.Parent)
	}
	if child.Parent[36:52] == parent.Parent[36:52] {
		t.Fatalf("child %q reuses its parent's span id", child.Parent)
	}
	for _, c := range child.Parent[36:52] {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			t.Fatalf("child span id in %q is not lower-case hexadecimal", child.Parent)
		}
	}
	if child.State != parent.State {
		t.Fatalf("child tracestate = %q, want %q verbatim", child.State, parent.State)
	}
}

// TestOutgoingFramesContinueTheContextTrace: a request sent from a context
// carrying a trace carries a child of it, and the cancellation that follows
// carries that request's members rather than a sibling span of them.
func TestOutgoingFramesContinueTheContextTrace(t *testing.T) {
	peer, conn, ctx := rawPeer(t, engine.Options{})
	traced, cancel := context.WithCancel(core.DefaultPropagator.Extract(context.Background(), carried))
	t.Cleanup(cancel)
	returned := make(chan error, 1)
	go func() { returned <- dispatch.Call(traced, peer.Wire(), []string{"far"}, nil, nil) }()

	request := readFrame(ctx, t, conn)
	sent := frameTrace(t, request)
	childOf(t, carried, sent)
	cancel()
	cancellation := readFrame(ctx, t, conn)
	if string(cancellation["kind"]) != `"cancel"` || string(cancellation["id"]) != string(request["id"]) {
		t.Fatalf("frame after the cancelled call = %v", cancellation)
	}
	if got := frameTrace(t, cancellation); got != sent {
		t.Fatalf("cancel carried %+v, not its request's %+v", got, sent)
	}
	receive(t, returned)
}

// TestARequestFromABareContextCarriesANewTrace: nothing correlates two calls
// made from a context that carries no trace, and each is nonetheless traced —
// a peer with no propagator configured still says where its frames came from.
func TestARequestFromABareContextCarriesANewTrace(t *testing.T) {
	peer, conn, ctx := rawPeer(t, engine.Options{})
	traces := make([]core.Trace, 0, 2)
	for range 2 {
		go func() { _ = dispatch.Call(context.Background(), peer.Wire(), []string{"far"}, nil, nil) }()
		trace := frameTrace(t, readFrame(ctx, t, conn))
		if len(trace.Parent) != 55 || trace.Parent[:3] != "00-" || trace.Parent[52:] != "-01" {
			t.Fatalf("a new trace = %q, not a sampled traceparent of version 00", trace.Parent)
		}
		if trace.State != "" {
			t.Fatalf("a new trace carries the tracestate %q of nothing", trace.State)
		}
		traces = append(traces, trace)
	}
	if traces[0].Parent[3:35] == traces[1].Parent[3:35] {
		t.Fatalf("two calls from bare contexts share the trace id in %q", traces[0].Parent)
	}
}

// TestAHandlerRunsUnderItsRequestsTrace: the trace of an incoming request is in
// the context its handler runs under and readable there; what the handler sends
// is a child of it; the response carries the request's members byte for byte.
// An incoming event's trace reaches its handler the same way.
func TestAHandlerRunsUnderItsRequestsTrace(t *testing.T) {
	handlerTraces := make(chan core.Trace, 2)
	peer, conn, ctx := rawPeer(t, engine.Options{Prepare: serving(func(peer *engine.Peer) map[string]dispatch.Handlers {
		return map[string]dispatch.Handlers{
			"progress": {Event: func(ctx context.Context, _ json.RawMessage) error {
				trace, _ := core.TraceOf(ctx)
				handlerTraces <- trace
				return nil
			}},
			"outer": {Request: func(ctx context.Context, _ json.RawMessage) (any, error) {
				trace, found := core.TraceOf(ctx)
				if !found {
					return nil, transports.ErrClosed
				}
				handlerTraces <- trace
				if err := dispatch.Emit(ctx, peer.Wire(), []string{"progress"}, 1); err != nil {
					return nil, err
				}
				var answer string
				if err := dispatch.Call(ctx, peer.Wire(), []string{"reverse"}, nil, &answer); err != nil {
					return nil, err
				}
				return answer, nil
			}},
		}
	})})
	write := func(data string) {
		t.Helper()
		if err := conn.Write(ctx, websocket.MessageText, []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"version":1,"kind":"request","id":"c:1","method":"5:outer","params":{},` + carriedMembers + `}`)
	if got := receive(t, handlerTraces); got != carried {
		t.Fatalf("the handler's context carried %+v, not the request's %+v", got, carried)
	}
	event := readFrame(ctx, t, conn)
	if string(event["event"]) != `"8:progress"` {
		t.Fatalf("frame after the traced request = %v", event)
	}
	emitted := frameTrace(t, event)
	childOf(t, carried, emitted)
	reverse := readFrame(ctx, t, conn)
	if string(reverse["method"]) != `"7:reverse"` {
		t.Fatalf("frame after the emitted event = %v", reverse)
	}
	called := frameTrace(t, reverse)
	childOf(t, carried, called)
	if called.Parent == emitted.Parent {
		t.Fatalf("two frames of one handler share the span id in %q", called.Parent)
	}
	write(`{"version":1,"kind":"response","id":` + string(reverse["id"]) + `,"result":"back"}`)
	response := readFrame(ctx, t, conn)
	if string(response["id"]) != `"c:1"` || string(response["result"]) != `"back"` {
		t.Fatalf("response to the traced request = %v", response)
	}
	if got := frameTrace(t, response); got != carried {
		t.Fatalf("the response carried %+v, not its request's %+v", got, carried)
	}
	write(`{"version":1,"kind":"event","event":"8:progress","data":1,` + carriedMembers + `}`)
	if got := receive(t, handlerTraces); got != carried {
		t.Fatalf("the event handler's context carried %+v, not the event's %+v", got, carried)
	}
	if peer.Err() != nil {
		t.Fatalf("a traced exchange closed the connection: %v", peer.Err())
	}
}

// recordingPropagator is a propagator of another making: it records what it is
// asked to extract and dictates what every outgoing frame carries.
type recordingPropagator struct {
	extracted chan core.Trace
	injects   core.Trace
}

func (p *recordingPropagator) Extract(ctx context.Context, trace core.Trace) context.Context {
	p.extracted <- trace
	return ctx
}

func (p *recordingPropagator) Inject(context.Context) core.Trace { return p.injects }

// TestACustomPropagatorSeesTheMembersVerbatim: the peer neither reads nor
// rewrites what a configured propagator is given or returns — the incoming
// members reach Extract as they arrived, and what Inject returns is what the
// outgoing frame carries. Only a response keeps its request's own members.
//
// An outgoing frame's trace is minted where the frame is: v0.6.0's raw
// Peer.Call injected with the peer's Propagator, while a call through the
// root Endpoint injects with its own CallOptions.Propagator and the root sends
// what it returns verbatim, as v0.6.0's root did. The peer's Propagator is the
// incoming side's.
func TestACustomPropagatorSeesTheMembersVerbatim(t *testing.T) {
	propagator := &recordingPropagator{
		extracted: make(chan core.Trace, 2),
		injects:   core.Trace{Parent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00", State: "rojo=00f067aa0ba902b7"},
	}
	peer, conn, ctx := rawPeer(t, engine.Options{
		Propagator: propagator,
		Prepare: serving(func(peer *engine.Peer) map[string]dispatch.Handlers {
			return map[string]dispatch.Handlers{"outer": {Request: func(ctx context.Context, _ json.RawMessage) (any, error) {
				var answer string
				return answer, dispatch.Call(ctx, peer.Wire(), []string{"reverse"}, nil, &answer, dispatch.CallOptions{Propagator: propagator})
			}}}
		}),
	})
	write := func(data string) {
		t.Helper()
		if err := conn.Write(ctx, websocket.MessageText, []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"version":1,"kind":"request","id":"c:1","method":"5:outer","params":{},` + carriedMembers + `}`)
	if got := receive(t, propagator.extracted); got != carried {
		t.Fatalf("Extract saw %+v, not the members %+v the frame carried", got, carried)
	}
	reverse := readFrame(ctx, t, conn)
	if got := frameTrace(t, reverse); got != propagator.injects {
		t.Fatalf("the outgoing request carried %+v, not the %+v Inject returned", got, propagator.injects)
	}
	write(`{"version":1,"kind":"response","id":` + string(reverse["id"]) + `,"result":"back"}`)
	if got := frameTrace(t, readFrame(ctx, t, conn)); got != carried {
		t.Fatalf("the response carried %+v, not its request's %+v", got, carried)
	}
	if peer.Err() != nil {
		t.Fatalf("a custom propagator closed the connection: %v", peer.Err())
	}
}
