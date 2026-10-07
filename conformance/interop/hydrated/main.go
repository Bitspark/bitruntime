// Command hydrated is the Go peer of the hydrated interoperability check
// (scripts/hydrated-interop.mjs), evidence for bitwire decision 0019. Test-only.
//
//	hydrated serve        listen at participant [s]; on connection, expose an echo
//	                      endpoint and send its reference payload at ["bootstrap"];
//	                      print the URL; exit when stdin closes
//	hydrated client <url> participant [c]: call "continue" with a reply HydratedWire, send a
//	                      third HydratedWire through the returned continuation, print "ok"
//	                      when the third HydratedWire receives the value
//
// The echo: (op, arg, reply) replies (arg, continuation); the continuation sends
// the value it receives to the HydratedWire it carries, then closes.
package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	hydrated "github.com/Bitspark/bitruntime/hydrated/go"
	websocket "github.com/Bitspark/bitruntime/websocket/go"
	ontos "github.com/Bitspark/bitwire/ontos/go/core"
	wire "github.com/Bitspark/bitwire/wire/go"
)

var (
	ns        = hydrated.NewNamespace("interop")
	bootstrap = wire.Path{ontos.NewAtom([]byte("bootstrap"))}
)

func text(s string) ontos.Atom { return ontos.NewAtom([]byte(s)) }

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

// participant composes a scope at path over an addressed link; bootstrap values
// go to onBootstrap, everything else to the scope with the link as context.
func participant(link wire.AddressedEndpoint, path wire.Path, onBootstrap func(ontos.Value)) *hydrated.Scope {
	scope, err := hydrated.NewScope(ns, path, link, hydrated.DefaultLimits)
	if err != nil {
		fail(err)
	}
	if _, err := link.Receive(func(p wire.Path, v ontos.Value) {
		if wire.PathEqual(p, bootstrap) {
			onBootstrap(v)
			return
		}
		if err := scope.Deliver(p, v, "link"); err != nil {
			fmt.Fprintln(os.Stderr, "diagnostic:", err)
		}
	}); err != nil {
		fail(err)
	}
	return scope
}

func echo(v wire.HydratedValue, _ wire.ReceivedContext) {
	xs, _ := hydrated.Items(v)
	cont := hydrated.NewEndpoint(1)
	_, _ = cont.Receive(func(next wire.HydratedValue, _ wire.ReceivedContext) {
		ys, _ := hydrated.Items(next)
		_ = ys[1].(wire.HydratedWire).Send(ys[0])
		_ = cont.Close()
	})
	r, _ := hydrated.NewTuple(xs[1], cont)
	_ = xs[2].(wire.HydratedWire).Send(r)
}

func serve() {
	server, err := websocket.Listen("127.0.0.1:0", websocket.Options{}, func(e *websocket.Endpoint, _ *http.Request) {
		link := core.Addressed(e)
		scope := participant(link, wire.Path{text("s")}, func(ontos.Value) {})
		service := hydrated.NewEndpoint(16)
		_, _ = service.Receive(echo)
		ref, err := scope.Expose(service)
		if err != nil {
			fail(err)
		}
		payload, err := ref.Payload()
		if err != nil {
			fail(err)
		}
		if err := link.Send(bootstrap, payload); err != nil {
			fail(err)
		}
	})
	if err != nil {
		fail(err)
	}
	fmt.Println(server.URL())
	_, _ = bufio.NewReader(os.Stdin).ReadString(0)
	_ = server.Close()
}

func client(url string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	e, err := websocket.Dial(ctx, url, websocket.Options{})
	if err != nil {
		fail(err)
	}
	refs := make(chan ontos.Value, 1)
	scope := participant(core.Addressed(e), wire.Path{text("c")}, func(v ontos.Value) { refs <- v })
	var payload ontos.Value
	select {
	case payload = <-refs:
	case <-time.After(10 * time.Second):
		fail(fmt.Errorf("no bootstrap reference"))
	}
	ref, err := hydrated.ReadReference(payload)
	if err != nil {
		fail(err)
	}
	reply := hydrated.NewEndpoint(1)
	replies := make(chan wire.HydratedValue, 1)
	_, _ = reply.Receive(func(v wire.HydratedValue, _ wire.ReceivedContext) { replies <- v })
	req, _ := hydrated.NewTuple(text("continue"), text("hello"), reply)
	if err := scope.Connect(ref).Send(req); err != nil {
		fail(err)
	}
	var r wire.HydratedValue
	select {
	case r = <-replies:
	case <-time.After(10 * time.Second):
		fail(fmt.Errorf("no reply"))
	}
	xs, _ := hydrated.Items(r)
	if !xs[0].(ontos.Atom).Equal(text("hello")) {
		fail(fmt.Errorf("wrong reply"))
	}
	third := hydrated.NewEndpoint(1)
	thirds := make(chan wire.HydratedValue, 1)
	_, _ = third.Receive(func(v wire.HydratedValue, _ wire.ReceivedContext) { thirds <- v })
	next, _ := hydrated.NewTuple(text("third HydratedWire"), third)
	if err := xs[1].(wire.HydratedWire).Send(next); err != nil {
		fail(err)
	}
	select {
	case v := <-thirds:
		if !v.(ontos.Atom).Equal(text("third HydratedWire")) {
			fail(fmt.Errorf("wrong third value"))
		}
	case <-time.After(10 * time.Second):
		fail(fmt.Errorf("third HydratedWire not reached"))
	}
	fmt.Println("ok")
	_ = e.Close()
}

func main() {
	if len(os.Args) < 2 {
		fail(fmt.Errorf("usage: hydrated serve | client <url>"))
	}
	switch os.Args[1] {
	case "serve":
		serve()
	case "client":
		if len(os.Args) < 3 {
			fail(fmt.Errorf("client needs a url"))
		}
		client(os.Args[2])
	default:
		fail(fmt.Errorf("unknown mode %q", os.Args[1]))
	}
}
