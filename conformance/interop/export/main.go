// Command export is the Go side of the cross-language export interoperability
// check (scripts/export-interop.mjs). Test-only.
//
//	export serve        listen; export a Wire, send its reference at ["r"], report
//	                    "got <hex>" for each delivered send and "released" when no
//	                    export remains; exit when stdin closes
//	export client <url> dial; import the reference received at ["r"], send "hello"
//	                    through it, release it, print "ok"
package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	export "github.com/Bitspark/bitruntime/export/go"
	websocket "github.com/Bitspark/bitruntime/websocket/go"
	ontos "github.com/Bitspark/bitwire/ontos/go/core"
	wire "github.com/Bitspark/bitwire/wire/go"
)

var (
	routeSeg = ontos.NewAtom([]byte("r"))
	rootSeg  = ontos.NewAtom([]byte("x"))
	root     = wire.Path{rootSeg}
)

type wireFunc func(ontos.Value) error

func (f wireFunc) Send(v ontos.Value) error { return f(v) }

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		fail(fmt.Errorf("usage: export serve | client <url>"))
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

func serve() {
	out := make(chan string, 16)
	server, err := websocket.Listen("127.0.0.1:0", websocket.Options{}, func(e *websocket.Endpoint, _ *http.Request) {
		ep := core.Addressed(e)
		table, err := export.NewTable(8)
		if err != nil {
			fail(err)
		}
		if _, err := ep.Receive(func(p wire.Path, v ontos.Value) {
			if len(p) > 0 && p[0].Equal(rootSeg) {
				table.Deliver(p[1:], v)
				if table.Live() == 0 {
					out <- "released"
				}
			}
		}); err != nil {
			fail(err)
		}
		ref, err := table.Export(wireFunc(func(v ontos.Value) error {
			encoded, err := wire.EncodeMessage(v, wire.DefaultMaxMessageBytes)
			if err != nil {
				return err
			}
			out <- "got " + hex.EncodeToString(encoded)
			return nil
		}))
		if err != nil {
			fail(err)
		}
		if err := ep.Send(wire.Path{routeSeg}, ref); err != nil {
			fail(err)
		}
	})
	if err != nil {
		fail(err)
	}
	fmt.Println(server.URL())
	stdin := make(chan struct{})
	go func() { _, _ = bufio.NewReader(os.Stdin).ReadString(0); close(stdin) }()
	for {
		select {
		case line := <-out:
			fmt.Println(line)
		case <-stdin:
			_ = server.Close()
			return
		}
	}
}

func client(url string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	e, err := websocket.Dial(ctx, url, websocket.Options{})
	if err != nil {
		fail(err)
	}
	ep := core.Addressed(e)
	importer := export.NewImporter(ep, root, nil)
	refs := make(chan ontos.Value, 1)
	if _, err := ep.Receive(func(p wire.Path, v ontos.Value) {
		if len(p) == 1 && p[0].Equal(routeSeg) {
			refs <- v
		}
	}); err != nil {
		fail(err)
	}
	var ref ontos.Value
	select {
	case ref = <-refs:
	case <-time.After(10 * time.Second):
		fail(fmt.Errorf("no reference received"))
	}
	imported, err := importer.Import(ref)
	if err != nil {
		fail(err)
	}
	release := imported.Hold()
	if err := imported.Wire().Send(ontos.NewAtom([]byte("hello"))); err != nil {
		fail(err)
	}
	release()
	time.Sleep(200 * time.Millisecond) // let the release leave before closing
	_ = e.Close()
	fmt.Println("ok")
}
