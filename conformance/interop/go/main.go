// Raw and addressed peers for cross-language and fresh-consumer observations.
package main

import (
	"bufio"
	"context"
	"fmt"
	core "github.com/Bitspark/bitruntime/core/go"
	ws "github.com/Bitspark/bitruntime/websocket/go"
	ontos "github.com/Bitspark/bitwire/ontos/go/core"
	wire "github.com/Bitspark/bitwire/wire/go"
	"net/http"
	"os"
	"time"
)

func sample() ontos.Value {
	return ontos.NewTuple(ontos.NewAtom([]byte("unknown.embedding")), ontos.NewAtom([]byte{255, 0, 128}), ontos.NewTuple())
}
func path() wire.Path {
	return wire.Path{ontos.NewAtom(nil), ontos.NewAtom([]byte{97, 47, 98}), ontos.NewAtom([]byte{255})}
}
func send(e wire.Endpoint, mode string, p wire.Path, v ontos.Value) error {
	if mode == "addressed" {
		return core.Addressed(e).Send(p, v)
	}
	return e.Send(v)
}
func receive(e wire.Endpoint, mode string, f func(wire.Path, ontos.Value)) {
	var err error
	if mode == "addressed" {
		_, err = core.Addressed(e).Receive(f)
	} else {
		_, err = e.Receive(func(v ontos.Value) { f(nil, v) })
	}
	if err != nil {
		panic(err)
	}
}
func echo(mode string) func(*ws.Endpoint, *http.Request) {
	return func(e *ws.Endpoint, _ *http.Request) {
		receive(e, mode, func(p wire.Path, v ontos.Value) {
			if err := send(e, mode, p, v); err != nil {
				panic(err)
			}
		})
		if err := send(e, mode, nil, ontos.NewAtom([]byte{255})); err != nil {
			panic(err)
		}
	}
}
func client(address, mode string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	e, err := ws.Dial(ctx, address, ws.Options{})
	if err != nil {
		panic(err)
	}
	defer e.Close()
	type observation struct {
		p wire.Path
		v ontos.Value
	}
	got := make(chan observation, 4)
	receive(e, mode, func(p wire.Path, v ontos.Value) { got <- observation{p, v} })
	if err = send(e, mode, path(), sample()); err != nil {
		panic(err)
	}
	greeting := false
	response := false
	for !greeting || !response {
		select {
		case x := <-got:
			if x.v.Equal(ontos.NewAtom([]byte{255})) {
				if len(x.p) != 0 {
					panic("greeting path changed")
				}
				greeting = true
				continue
			}
			if !x.v.Equal(sample()) {
				panic("opaque message changed")
			}
			if mode == "addressed" {
				if len(x.p) != len(path()) {
					panic("path length changed")
				}
				for i, k := range path() {
					if !x.p[i].Equal(k) {
						panic("path bytes changed")
					}
				}
			}
			response = true
		case <-ctx.Done():
			panic("peer observation timed out")
		}
	}
	fmt.Println("ok")
}
func main() {
	if len(os.Args) > 1 && os.Args[1] == "client" {
		client(os.Args[2], os.Args[3])
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "smoke" {
		for _, mode := range []string{"raw", "addressed"} {
			a, b, err := core.NewPair(core.PairOptions{})
			if err != nil {
				panic(err)
			}
			got := make(chan ontos.Value, 1)
			receive(b, mode, func(_ wire.Path, v ontos.Value) { got <- v })
			if err = send(a, mode, path(), sample()); err != nil {
				panic(err)
			}
			select {
			case v := <-got:
				if !v.Equal(sample()) {
					panic("local message changed")
				}
			case <-time.After(10 * time.Second):
				panic("local observation timed out")
			}
			a.Close()
			s, err := ws.Listen("", ws.Options{}, echo(mode))
			if err != nil {
				panic(err)
			}
			client(s.URL(), mode)
			s.Close()
		}
		return
	}
	mode := "raw"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	s, err := ws.Listen("", ws.Options{}, echo(mode))
	if err != nil {
		panic(err)
	}
	defer s.Close()
	fmt.Println(s.URL())
	bufio.NewScanner(os.Stdin).Scan()
}
