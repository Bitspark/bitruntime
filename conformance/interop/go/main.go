// A generic envelope peer used only for cross-language and fresh-consumer checks.
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

func sample() wire.Envelope {
	return wire.Envelope{Source: wire.Path{ontos.NewAtom([]byte{0, 255})}, Destination: wire.Path{ontos.NewAtom(nil), ontos.NewAtom([]byte("a/b"))}, ID: ontos.NewAtom(nil), Payload: ontos.NewTuple(ontos.NewAtom([]byte("unknown.embedding")), ontos.NewAtom([]byte{255, 0, 128}), ontos.NewTuple())}
}
func echo(e *ws.Endpoint, _ *http.Request) {
	e.Receive(func(request wire.Envelope) {
		if request.Correlation != nil {
			return
		}
		id := request.ID
		if err := e.Send(wire.Envelope{Source: request.Destination, Destination: request.Source, ID: ontos.NewAtom([]byte{255}), Correlation: &id, Payload: request.Payload}); err != nil {
			panic(err)
		}
	})
	if err := e.Send(sample()); err != nil {
		panic(err)
	}
}
func client(address string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	e, err := ws.Dial(ctx, address, ws.Options{})
	if err != nil {
		panic(err)
	}
	defer e.Close()
	got := make(chan wire.Envelope, 4)
	e.Receive(func(v wire.Envelope) { got <- v })
	request := sample()
	if err = e.Send(request); err != nil {
		panic(err)
	}
	for {
		select {
		case response := <-got:
			if response.Correlation == nil {
				continue
			}
			if !response.Correlation.Equal(request.ID) || !response.Payload.Equal(request.Payload) || len(response.Source) != 2 || !response.Source[0].Equal(request.Destination[0]) || !response.Source[1].Equal(request.Destination[1]) {
				panic("cross-language envelope changed")
			}
			fmt.Println("ok")
			return
		case <-ctx.Done():
			panic("peer observation timed out")
		}
	}
}
func main() {
	if len(os.Args) > 1 && os.Args[1] == "client" {
		client(os.Args[2])
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "smoke" {
		a, b, err := core.NewPair(core.PairOptions{})
		if err != nil {
			panic(err)
		}
		got := make(chan wire.Envelope, 1)
		b.Receive(func(e wire.Envelope) { got <- e })
		a.Send(sample())
		if !(<-got).Payload.Equal(sample().Payload) {
			panic("local pair changed payload")
		}
		a.Close()
		s, err := ws.Listen("", ws.Options{}, echo)
		if err != nil {
			panic(err)
		}
		defer s.Close()
		client(s.URL())
		return
	}
	s, err := ws.Listen("", ws.Options{}, echo)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	fmt.Println(s.URL())
	bufio.NewScanner(os.Stdin).Scan()
}
