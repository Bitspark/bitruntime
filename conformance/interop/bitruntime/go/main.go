// Command bitruntime-interop is bitruntime's Go program of the interoperability
// scenario in conformance/interop/README.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	dispatch "github.com/Bitspark/bitruntime/dispatch/go"
	engine "github.com/Bitspark/bitruntime/engine/go"
	websocket "github.com/Bitspark/bitruntime/engine/websocket/go"
	wire "github.com/Bitspark/bitwire/wire/go"
)

const name = "bitruntime-go"
const maxFrameBytes = 4 << 20

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: server <port> | client <url>")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "server":
		err = serve(os.Args[2])
	case "client":
		err = client(os.Args[2])
	default:
		err = fmt.Errorf("unknown role %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func serve(port string) error {
	var mu sync.Mutex
	sawCancel := make(chan struct{})
	handler, err := websocket.NewHandler(websocket.ServerOptions{
		Authenticate: func(r *http.Request) (context.Context, error) { return context.Background(), nil },
		CheckOrigin:  func(*http.Request) bool { return true },
		Options: engine.Options{MaxFrameBytes: maxFrameBytes, Prepare: func(peer *engine.Peer) error {
			d, err := dispatch.NewDispatcher(peer.Wire())
			if err != nil {
				return err
			}
			handle := func(path []string, h dispatch.Handler) error {
				_, err := dispatch.Handle(d, path, h)
				return err
			}
			return errors.Join(
				handle([]string{"echo"}, func(ctx context.Context, raw json.RawMessage) (any, error) { return raw, nil }),
				handle([]string{"spaces", "a/b", "echo"}, func(ctx context.Context, raw json.RawMessage) (any, error) {
					return map[string]any{"space": "a/b", "params": raw}, nil
				}),
				handle([]string{"spaces", "é", ""}, func(context.Context, json.RawMessage) (any, error) { return "unicode-empty", nil }),
				handle([]string{"fail"}, func(context.Context, json.RawMessage) (any, error) {
					return nil, &core.PublicError{Code: "bad_request", Message: "refused on purpose", Data: json.RawMessage(`{"n":1}`)}
				}),
				handle([]string{"wait"}, func(ctx context.Context, raw json.RawMessage) (any, error) {
					if err := dispatch.Emit(ctx, peer.Wire(), []string{"waiting"}, nil); err != nil {
						return nil, err
					}
					<-ctx.Done()
					mu.Lock()
					select {
					case <-sawCancel:
					default:
						close(sawCancel)
					}
					mu.Unlock()
					return nil, ctx.Err()
				}),
				handle([]string{"cancelled"}, func(ctx context.Context, raw json.RawMessage) (any, error) {
					select {
					case <-sawCancel:
						return true, nil
					case <-time.After(2 * time.Second):
						return false, nil
					}
				}),
				handle([]string{"meta"}, func(ctx context.Context, raw json.RawMessage) (any, error) {
					meta := core.MetaFrom(ctx)
					if meta == nil {
						meta = core.Meta{}
					}
					return meta, nil
				}),
				handle([]string{"reverse"}, func(ctx context.Context, raw json.RawMessage) (any, error) {
					var who string
					err := dispatch.Call(ctx, peer.Wire(), []string{"whoami"}, nil, &who)
					return who, err
				}),
				handle([]string{"big"}, func(ctx context.Context, raw json.RawMessage) (any, error) {
					var args struct{ N int }
					if err := json.Unmarshal(raw, &args); err != nil {
						return nil, err
					}
					return strings.Repeat("x", args.N), nil
				}),
				func() error {
					_, err := dispatch.Register(d, []string{"ping"}, dispatch.Handlers{Event: func(ctx context.Context, raw json.RawMessage) error {
						return dispatch.Emit(context.Background(), peer.Wire(), []string{"pong"}, map[string]json.RawMessage{"echo": raw})
					}})
					return err
				}(),
			)
		}},
	})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/wire", handler)
	fmt.Printf("LISTEN ws://%s/wire\n", listener.Addr())
	return http.Serve(listener, mux)
}

func client(url string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pongs := make(chan json.RawMessage, 1)
	peer, _, err := websocket.Dial(ctx, url, websocket.DialOptions{Options: engine.Options{MaxFrameBytes: maxFrameBytes, Prepare: func(peer *engine.Peer) error {
		d, err := dispatch.NewDispatcher(peer.Wire())
		if err != nil {
			return err
		}
		if _, err := dispatch.Handle(d, []string{"whoami"}, func(context.Context, json.RawMessage) (any, error) { return name + "-client", nil }); err != nil {
			return err
		}
		_, err = dispatch.Register(d, []string{"pong"}, dispatch.Handlers{Event: func(ctx context.Context, raw json.RawMessage) error {
			pongs <- raw
			return nil
		}})
		return err
	}}})
	if err != nil {
		return err
	}
	defer peer.Close()
	root := peer.Wire()
	observed := map[string]any{}
	call := func(path []string, params any) (json.RawMessage, error) {
		var result json.RawMessage
		err := dispatch.Call(ctx, root, path, params, &result)
		return result, err
	}
	code := func(err error) any {
		var public *core.PublicError
		if errors.As(err, &public) {
			return map[string]any{"code": public.Code, "message": public.Message, "data": public.Data}
		}
		if err != nil {
			return err.Error()
		}
		return nil
	}
	if result, err := call([]string{"echo"}, json.RawMessage(`{"a":[1,"x",null,true],"n":1e3,"u":"😀"}`)); err != nil {
		observed["echo"] = code(err)
	} else {
		observed["echo"] = result
	}
	if result, err := call([]string{"spaces", "a/b", "echo"}, map[string]int{"x": 1}); err == nil {
		observed["nested"] = result
	} else {
		observed["nested"] = code(err)
	}
	if result, err := call([]string{"spaces", "é", ""}, nil); err == nil {
		observed["unicodeEmpty"] = result
	} else {
		observed["unicodeEmpty"] = code(err)
	}
	_, err = call([]string{"missing"}, nil)
	var public *core.PublicError
	if errors.As(err, &public) {
		observed["missing"] = public.Code
	} else {
		observed["missing"] = code(err)
	}
	_, err = call([]string{"fail"}, nil)
	observed["fail"] = code(err)
	if err := dispatch.Emit(ctx, root, []string{"ping"}, 7); err != nil {
		observed["pong"] = code(err)
	} else {
		select {
		case pong := <-pongs:
			observed["pong"] = pong
		case <-time.After(5 * time.Second):
			observed["pong"] = "no pong"
		}
	}
	withdrawn, withdraw := context.WithTimeout(ctx, 300*time.Millisecond)
	err = dispatch.Call(withdrawn, root, []string{"wait"}, nil, nil)
	withdraw()
	observed["withdrawn"] = err != nil && errors.Is(err, context.DeadlineExceeded)
	if result, err := call([]string{"cancelled"}, nil); err == nil {
		observed["serverSawCancel"] = result
	} else {
		observed["serverSawCancel"] = code(err)
	}
	var meta json.RawMessage
	if err := dispatch.Call(core.WithMeta(ctx, core.Meta{"tenant": "t1"}), root, []string{"meta"}, nil, &meta); err == nil {
		observed["meta"] = meta
	} else {
		observed["meta"] = code(err)
	}
	if result, err := call([]string{"reverse"}, nil); err == nil {
		observed["reverse"] = result
	} else {
		observed["reverse"] = code(err)
	}
	var big string
	if err := dispatch.Call(ctx, root, []string{"big"}, map[string]int{"n": 3 << 20}, &big); err == nil {
		observed["bigLength"] = len(big)
	} else {
		observed["bigLength"] = code(err)
	}
	var wg sync.WaitGroup
	var matched sync.Map
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var got map[string]int
			if err := dispatch.Call(ctx, root, []string{"echo"}, map[string]int{"i": i}, &got); err == nil && got["i"] == i {
				matched.Store(i, true)
			}
		}()
	}
	wg.Wait()
	concurrent := 0
	matched.Range(func(any, any) bool { concurrent++; return true })
	observed["concurrent"] = concurrent
	var _ wire.AddressedWire = root
	return json.NewEncoder(os.Stdout).Encode(observed)
}
