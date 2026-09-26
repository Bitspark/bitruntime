import {spawnSync} from 'node:child_process';
import {mkdtempSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';

const revision = process.argv[2];
if (!revision || !/^[a-zA-Z0-9._-]+$/.test(revision)) {
  throw new Error('Pass the pushed commit or release tag to verify');
}
const temp = mkdtempSync(join(tmpdir(), 'bitruntime-go-consumer-'));
function run(args) {
  const result = spawnSync('go', args, {
    cwd: temp, stdio: 'inherit', env: {...process.env, GOWORK: 'off'},
  });
  if (result.status !== 0) throw new Error(`go ${args.join(' ')} failed`);
}
run(['mod', 'init', 'example.com/bitruntime-consumer']);
writeFileSync(join(temp, 'main.go'), `package main
import (
  "context"
  "encoding/json"
  "errors"
  "fmt"
  "net/http"
  "net/http/httptest"
  "strings"
  "time"
  core "github.com/Bitspark/bitruntime/core/go"
  dispatch "github.com/Bitspark/bitruntime/dispatch/go"
  engine "github.com/Bitspark/bitruntime/engine/go"
  websocket "github.com/Bitspark/bitruntime/engine/websocket/go"
  wire "github.com/Bitspark/bitwire/wire/go"
)
type target struct { seen *int }
func (t target) Send(wire.Message) error { *t.seen++; return nil }
func trees() {
  seen := 0
  leaf, err := core.Compose[wire.Wire](target{&seen}, nil)
  if err != nil { panic(err) }
  tree, err := core.Compose[wire.Wire](target{&seen}, []wire.Child[wire.Wire]{{Key:[]byte("child"), Tree:leaf}})
  if err != nil { panic(err) }
  selected, ok := core.Select[wire.Wire](tree, wire.TreePath{[]byte("child")})
  if !ok || selected != leaf { panic("selection did not retain child identity") }
  message := wire.Message{Frame:wire.ProfileFrame{Version:1, Kind:wire.ProfileEvent}}
  if err = core.Send(tree, wire.TreePath{[]byte("child")}, message); err != nil { panic(err) }
  if err = core.AsAddressed(tree).Send([]string{"child"}, message); err != nil { panic(err) }
  if seen != 2 { panic("derived sends did not reach the selected primitive") }
  if err = core.Send(tree, wire.TreePath{[]byte{255}}, message); !errors.Is(err, core.ErrMissingPath) { panic("missing path was not refused") }
}
func serve(d *dispatch.Dispatcher) error {
  _, err := dispatch.Handle(d, []string{"spaces", "a/b", "echo"}, func(_ context.Context, raw json.RawMessage) (any, error) { return raw, nil })
  return err
}
func runtimePath() {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
  defer cancel()
  left, right, err := core.NewPair(core.PairOptions{})
  if err != nil { panic(err) }
  d, err := dispatch.NewDispatcher(right)
  if err != nil { panic(err) }
  if err = serve(d); err != nil { panic(err) }
  var got map[string]int
  if err = dispatch.Call(ctx, core.At(left, []string{"spaces", "a/b"}), []string{"echo"}, map[string]int{"n": 1}, &got); err != nil || got["n"] != 1 { panic(fmt.Sprint("pair call: ", got, err)) }
  handler, err := websocket.NewHandler(websocket.ServerOptions{
    Authenticate: func(*http.Request) (context.Context, error) { return context.Background(), nil },
    CheckOrigin: func(*http.Request) bool { return true },
    Options: engine.Options{Prepare: func(p *engine.Peer) error {
      d, err := dispatch.NewDispatcher(p.Wire())
      if err != nil { return err }
      return serve(d)
    }},
  })
  if err != nil { panic(err) }
  server := httptest.NewServer(handler)
  defer server.Close()
  peer, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), websocket.DialOptions{})
  if err != nil { panic(err) }
  defer peer.Close()
  got = nil
  if err = dispatch.Call(ctx, peer.Wire(), []string{"spaces", "a/b", "echo"}, map[string]int{"n": 2}, &got); err != nil || got["n"] != 2 { panic(fmt.Sprint("socket call: ", got, err)) }
}
func main() {
  trees()
  runtimePath()
  fmt.Println("Fresh public Go module consumer passed: trees, a local pair and a WebSocket peer.")
}
`);
run(['get', `github.com/Bitspark/bitruntime@${revision}`]);
run(['mod', 'tidy']);
run(['run', '.']);
console.log(`Installed public Go module at ${revision} in ${temp}`);
