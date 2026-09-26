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
  const result = spawnSync('go', args, {cwd: temp, stdio: 'inherit'});
  if (result.status !== 0) throw new Error(`go ${args.join(' ')} failed`);
}
run(['mod', 'init', 'example.com/bitruntime-consumer']);
writeFileSync(join(temp, 'main.go'), `package main
import (
  "errors"
  "fmt"
  core "github.com/Bitspark/bitruntime/core/go"
  wire "github.com/Bitspark/bitwire/wire/go"
)
type target struct { seen *int }
func (t target) Send(wire.Message) error { *t.seen++; return nil }
func main() {
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
  fmt.Println("Fresh public Go module consumer passed.")
}
`);
run(['get', `github.com/Bitspark/bitruntime@${revision}`]);
run(['run', '.']);
console.log(`Installed public Go module at ${revision} in ${temp}`);
