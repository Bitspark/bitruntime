package export

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
	"time"

	core "github.com/Bitspark/bitruntime/core/go"
	ontos "github.com/Bitspark/bitwire/ontos/go/core"
	wire "github.com/Bitspark/bitwire/wire/go"
)

// testdata/export-vectors.json is bitwire's corpus at the record's commit
// (its revision with the swap-back rule; see the PR history), copied unchanged.
const vectorsSHA256 = "dc6513a7aca5cfd9271bf31dbaae0cb34b1c198fea60ee16d1d0cf83f8f7f512"

type vectors struct {
	Encode []struct {
		Name    string          `json:"name"`
		Value   json.RawMessage `json:"value"`
		Path    []string        `json:"path"`
		Message json.RawMessage `json:"message"`
		Hex     string          `json:"hex"`
	} `json:"encode"`
	RejectReference []struct {
		Name  string          `json:"name"`
		Value json.RawMessage `json:"value"`
	} `json:"rejectReference"`
	Table struct {
		Scope    string   `json:"scope"`
		Live     []string `json:"live"`
		Released []string `json:"released"`
	} `json:"table"`
	Deliveries []struct {
		Name   string          `json:"name"`
		Path   []string        `json:"path"`
		Expect json.RawMessage `json:"expect"`
	} `json:"deliveries"`
}

func load(t *testing.T) vectors {
	t.Helper()
	data, err := os.ReadFile("testdata/export-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	if sum := sha256.Sum256(data); hex.EncodeToString(sum[:]) != vectorsSHA256 {
		t.Fatal("testdata/export-vectors.json differs from the pinned record corpus")
	}
	var v vectors
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func atomHex(t *testing.T, s string) ontos.Atom {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return ontos.NewAtom(b)
}

func value(t *testing.T, raw json.RawMessage) ontos.Value {
	t.Helper()
	var v struct {
		Atom  *string           `json:"atom"`
		Tuple []json.RawMessage `json:"tuple"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	if v.Atom != nil {
		return atomHex(t, *v.Atom)
	}
	items := make([]ontos.Value, len(v.Tuple))
	for i, item := range v.Tuple {
		items[i] = value(t, item)
	}
	return ontos.NewTuple(items...)
}

func path(t *testing.T, keys []string) wire.Path {
	p := wire.Path{}
	for _, k := range keys {
		p = append(p, atomHex(t, k))
	}
	return p
}

func TestEncodeAndRejectVectors(t *testing.T) {
	v := load(t)
	for _, c := range v.Encode {
		var built ontos.Value
		if c.Path != nil {
			var err error
			if built, err = wire.PackAddressed(path(t, c.Path), value(t, c.Message)); err != nil {
				t.Fatal(err)
			}
		} else {
			built = value(t, c.Value)
			scope, id, err := ParseReference(built)
			if err != nil || !Reference(scope, id).Equal(built) {
				t.Fatalf("%s: reference does not round-trip: %v", c.Name, err)
			}
		}
		encoded, err := wire.EncodeMessage(built, wire.DefaultMaxMessageBytes)
		if err != nil || hex.EncodeToString(encoded) != c.Hex {
			t.Fatalf("%s: encoded %x", c.Name, encoded)
		}
	}
	for _, c := range v.RejectReference {
		if _, _, err := ParseReference(value(t, c.Value)); err == nil {
			t.Fatalf("%s: accepted", c.Name)
		}
	}
}

type recorder struct{ got chan ontos.Value }

func (r recorder) Send(v ontos.Value) error { r.got <- v; return nil }

func newRecorder() recorder { return recorder{make(chan ontos.Value, 16)} }

// The record's declared table: scope a0…, live ids 1 and 2, id 3 released.
func TestDeliveryVectors(t *testing.T) {
	v := load(t)
	for _, c := range v.Deliveries {
		t.Run(c.Name, func(t *testing.T) {
			tb, _ := NewTable(8)
			tb.scope = atomHex(t, v.Table.Scope)
			targets := map[string]recorder{}
			for _, idHex := range []string{"31", "32", "33"} {
				r := newRecorder()
				ref, err := tb.Export(r)
				if err != nil {
					t.Fatal(err)
				}
				_, id, _ := ParseReference(ref)
				if hex.EncodeToString(id.Bytes()) != idHex {
					t.Fatalf("issued id %x, want %s", id.Bytes(), idHex)
				}
				targets[idHex] = r
			}
			tb.Withdraw(Reference(tb.scope, atomHex(t, "33")))
			refused := make(chan string, 2)
			tb.OnRefuse = func(reason string, _ wire.Path) { refused <- reason }
			msg := ontos.NewAtom([]byte("m"))
			tb.Deliver(path(t, c.Path), msg)
			var expect struct {
				Deliver *string `json:"deliver"`
				Release *string `json:"release"`
				Refuse  string  `json:"refuse"`
			}
			var nothing string
			if json.Unmarshal(c.Expect, &nothing) == nil {
				if tb.Live() != 2 || len(refused) != 0 {
					t.Fatalf("expected nothing: live %d, refusals %d", tb.Live(), len(refused))
				}
				return
			}
			_ = json.Unmarshal(c.Expect, &expect)
			switch {
			case expect.Deliver != nil:
				select {
				case got := <-targets[*expect.Deliver].got:
					if !got.Equal(msg) {
						t.Fatalf("delivered %v", got)
					}
				default:
					t.Fatal("not delivered")
				}
			case expect.Release != nil:
				if tb.Live() != 1 {
					t.Fatalf("live %d after release", tb.Live())
				}
			default:
				if got := <-refused; got != expect.Refuse {
					t.Fatalf("refused %s, want %s", got, expect.Refuse)
				}
			}
			for id, r := range targets {
				if expect.Deliver == nil || id != *expect.Deliver {
					if len(r.got) != 0 {
						t.Fatalf("export %s received a value", id)
					}
				}
			}
		})
	}
}

// --- observations over real connections -------------------------------------

var (
	routeSeg = ontos.NewAtom([]byte("r"))
	rootSeg  = ontos.NewAtom([]byte("x"))
	root     = wire.Path{rootSeg}
)

// side is one end of a connection: its dispatcher (the single receive owner),
// export table and importer.
type side struct {
	ep       wire.AddressedEndpoint
	table    *Table
	importer *Importer
	refused  chan string
}

func connect(t *testing.T) (*side, *side, *core.PairEndpoint) {
	x, y, err := core.NewPair(core.PairOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mk := func(e *core.PairEndpoint) *side {
		tb, _ := NewTable(8)
		ep := core.Addressed(e)
		s := &side{ep: ep, table: tb, importer: NewImporter(ep, root, tb), refused: make(chan string, 8)}
		tb.OnRefuse = func(reason string, _ wire.Path) { s.refused <- reason }
		return s
	}
	return mk(x), mk(y), x
}

func (s *side) listen(t *testing.T, onRoute func(record ontos.Tuple)) {
	if _, err := s.ep.Receive(func(p wire.Path, v ontos.Value) {
		switch {
		case len(p) > 0 && p[0].Equal(rootSeg):
			s.table.Deliver(p[1:], v)
		case len(p) > 0 && p[0].Equal(routeSeg) && onRoute != nil:
			onRoute(v.(ontos.Tuple))
		}
	}); err != nil {
		t.Fatal(err)
	}
}

// hop forwards (payload, (refs…)) records, re-exporting each declared reference.
func hop(t *testing.T, in, out *side) {
	in.listen(t, func(rec ontos.Tuple) {
		var next []ontos.Value
		for _, ref := range rec.At(1).(ontos.Tuple).Items() {
			imp, err := in.importer.Import(ref)
			if err != nil {
				t.Error(err)
				return
			}
			nref, err := out.table.ReExport(imp)
			if err != nil {
				t.Error(err)
				return
			}
			next = append(next, nref)
		}
		_ = out.ep.Send(wire.Path{routeSeg}, ontos.NewTuple(rec.At(0), ontos.NewTuple(next...)))
	})
}

func within[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
		var zero T
		return zero
	}
}

func eventually(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func quiet[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected %s: %v", what, v)
	case <-time.After(50 * time.Millisecond):
	}
}

// A reply crosses two forwarding boundaries and the service's release travels back.
func TestReplyAcrossTwoBoundaries(t *testing.T) {
	k, r1in, _ := connect(t)
	r1out, r2in, _ := connect(t)
	r2out, s, _ := connect(t)
	k.listen(t, nil)
	hop(t, r1in, r1out)
	hop(t, r2in, r2out)
	r1out.listen(t, nil)
	r2out.listen(t, nil)
	s.listen(t, func(rec ontos.Tuple) {
		imp, _ := s.importer.Import(rec.At(1).(ontos.Tuple).At(0))
		release := imp.Hold()
		_ = imp.Wire().Send(ontos.NewTuple(ontos.NewAtom([]byte("ok")), rec.At(0)))
		release()
	})
	reply := newRecorder()
	ref, _ := k.table.Export(reply)
	if err := k.ep.Send(wire.Path{routeSeg}, ontos.NewTuple(ontos.NewAtom([]byte("hello")), ontos.NewTuple(ref))); err != nil {
		t.Fatal(err)
	}
	if v := within(t, reply.got); !v.Equal(ontos.NewTuple(ontos.NewAtom([]byte("ok")), ontos.NewAtom([]byte("hello")))) {
		t.Fatalf("reply %v", v)
	}
	eventually(t, func() bool { return k.table.Live()+r1out.table.Live()+r2out.table.Live() == 0 }, "release did not reach the caller")
	quiet(t, reply.got, "second reply")
}

// Two tables both issue id 1; a reference copied from one onto the other is foreign there.
func TestTwoConnectionAlias(t *testing.T) {
	a1, b1, _ := connect(t)
	a2, b2, _ := connect(t)
	for _, s := range []*side{a1, b1, a2, b2} {
		s.listen(t, nil)
	}
	first, second := newRecorder(), newRecorder()
	ref1, _ := a1.table.Export(first)
	ref2, _ := a2.table.Export(second)
	_, id1, _ := ParseReference(ref1)
	_, id2, _ := ParseReference(ref2)
	if !id1.Equal(id2) {
		t.Fatal("the case needs coinciding ids")
	}
	copied, _ := b2.importer.Import(ref1) // a1's reference used on the a2–b2 connection
	_ = copied.Wire().Send(ontos.NewAtom([]byte("forged")))
	if reason := within(t, a2.refused); reason != ForeignReference {
		t.Fatalf("reason %s", reason)
	}
	quiet(t, second.got, "delivery to the coinciding export")
	quiet(t, first.got, "delivery across connections")
}

// A new connection is a new scope; ids are never reused within a scope.
func TestScopeRenewalAndIDsNotReused(t *testing.T) {
	old, _ := NewTable(4)
	renewed, _ := NewTable(4)
	if old.Scope().Equal(renewed.Scope()) {
		t.Fatal("two tables share a scope")
	}
	r := newRecorder()
	ref1, _ := old.Export(r)
	old.Withdraw(ref1)
	ref2, _ := old.Export(r)
	_, id1, _ := ParseReference(ref1)
	_, id2, _ := ParseReference(ref2)
	if id1.Equal(id2) {
		t.Fatal("an id was reused within its scope")
	}
	refused := make(chan string, 1)
	old.OnRefuse = func(reason string, _ wire.Path) { refused <- reason }
	old.Deliver(wire.Path{sendVerb, old.Scope(), id1}, ontos.NewTuple())
	if <-refused != UnknownReference {
		t.Fatal("a released id was accepted")
	}
	renewed.OnRefuse = old.OnRefuse
	renewed.Deliver(wire.Path{sendVerb, old.Scope(), id2}, ontos.NewTuple())
	if <-refused != ForeignReference {
		t.Fatal("an old scope reached a renewed table")
	}
}

// Fan-out: one import re-exported on two connections; releasing one branch
// sends nothing upstream, the other still delivers, and the last release sends once.
func TestFanOutPartialRelease(t *testing.T) {
	k, fin, _ := connect(t)    // caller ↔ forwarder
	fout1, b1, _ := connect(t) // forwarder ↔ branch 1
	fout2, b2, _ := connect(t) // forwarder ↔ branch 2
	releases := make(chan string, 4)
	k.table.OnRefuse = func(reason string, _ wire.Path) { releases <- "refused " + reason }
	k.listen(t, nil)
	fin.listen(t, nil)
	fout1.listen(t, nil)
	fout2.listen(t, nil)
	b1.listen(t, nil)
	b2.listen(t, nil)
	target := newRecorder()
	ref, _ := k.table.Export(target)
	imp, _ := fin.importer.Import(ref)
	ref1, _ := fout1.table.ReExport(imp)
	ref2, _ := fout2.table.ReExport(imp)
	branch1, _ := b1.importer.Import(ref1)
	branch2, _ := b2.importer.Import(ref2)
	rel1, rel2 := branch1.Hold(), branch2.Hold()
	rel1()
	eventually(t, func() bool { return fout1.table.Live() == 0 }, "branch 1 was not released")
	if k.table.Live() != 1 {
		t.Fatal("releasing one branch released the import upstream")
	}
	_ = branch2.Wire().Send(ontos.NewAtom([]byte("survivor")))
	if v := within(t, target.got); !v.Equal(ontos.NewAtom([]byte("survivor"))) {
		t.Fatalf("got %v", v)
	}
	rel2()
	eventually(t, func() bool { return k.table.Live() == 0 }, "the last release did not reach the exporter")
	quiet(t, releases, "refusal")
}

// A re-export retires when its source connection ends and never follows a replacement.
func TestRetirementWithSource(t *testing.T) {
	k, fin, kLink := connect(t)
	fout, b, _ := connect(t)
	k.listen(t, nil)
	fin.listen(t, nil)
	fout.listen(t, nil)
	b.listen(t, nil)
	target := newRecorder()
	ref, _ := k.table.Export(target)
	imp, _ := fin.importer.Import(ref)
	ref2, _ := fout.table.ReExport(imp)
	downstream, _ := b.importer.Import(ref2)
	kLink.Close()
	fin.importer.Close() // the forwarder's host: its source connection ended
	if fout.table.Live() != 0 {
		t.Fatal("the re-export outlived its source")
	}
	_ = downstream.Wire().Send(ontos.NewAtom([]byte("late")))
	if reason := within(t, fout.refused); reason != UnknownReference {
		t.Fatalf("reason %s", reason)
	}
	quiet(t, target.got, "delivery after retirement")
	if _, err := fout.table.ReExport(imp); err == nil {
		t.Fatal("a retired import was re-exported")
	}
}

// Release never closes the target, and the table is bounded.
func TestReleaseAndBound(t *testing.T) {
	tb, _ := NewTable(1)
	target := newRecorder()
	ref, _ := tb.Export(target)
	if _, err := tb.Export(target); err != ErrLimit {
		t.Fatalf("bound: %v", err)
	}
	_, id, _ := ParseReference(ref)
	tb.Deliver(wire.Path{relVerb, tb.Scope(), id}, ontos.NewTuple())
	tb.Deliver(wire.Path{relVerb, tb.Scope(), id}, ontos.NewTuple())
	if tb.Live() != 0 {
		t.Fatal("release did not end the export")
	}
	if err := target.Send(ontos.NewTuple()); err != nil {
		t.Fatal("release affected the target")
	}
	if _, err := tb.Export(target); err != nil {
		t.Fatal("the released slot was not available")
	}
}

// A reference the peer returns is swapped back to the registered Wire itself,
// and releasing that import never ends this side's own export.
func TestSwapBack(t *testing.T) {
	a, b, _ := connect(t)
	a.listen(t, nil)
	b.listen(t, nil)
	target := newRecorder()
	ref, _ := a.table.Export(target)
	back, err := a.importer.Import(ref) // b returned a's reference to a
	if err != nil {
		t.Fatal(err)
	}
	if back.Wire() != wire.Wire(target) {
		t.Fatal("a returned reference was not swapped back to the registered Wire")
	}
	release := back.Hold()
	_ = back.Wire().Send(ontos.NewAtom([]byte("home")))
	if v := within(t, target.got); !v.Equal(ontos.NewAtom([]byte("home"))) {
		t.Fatalf("got %v", v)
	}
	release()
	time.Sleep(20 * time.Millisecond)
	if a.table.Live() != 1 {
		t.Fatal("releasing a swapped-back import ended the export")
	}
	a.table.Withdraw(ref)
	stale, _ := a.importer.Import(ref)
	if err := stale.Wire().Send(ontos.NewTuple()); err == nil {
		t.Fatal("a returned reference to an ended export was accepted")
	}
	if reason := within(t, a.refused); reason != UnknownReference {
		t.Fatalf("reason %s", reason)
	}
	quiet(t, target.got, "delivery after withdrawal")
}
