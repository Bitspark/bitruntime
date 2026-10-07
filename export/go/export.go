// Package export realizes bitwire's live Wire export (decision 0018):
// connection-scoped exports of send-only Wires over addressed reference routes,
// import through the addressed facade, release, and re-export through another
// hop with source retirement and dependent-lifetime accounting.
//
// One Table serves one side of one connection. The connection's dispatcher stays
// its single receive owner and hands the Table every addressed value received
// under the side's export root. An Importer serves the same side for references
// received from the peer.
package export

import (
	"crypto/rand"
	"errors"
	"strconv"
	"sync"

	core "github.com/Bitspark/bitruntime/core/go"
	ontos "github.com/Bitspark/bitwire/ontos/go/core"
	wire "github.com/Bitspark/bitwire/wire/go"
)

var (
	refLabel = ontos.NewAtom([]byte("bitwire/ref/1"))
	sendVerb = ontos.NewAtom([]byte("send"))
	relVerb  = ontos.NewAtom([]byte("release"))
)

// Refusal reasons, observed by the exporting side only.
const (
	MalformedRoute   = "malformed-route"
	ForeignReference = "foreign-reference"
	UnknownReference = "unknown-reference"
)

// ErrLimit is returned when an export would exceed the table's bound.
var ErrLimit = errors.New("export-limit")

// ErrClosed is returned by a closed table or importer.
var ErrClosed = errors.New("export scope ended")

// Reference builds the reference value (bitwire/ref/1, scope, id).
func Reference(scope, id ontos.Atom) ontos.Value { return ontos.NewTuple(refLabel, scope, id) }

// ParseReference reads a reference value.
func ParseReference(v ontos.Value) (scope, id ontos.Atom, err error) {
	t, ok := v.(ontos.Tuple)
	if !ok || t.Len() != 3 || !refLabel.Equal(t.At(0)) {
		return ontos.Atom{}, ontos.Atom{}, errors.New("not a reference")
	}
	s, ok1 := t.At(1).(ontos.Atom)
	i, ok2 := t.At(2).(ontos.Atom)
	if !ok1 || !ok2 {
		return ontos.Atom{}, ontos.Atom{}, errors.New("reference scope and id must be atoms")
	}
	return s, i, nil
}

type entry struct {
	target    wire.Wire
	onRelease func() // runs once when the export ends, for any reason
}

// Table holds one connection side's exports.
type Table struct {
	mu       sync.Mutex
	scope    ontos.Atom
	next     uint64
	limit    int
	entries  map[string]*entry
	closed   bool
	OnRefuse func(reason string, path wire.Path)
}

// NewTable creates a table with a fresh random scope and a bound on live exports.
func NewTable(limit int) (*Table, error) {
	if limit <= 0 {
		return nil, errors.New("export limit must be positive")
	}
	scope := make([]byte, 16)
	if _, err := rand.Read(scope); err != nil {
		return nil, err
	}
	return &Table{scope: ontos.NewAtom(scope), limit: limit, entries: map[string]*entry{}}, nil
}

// Scope is the table's scope atom.
func (t *Table) Scope() ontos.Atom { return t.scope }

// Export registers target and returns its reference. Ids are never reused.
func (t *Table) Export(target wire.Wire) (ontos.Value, error) {
	return t.export(target, nil)
}

func (t *Table) export(target wire.Wire, onRelease func()) (ontos.Value, error) {
	if target == nil {
		return nil, errors.New("nil target")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrClosed
	}
	if len(t.entries) >= t.limit {
		return nil, ErrLimit
	}
	t.next++
	id := ontos.NewAtom([]byte(strconv.FormatUint(t.next, 10)))
	t.entries[string(id.Bytes())] = &entry{target: target, onRelease: onRelease}
	return Reference(t.scope, id), nil
}

// Withdraw ends an export from the exporting side. Ending it twice changes nothing.
func (t *Table) Withdraw(ref ontos.Value) {
	scope, id, err := ParseReference(ref)
	if err != nil || !scope.Equal(t.scope) {
		return
	}
	t.end(string(id.Bytes()))
}

func (t *Table) end(id string) {
	t.mu.Lock()
	e := t.entries[id]
	delete(t.entries, id)
	t.mu.Unlock()
	if e != nil && e.onRelease != nil {
		e.onRelease()
	}
}

// Live reports the number of live exports.
func (t *Table) Live() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.entries)
}

// Deliver decides one addressed value received under the export root; path is
// relative to that root.
func (t *Table) Deliver(path wire.Path, message ontos.Value) {
	if len(path) != 3 || !(path[0].Equal(sendVerb) || path[0].Equal(relVerb)) {
		t.refuse(MalformedRoute, path)
		return
	}
	if !path[1].Equal(t.scope) {
		t.refuse(ForeignReference, path)
		return
	}
	id := string(path[2].Bytes())
	if path[0].Equal(relVerb) {
		t.end(id) // releasing a released or unknown id changes nothing
		return
	}
	t.mu.Lock()
	e := t.entries[id]
	t.mu.Unlock()
	if e == nil {
		t.refuse(UnknownReference, path)
		return
	}
	_ = e.target.Send(message) // the target's own admission; release never closes it
}

// Close ends the scope with its connection: every export ends.
func (t *Table) Close() {
	t.mu.Lock()
	t.closed = true
	entries := t.entries
	t.entries = map[string]*entry{}
	t.mu.Unlock()
	for _, e := range entries {
		if e.onRelease != nil {
			e.onRelease()
		}
	}
}

func (t *Table) refuse(reason string, path wire.Path) {
	if t.OnRefuse != nil {
		t.OnRefuse(reason, append(wire.Path{}, path...))
	}
}

// Importer turns references received from the peer into send-only proxies.
type Importer struct {
	mu      sync.Mutex
	root    wire.AddressedWire
	imports map[*Imported]struct{}
	closed  bool
}

// NewImporter serves one side: sender is its addressed sender on the
// connection, peerRoot the peer's declared export root.
func NewImporter(sender wire.AddressedWire, peerRoot wire.Path) *Importer {
	return &Importer{root: core.Under(sender, peerRoot), imports: map[*Imported]struct{}{}}
}

// Imported is one import with its dependents.
type Imported struct {
	importer   *Importer
	scope, id  ontos.Atom
	proxy      wire.Wire
	mu         sync.Mutex
	dependents int
	ended      bool
	retirees   []func() // re-exports that depend on this import
}

// Import imports a reference. It grants send only and validates nothing: a bad
// reference is refused by the exporter.
func (m *Importer) Import(ref ontos.Value) (*Imported, error) {
	scope, id, err := ParseReference(ref)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	i := &Imported{importer: m, scope: scope, id: id, proxy: core.Bind(m.root, wire.Path{sendVerb, scope, id})}
	m.imports[i] = struct{}{}
	return i, nil
}

// Wire is the send-only proxy. A send is local admission on the connection.
func (i *Imported) Wire() wire.Wire { return i.proxy }

// Hold adds a dependent and returns its release. When the last dependent is
// released, exactly one release is sent to the exporter.
func (i *Imported) Hold() (release func()) {
	i.mu.Lock()
	i.dependents++
	i.mu.Unlock()
	var once sync.Once
	return func() { once.Do(i.drop) }
}

func (i *Imported) drop() {
	i.mu.Lock()
	i.dependents--
	last := i.dependents == 0 && !i.ended
	if last {
		i.ended = true
	}
	i.mu.Unlock()
	if last {
		i.importer.forget(i)
		_ = i.importer.root.Send(wire.Path{relVerb, i.scope, i.id}, ontos.NewTuple())
	}
}

func (m *Importer) forget(i *Imported) {
	m.mu.Lock()
	delete(m.imports, i)
	m.mu.Unlock()
}

// retire ends the import because its connection ended: every re-export of it
// ends, and nothing is sent.
func (i *Imported) retire() {
	i.mu.Lock()
	i.ended = true
	retirees := i.retirees
	i.retirees = nil
	i.mu.Unlock()
	for _, r := range retirees {
		r()
	}
}

// Close retires every import from this connection, as its connection ended.
func (m *Importer) Close() {
	m.mu.Lock()
	m.closed = true
	imports := m.imports
	m.imports = map[*Imported]struct{}{}
	m.mu.Unlock()
	for i := range imports {
		i.retire()
	}
}

// ReExport exports an import's proxy on this table, as a dependent of the
// import. The re-export retires when the import's connection ends; releasing
// it drops only its own dependency.
func (t *Table) ReExport(i *Imported) (ontos.Value, error) {
	release := i.Hold()
	ref, err := t.export(i.proxy, release)
	if err != nil {
		release()
		return nil, err
	}
	_, id, _ := ParseReference(ref)
	i.mu.Lock()
	if i.ended {
		i.mu.Unlock()
		t.end(string(id.Bytes()))
		return nil, ErrClosed
	}
	i.retirees = append(i.retirees, func() { t.end(string(id.Bytes())) })
	i.mu.Unlock()
	return ref, nil
}
