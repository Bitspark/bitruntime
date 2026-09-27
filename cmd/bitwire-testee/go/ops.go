// Ported from nightseam v0.6.0 conformance/go/testee/ops.go (5cc9723).

package main

// ops is every op the testee serves, by name: the families composed here
// and nowhere else. The tunnel's, the live layer's and the wire recorder's
// are not among them: bitruntime implements none of them yet, so their ops
// answer unsupported.
func (t *testee) ops() map[string]func(request) (any, error) {
	all := map[string]func(request) (any, error){}
	for _, family := range []map[string]func(request) (any, error){t.seamOps(), t.peerOps()} {
		for name, handler := range family {
			all[name] = handler
		}
	}
	return all
}
