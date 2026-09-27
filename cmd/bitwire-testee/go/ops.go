package main

// ops is every op the testee serves, by name: the families composed here
// and nowhere else.
func (t *testee) ops() map[string]func(request) (any, error) {
	all := map[string]func(request) (any, error){}
	for _, family := range []map[string]func(request) (any, error){t.seamOps(), t.peerOps(), t.tunnelOps(), t.liveOps(), t.recordedWireOps()} {
		for name, handler := range family {
			all[name] = handler
		}
	}
	return all
}
