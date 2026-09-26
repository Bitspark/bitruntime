package duplex_test

import (
	"testing"

	"github.com/Bitspark/nightseam/duplex/go"
	"github.com/Bitspark/nightseam/duplex/go/duplextest"
)

// TestPipeIsAConformingTransport: the in-memory pipe keeps every promise of
// the seam, so a protocol proven over it is proven over the seam.
func TestPipeIsAConformingTransport(t *testing.T) {
	duplextest.Run(t, func(t *testing.T, limit int64) (duplex.Conn, duplex.Conn) {
		return duplex.Pipe(limit)
	})
}
