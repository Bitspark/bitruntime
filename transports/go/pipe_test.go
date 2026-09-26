package transports_test

import (
	"testing"

	transports "github.com/Bitspark/bitruntime/transports/go"
	"github.com/Bitspark/bitruntime/transports/go/transporttest"
)

// TestPipeIsAConformingTransport: the in-memory pipe keeps every promise of
// the seam, so a protocol proven over it is proven over the seam.
func TestPipeIsAConformingTransport(t *testing.T) {
	transporttest.Run(t, func(t *testing.T, limit int64) (transports.Conn, transports.Conn) {
		return transports.Pipe(limit)
	})
}
