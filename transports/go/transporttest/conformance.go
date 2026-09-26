// Package transporttest holds every transport to what a frames duplex
// connection promises. A transport's own tests call Run with a way to make
// a connected pair; what Run checks is the seam's contract and nothing of
// the transport, so a protocol written against the seam can trust the same
// things wherever it runs: frames arrive in order and whole, a send waits
// when the receiver does not, a close carries its code and reason across,
// an observe-only code is never sent, and a frame over the limit is refused
// before it is delivered, with 1009 for its sender.
package transporttest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	transports "github.com/Bitspark/bitruntime/transports/go"
)

// Connect makes a fresh connected pair with the given receive limit on both
// ends. The test closes what it opened; the transport may register cleanup
// with t.
type Connect func(t *testing.T, limit int64) (a, b transports.Conn)

// Run holds a transport to the seam.
func Run(t *testing.T, connect Connect) {
	t.Helper()
	t.Run("frames arrive in order and whole, text and binary alike", func(t *testing.T) {
		a, b := connect(t, 1<<20)
		defer a.Abort()
		defer b.Abort()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		const n = 64
		errs := make(chan error, 1)
		go func() {
			for i := 0; i < n; i++ {
				kind := transports.Text
				if i%3 == 0 {
					kind = transports.Binary
				}
				if err := a.Send(ctx, transports.Frame{Kind: kind, Data: []byte(fmt.Sprintf("frame %02d", i))}); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
		for i := 0; i < n; i++ {
			frame, err := b.Receive(ctx)
			if err != nil {
				t.Fatalf("receive %d: %v", i, err)
			}
			want := transports.Text
			if i%3 == 0 {
				want = transports.Binary
			}
			if frame.Kind != want || !bytes.Equal(frame.Data, []byte(fmt.Sprintf("frame %02d", i))) {
				t.Fatalf("frame %d arrived as %s %q", i, frame.Kind, frame.Data)
			}
		}
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	})
	t.Run("a send waits while the receiver does not receive", func(t *testing.T) {
		a, b := connect(t, 1<<20)
		defer a.Abort()
		defer b.Abort()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		big := bytes.Repeat([]byte("x"), 256<<10)
		// Nobody receives on b. Sooner or later the transport can take no
		// more and Send must wait — until the context ends, not forever, and
		// not into a buffer of its own that hides the stall.
		for i := 0; i < 400; i++ {
			err := a.Send(ctx, transports.Frame{Kind: transports.Binary, Data: big})
			if err == nil {
				continue
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("a blocked send failed with %v, not the context's deadline", err)
			}
			return
		}
		t.Fatal("400 frames of 256 KiB were accepted with nobody receiving; the transport buffers without bound")
	})
	t.Run("a close carries its code and reason across", func(t *testing.T) {
		a, b := connect(t, 1<<20)
		defer a.Abort()
		defer b.Abort()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := a.Send(ctx, transports.Frame{Kind: transports.Text, Data: []byte("last")}); err != nil {
			t.Fatal(err)
		}
		if frame, err := b.Receive(ctx); err != nil || string(frame.Data) != "last" {
			t.Fatalf("receive before close: %q, %v", frame.Data, err)
		}
		go func() { _ = a.Close(ctx, transports.CodePolicyViolation, "an observer sent a deciding frame") }()
		_, err := b.Receive(ctx)
		var closed *transports.CloseError
		if !errors.As(err, &closed) {
			t.Fatalf("receive after the remote closed: %v", err)
		}
		if closed.Code != transports.CodePolicyViolation || closed.Reason != "an observer sent a deciding frame" {
			t.Fatalf("the close arrived as %d %q", closed.Code, closed.Reason)
		}
		if _, err := b.Receive(ctx); err == nil {
			t.Fatal("a closed connection received")
		}
		if err := a.Send(ctx, transports.Frame{Kind: transports.Text, Data: []byte("late")}); !errors.Is(err, transports.ErrClosed) {
			t.Fatalf("send after closing: %v", err)
		}
		if _, err := a.Receive(ctx); !errors.Is(err, transports.ErrClosed) {
			t.Fatalf("receive after closing: %v", err)
		}
		if !errors.Is(closed, transports.ErrClosed) {
			t.Fatal("a remote close is not classified as a closed carrier")
		}
	})
	t.Run("an observe-only code is never sent", func(t *testing.T) {
		a, b := connect(t, 1<<20)
		defer a.Abort()
		defer b.Abort()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, code := range []transports.Code{transports.CodeNoStatus, transports.CodeAbnormalClosure, transports.CodeTLSHandshake, 0, 999, 2000, 5000} {
			if err := a.Close(ctx, code, ""); !errors.Is(err, transports.ErrUnsendableCode) {
				t.Fatalf("close with %d: %v", code, err)
			}
		}
		// The refused closes sent nothing and ended nothing.
		if err := a.Send(ctx, transports.Frame{Kind: transports.Text, Data: []byte("still open")}); err != nil {
			t.Fatal(err)
		}
		if frame, err := b.Receive(ctx); err != nil || string(frame.Data) != "still open" {
			t.Fatalf("after refused closes: %q, %v", frame.Data, err)
		}
	})
	t.Run("a frame over the limit is refused, and the connection with it", func(t *testing.T) {
		a, b := connect(t, 1024)
		defer a.Abort()
		defer b.Abort()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		go func() {
			_ = a.Send(ctx, transports.Frame{Kind: transports.Text, Data: bytes.Repeat([]byte("y"), 1025)})
		}()
		if frame, err := b.Receive(ctx); err == nil {
			t.Fatalf("a frame of %d bytes was delivered over a limit of 1024", len(frame.Data))
		} else if !errors.Is(err, transports.ErrClosed) {
			t.Fatalf("refusing a frame over the limit is not a closed carrier: %v", err)
		}
		if _, err := b.Receive(ctx); err == nil {
			t.Fatal("the connection received again after refusing a frame over the limit")
		}
		// The sender learns that the connection ended. Where the transport
		// carries the receiver's close to it, the code it observes is 1009; a
		// byte stream that loses the close to its own teardown reads 1006.
		_, err := a.Receive(ctx)
		var closed *transports.CloseError
		if !errors.As(err, &closed) || (closed.Code != transports.CodeTooLarge && closed.Code != transports.CodeAbnormalClosure) {
			t.Fatalf("the sender of a frame over the limit observed %v, not 1009", err)
		}
	})
	t.Run("an abort ends the connection at once on both sides", func(t *testing.T) {
		a, b := connect(t, 1<<20)
		defer b.Abort()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		received := make(chan error, 1)
		started := make(chan struct{})
		go func() { close(started); _, err := b.Receive(ctx); received <- err }()
		<-started
		if err := a.Abort(); err != nil {
			t.Fatal(err)
		}
		if err := <-received; err == nil || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("the remote side of an aborted connection did not end on abort: %v", err)
		}
		if _, err := a.Receive(ctx); !errors.Is(err, transports.ErrClosed) {
			t.Fatalf("receive on the aborted side: %v", err)
		}
	})
}
