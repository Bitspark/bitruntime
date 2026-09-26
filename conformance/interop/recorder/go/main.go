// Command interop-recorder speaks bitwire/1 frames by hand to a server of the
// interoperability scenario and prints, one per line, every frame the server
// sent back and how the connection ended. Two servers speak the same revision
// byte for byte when their transcripts are equal.
//
// The recorder sends one request at a time and waits for its answer, so the
// transcript does not depend on handler scheduling. The only bytes it masks are
// trace identifiers the server minted for its own frames, which are random by
// design.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/coder/websocket"
)

const trace = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

// exchange is one frame the recorder sends and how many server frames it waits
// for before sending the next.
type exchange struct {
	send   string
	expect int
}

var script = []exchange{
	{`{"version":1,"kind":"request","id":"c:1","method":"4:echo","params":{"z":1,"a":[1e3,"é","é",null]},"traceparent":"` + trace + `","tracestate":"k=v"}`, 1},
	{`{"version":1,"kind":"request","id":"c:2","method":"6:spaces3:a/b4:echo","params":{"x":1}}`, 1},
	{`{"version":1,"kind":"request","id":"c:3","method":"6:spaces2:é0:","params":null}`, 1},
	{`{"version":1,"kind":"request","id":"c:4","method":"7:missing","params":null}`, 1},
	{`{"version":1,"kind":"request","id":"c:5","method":"4:fail","params":{},"traceparent":"` + trace + `"}`, 1},
	{`{"version":1,"kind":"event","event":"4:ping","data":7,"traceparent":"` + trace + `"}`, 1},
	{`{"version":1,"kind":"request","id":"c:6","method":"4:meta","params":null,"meta":{"tenant":"t1","b":"2"}}`, 1},
	{`{"version":1,"kind":"request","id":"c:7","method":"7:reverse","params":null,"traceparent":"` + trace + `"}`, 2},
	{`{"version":1,"kind":"request","id":"c:9","method":"4:wait","params":null}`, 0},
	{`{"version":1,"kind":"cancel","id":"c:9"}`, 1},
	{`{"version":1,"kind":"request","id":"c:10","method":"echo","params":1}`, 1},
	{`{"version":1,"kind":"request","id":"c:11","method":"3:big","params":{"n":5}}`, 1},
	{`{"version":1,"kind":"request","id":"c:12","method":"4:echo","params":"x","meta":{"nightseam.reserved":"no"}}`, 0},
}

// minted matches trace members the server generated for frames of its own.
var minted = regexp.MustCompile(`"traceparent":"00-([0-9a-f]{32})-([0-9a-f]{16})-01"`)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: interop-recorder <url>")
		os.Exit(2)
	}
	if err := record(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func record(url string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(4 << 20)
	read := func() (string, error) {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return "", err
		}
		text := string(data)
		// The server's reverse request is answered at once so that the
		// exchange that caused it can complete.
		if strings.Contains(text, `"kind":"request"`) && strings.Contains(text, `"method":"6:whoami"`) {
			id := regexp.MustCompile(`"id":"(s:[0-9]+)"`).FindStringSubmatch(text)
			if id == nil {
				return "", errors.New("reverse request without an identifier")
			}
			answer := `{"version":1,"kind":"response","id":"` + id[1] + `","result":"recorder"}`
			if err := conn.Write(ctx, websocket.MessageText, []byte(answer)); err != nil {
				return "", err
			}
		}
		return mask(text), nil
	}
	for _, step := range script {
		if err := conn.Write(ctx, websocket.MessageText, []byte(step.send)); err != nil {
			return err
		}
		for range step.expect {
			text, err := read()
			if err != nil {
				return fmt.Errorf("after %s: %w", step.send, err)
			}
			fmt.Println(text)
		}
	}
	// The last frame breaks the protocol; the server must end the connection
	// with the profile's code and reason.
	_, _, err = conn.Read(ctx)
	fmt.Printf("END %d %q\n", websocket.CloseStatus(err), closeReason(err))
	return nil
}

func mask(text string) string {
	return minted.ReplaceAllStringFunc(text, func(member string) string {
		match := minted.FindStringSubmatch(member)
		if match[1] == "0af7651916cd43dd8448eb211c80319c" {
			// A child of the recorder's trace keeps its trace id; only the
			// server's own span identifier is random.
			if match[2] == "b7ad6b7169203331" {
				return member
			}
			return `"traceparent":"00-0af7651916cd43dd8448eb211c80319c-SPAN-01"`
		}
		return `"traceparent":"MINTED"`
	})
}

func closeReason(err error) string {
	var closed websocket.CloseError
	if errors.As(err, &closed) {
		return closed.Reason
	}
	return ""
}
