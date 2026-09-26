// Package profile holds the bitwire/1 envelope: its JSON frame, the canonical
// encoding of an addressed path into the frame's method or event name, and the
// validation a peer applies before it admits a frame. It is shared by the
// in-process pair, the protocol engine and the helpers, and is not public API.
package profile

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	wire "github.com/Bitspark/bitwire/wire/go"
)

// ErrPath refuses a path segment that is not Unicode-scalar text, or an
// encoded name that is not the canonical form of one.
var ErrPath = errors.New("invalid wire path")

// Frame is one bitwire/1 envelope as it travels. The path of an addressed
// request or event is its canonical encoding in Method or Event.
type Frame struct {
	Version int                `json:"version"`
	Kind    string             `json:"kind"`
	ID      string             `json:"id,omitempty"`
	Method  string             `json:"method,omitempty"`
	Params  json.RawMessage    `json:"params,omitempty"`
	Result  json.RawMessage    `json:"result,omitempty"`
	Error   *wire.ProfileError `json:"error,omitempty"`
	Event   string             `json:"event,omitempty"`
	Data    json.RawMessage    `json:"data,omitempty"`
	// W3C Trace Context, which every kind may carry and none requires.
	Traceparent string `json:"traceparent,omitempty"`
	Tracestate  string `json:"tracestate,omitempty"`
	// What is about a call rather than the call, which a request and an event
	// may carry. The profile carries it verbatim and reads nothing into it.
	Meta map[string]string `json:"meta,omitempty"`
}

// MetaReserved prefixes the meta keys the profile keeps for itself. Revision 1
// defines none, so every key under it is refused, on the way out as on the way
// in. The prefix keeps its historical spelling until a later revision.
const MetaReserved = "nightseam."

// EncodePath concatenates UTF-8 byte-length-prefixed scalar-string segments.
// The empty path is "", while a single empty segment is "0:".
func EncodePath(path []string) (string, error) {
	var encoded strings.Builder
	for _, segment := range path {
		if !utf8.ValidString(segment) {
			return "", ErrPath
		}
		encoded.WriteString(strconv.Itoa(len(segment)))
		encoded.WriteByte(':')
		encoded.WriteString(segment)
	}
	return encoded.String(), nil
}

// DecodePath accepts only the canonical form of EncodePath, without Unicode
// normalization or interpretation of dots, slashes or empty segments.
func DecodePath(encoded string) ([]string, error) {
	path := []string{}
	for encoded != "" {
		colon := strings.IndexByte(encoded, ':')
		if colon <= 0 {
			return nil, ErrPath
		}
		digits := encoded[:colon]
		if len(digits) > 1 && digits[0] == '0' {
			return nil, ErrPath
		}
		for _, digit := range digits {
			if digit < '0' || digit > '9' {
				return nil, ErrPath
			}
		}
		length, err := strconv.ParseUint(digits, 10, 64)
		encoded = encoded[colon+1:]
		if err != nil || length > uint64(len(encoded)) {
			return nil, ErrPath
		}
		segment := encoded[:int(length)]
		if !utf8.ValidString(segment) {
			return nil, ErrPath
		}
		path = append(path, segment)
		encoded = encoded[int(length):]
	}
	return path, nil
}

// ValidPath says whether every segment is Unicode-scalar text: the access
// law's rule, independent of how the profile encodes a path.
func ValidPath(path []string) bool {
	for _, segment := range path {
		if !utf8.ValidString(segment) {
			return false
		}
	}
	return true
}

// frame and PublicError are what Decode reads an envelope into. Their names
// are v0.6.0's, because a decoder's error names the type it failed to fill,
// and a peer sends that error as the reason it refuses the frame.
type frame struct {
	Version     int               `json:"version"`
	Kind        string            `json:"kind"`
	ID          string            `json:"id,omitempty"`
	Method      string            `json:"method,omitempty"`
	Params      json.RawMessage   `json:"params,omitempty"`
	Result      json.RawMessage   `json:"result,omitempty"`
	Error       *PublicError      `json:"error,omitempty"`
	Event       string            `json:"event,omitempty"`
	Data        json.RawMessage   `json:"data,omitempty"`
	Traceparent string            `json:"traceparent,omitempty"`
	Tracestate  string            `json:"tracestate,omitempty"`
	Meta        map[string]string `json:"meta,omitempty"`
}

// PublicError is a decoded envelope's public error.
type PublicError struct {
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Decode admits one envelope exactly as the profile allows it.
func Decode(data []byte) (Frame, error) {
	decoded, err := decode(data)
	f := Frame{Version: decoded.Version, Kind: decoded.Kind, ID: decoded.ID, Method: decoded.Method,
		Params: decoded.Params, Result: decoded.Result, Event: decoded.Event, Data: decoded.Data,
		Traceparent: decoded.Traceparent, Tracestate: decoded.Tracestate, Meta: decoded.Meta}
	if decoded.Error != nil {
		f.Error = &wire.ProfileError{Code: decoded.Error.Code, Message: decoded.Error.Message, Data: decoded.Error.Data}
	}
	return f, err
}

func decode(data []byte) (frame, error) {
	var f frame
	if err := RawUnicode(data); err != nil {
		return f, err
	}
	// Validate members separately so duplicate fields and explicit members from
	// another frame kind cannot disappear into Go zero values while decoding.
	fields, err := frameMembers(data)
	if err != nil {
		return f, err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&f); err != nil {
		return f, fmt.Errorf("invalid duplex frame: %w", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return f, errors.New("invalid trailing duplex frame content")
	}
	if f.Version != 1 {
		return f, errors.New("unsupported duplex frame version")
	}
	valid := false
	allowed := map[string]bool{"version": true, "kind": true, "traceparent": true, "tracestate": true}
	switch f.Kind {
	case "request":
		allowed["id"], allowed["method"], allowed["params"], allowed["meta"] = true, true, true, true
		valid = f.ID != "" && f.Method != "" && len(f.Params) > 0 && len(f.Result) == 0 && f.Error == nil && f.Event == "" && len(f.Data) == 0
	case "response":
		allowed["id"], allowed["result"], allowed["error"] = true, true, true
		valid = f.ID != "" && f.Method == "" && len(f.Params) == 0 && (len(f.Result) > 0) != (f.Error != nil) && f.Event == "" && len(f.Data) == 0 && f.Meta == nil
		_, hasResult := fields["result"]
		_, hasError := fields["error"]
		valid = valid && hasResult != hasError
	case "event":
		allowed["event"], allowed["data"], allowed["meta"] = true, true, true
		valid = f.ID == "" && f.Method == "" && len(f.Params) == 0 && len(f.Result) == 0 && f.Error == nil && f.Event != "" && len(f.Data) > 0
	case "cancel":
		allowed["id"] = true
		valid = f.ID != "" && f.Method == "" && len(f.Params) == 0 && len(f.Result) == 0 && f.Error == nil && f.Event == "" && len(f.Data) == 0 && f.Meta == nil
	}
	for name := range fields {
		if !allowed[name] {
			valid = false
		}
	}
	if f.Error != nil && (f.Error.Code == "" || f.Error.Message == "") {
		valid = false
	}
	// A trace the peer cannot read is a trace it would carry wrongly; tracestate
	// has no form of its own and travels alone when an intermediary strips one.
	if _, traced := fields["traceparent"]; traced && !ValidTraceparent(f.Traceparent) {
		valid = false
	}
	// Meta maps names to strings and may be empty; keys under the reserved
	// prefix are the profile's to define and it defines none in this version,
	// so a frame carrying one is refused rather than read as a consumer's.
	if raw, carried := fields["meta"]; carried && !validMeta(raw) {
		valid = false
	}
	if !valid {
		return f, errors.New("invalid duplex frame shape")
	}
	return f, nil
}

func frameMembers(data []byte) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("duplex frame must be an object")
	}
	members := make(map[string]json.RawMessage)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return nil, err
		}
		name, ok := key.(string)
		if !ok {
			return nil, errors.New("invalid duplex field name")
		}
		if _, exists := members[name]; exists {
			return nil, fmt.Errorf("duplicate duplex field %q", name)
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return nil, err
		}
		members[name] = value
	}
	if _, err := d.Token(); err != nil {
		return nil, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return nil, errors.New("invalid trailing duplex frame content")
	}
	return members, nil
}

// validMeta holds meta to what a carriage is, reading the member as it was
// spelled rather than as the frame holds it: an object, since a member spelled
// null is not an absent one; every value a string, which map[string]string
// cannot tell from a null it would read as the empty one; and no key of the
// reserved prefix.
func validMeta(raw json.RawMessage) bool {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil || values == nil {
		return false
	}
	for key, value := range values {
		if strings.HasPrefix(key, MetaReserved) || len(value) == 0 || value[0] != '"' {
			return false
		}
	}
	return true
}

// ValidID holds a request identifier to the profile's form: the role's prefix
// followed by a decimal serial without leading zeros, at most 20 digits.
func ValidID(id, prefix string) bool {
	if !strings.HasPrefix(id, prefix) {
		return false
	}
	n := strings.TrimPrefix(id, prefix)
	if n == "" || n[0] == '0' {
		return false
	}
	for _, r := range n {
		if r < '0' || r > '9' {
			return false
		}
	}
	return len(n) <= 20
}

// ValidTraceparent holds a traceparent to the one form W3C Trace Context gives
// it: version, trace id, parent id and flags, lower-case hexadecimal, dashed.
func ValidTraceparent(value string) bool {
	if len(value) != 55 || value[2] != '-' || value[35] != '-' || value[52] != '-' {
		return false
	}
	for i := 0; i < len(value); i++ {
		if i == 2 || i == 35 || i == 52 {
			continue
		}
		if c := value[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Validate holds a structured frame to the envelope it would travel as, with
// name as its method or event, and to a carrier's frame limit. A logical return
// address identifies an origin independently of the carrier role, so either
// identifier prefix is valid before a peer remaps it.
func Validate(name string, value wire.ProfileFrame, limit int64) error {
	f := Frame{Version: value.Version, Kind: string(value.Kind), ID: value.ID,
		Params: value.Params, Result: value.Result, Data: value.Data,
		Traceparent: value.Traceparent, Tracestate: value.Tracestate, Meta: value.Meta}
	if value.Error != nil {
		copied := *value.Error
		f.Error = &copied
	}
	switch value.Kind {
	case wire.ProfileRequest:
		f.Method = name
	case wire.ProfileEvent:
		f.Event = name
	}
	data, err := MarshalJSON(f)
	if err != nil {
		return err
	}
	if limit > 0 && int64(len(data)) > limit {
		return errors.New("wire frame exceeds the carrier limit")
	}
	if _, err := Decode(data); err != nil {
		return err
	}
	if f.ID != "" && !ValidID(f.ID, "c:") && !ValidID(f.ID, "s:") {
		return errors.New("invalid wire request identifier")
	}
	return nil
}
