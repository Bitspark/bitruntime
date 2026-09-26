package runtime

import (
	"encoding/json"
	"testing"
)

type unicodeText struct {
	calls *int
	text  string
}

func (v unicodeText) MarshalText() ([]byte, error) { *v.calls++; return []byte(v.text), nil }

// Encoder hooks must run exactly once, and malformed text must be refused
// before encoding/json has a chance to replace it with an ordinary U+FFFD.
func TestStrictEncoderCallsHooksOnce(t *testing.T) {
	for _, text := range []string{"hello", "😀�", string([]byte{0xff})} {
		calls := 0
		_, err := MarshalJSON(struct{ Text unicodeText }{unicodeText{&calls, text}})
		if calls != 1 {
			t.Fatalf("MarshalText called %d times", calls)
		}
		if (err != nil) != (text == string([]byte{0xff})) {
			t.Fatalf("text %q: %v", text, err)
		}
		calls = 0
		_, err = MarshalJSON(map[unicodeText]string{{&calls, text}: "value"})
		if calls != 1 || (err != nil) != (text == string([]byte{0xff})) {
			t.Fatalf("map key %q: calls=%d, err=%v", text, calls, err)
		}
	}
	for _, raw := range []string{`"\uD800"`, `{"x":"\uD800","x":"valid"}`, string([]byte{'"', 0xff, '"'})} {
		if _, err := MarshalJSON(struct{ Raw unicodeRaw }{unicodeRaw(raw)}); err == nil {
			t.Fatalf("accepted custom output %q", raw)
		}
	}
}

func TestStrictEncoderPreservesValidEncoding(t *testing.T) {
	type embedded struct{ Visible string }
	for _, value := range []any{
		[]json.Number{"", "1e100", "-2"},
		map[json.Number]int{"not-a-number": 1},
		struct {
			Number json.Number `json:",string"`
		}{json.Number("1e100")},
		struct {
			embedded
			Omitted string `json:"-"`
			Empty   string `json:",omitempty"`
			Number  int    `json:",string"`
		}{embedded{"<😀�>"}, string([]byte{0xff}), "", 42},
		struct {
			Absent Optional[string] `json:",omitzero"`
			Null   Nullable[string]
		}{Optional[string]{Value: string([]byte{0xff})}, Null[string]()},
		map[string]any{"z": json.Number("1e100"), "a": []byte{0xff, 0xfe}, "raw": json.RawMessage(`{"a": 1}`)},
	} {
		want, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		got, err := MarshalJSON(value)
		if err != nil || string(got) != string(want) {
			t.Fatalf("strict=%s, std=%s, err=%v", got, want, err)
		}
	}
}
