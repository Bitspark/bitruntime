package runtime

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// TestUnicodeDomain is the same source table read by the TypeScript runtime.
// Decode only the table wrapper; the runtime must see each original JSON text.
func TestUnicodeDomain(t *testing.T) {
	data, err := os.ReadFile("../../conformance/tables/unicode.json")
	if err != nil {
		t.Fatal(err)
	}
	var table struct {
		Rows []struct {
			Name, Raw string
			Valid     bool
		}
	}
	if err := json.Unmarshal(data, &table); err != nil {
		t.Fatal(err)
	}
	schema := MustSchema(`{"types":{}}`, "", nil)
	for _, row := range table.Rows {
		t.Run(row.Name, func(t *testing.T) {
			err := schema.ValidateExpressionRaw("json", []byte(row.Raw))
			if (err == nil) != row.Valid {
				t.Fatalf("raw valid=%v: %v", row.Valid, err)
			}
			if !row.Valid && err.Error() != "invalid Unicode: expected Unicode scalar strings" {
				t.Fatalf("diagnostic: %v", err)
			}
			frame := `{"version":1,"kind":"event","event":"probe","data":` + row.Raw + `}`
			_, err = decodeFrame([]byte(frame))
			if (err == nil) != row.Valid {
				t.Fatalf("frame valid=%v: %v", row.Valid, err)
			}
			// Even an unused descriptor extension cannot normalize a string.
			_, err = NewSchema([]byte(`{"types":{},"extension":`+row.Raw+`}`), "", nil)
			if (err == nil) != row.Valid {
				t.Fatalf("descriptor valid=%v: %v", row.Valid, err)
			}
		})
	}
}

func TestUnicodeBeforeGoJSONReplacement(t *testing.T) {
	schema := MustSchema(`{"types":{}}`, "", nil)
	for _, raw := range [][]byte{[]byte{'"', 0xff, '"'}, []byte{'"', 0xed, 0xa0, 0x80, '"'}} {
		if err := schema.ValidateExpressionRaw("json", raw); err == nil {
			t.Fatal("accepted malformed UTF-8")
		}
		if _, err := NewSchema(append(append([]byte(`{"types":{},"x":`), raw...), '}'), "", nil); err == nil {
			t.Fatal("accepted malformed descriptor UTF-8")
		}
	}
	for _, value := range []any{
		map[json.Number]int{json.Number(string([]byte{0xff})): 1},
		string([]byte{0xff}), map[string]any{"nested": []any{string([]byte{0xff})}},
		map[string]int{string([]byte{0xff}): 1}, json.RawMessage(`"\uD800"`),
		unicodeRaw(`"\uD800"`), Some(string([]byte{0xff})), NonNull(string([]byte{0xff})),
	} {
		if err := schema.ValidateValue("json", value); err == nil {
			t.Errorf("accepted malformed value %T", value)
		}
	}
	for _, raw := range []string{`{"types":{"Literal":{"kind":"alias","type":{"literal":"\uD800"}}}}`, `{"types":{"Enum":{"kind":"enum","values":["\uDC00"]}}}`, `{"types":{"Union":{"kind":"union","tag":"kind","value":"value","variants":{"\uD800":{"empty":true}}}}}`} {
		if _, err := NewSchema([]byte(raw), "", nil); err == nil {
			t.Fatalf("descriptor normalized: %s", raw)
		}
	}
	if err := schema.ValidateExpressionRaw(map[string]any{"literal": string([]byte{0xff})}, []byte(`"�"`)); err == nil || !strings.Contains(err.Error(), "Unicode") {
		t.Fatalf("in-memory expression: %v", err)
	}
}

type unicodeRaw string

func (v unicodeRaw) MarshalJSON() ([]byte, error) { return []byte(v), nil }
