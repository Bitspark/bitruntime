package profile

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestUnicodeDomain reads the same bitwire/1 table as the TypeScript runtime.
// Decode only the table wrapper; the decoder must see each original JSON text.
func TestUnicodeDomain(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "vectors", "bitwire-1", "unicode.json"))
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
	if len(table.Rows) == 0 {
		t.Fatal("empty unicode table")
	}
	for _, row := range table.Rows {
		t.Run(row.Name, func(t *testing.T) {
			err := RawUnicode([]byte(row.Raw))
			if (err == nil) != row.Valid && row.Valid {
				t.Fatalf("raw valid=%v: %v", row.Valid, err)
			}
			frame := `{"version":1,"kind":"event","event":"probe","data":` + row.Raw + `}`
			_, err = Decode([]byte(frame))
			if (err == nil) != row.Valid {
				t.Fatalf("frame valid=%v: %v", row.Valid, err)
			}
		})
	}
}

func TestUnicodeBeforeGoJSONReplacement(t *testing.T) {
	for _, raw := range [][]byte{{'"', 0xff, '"'}, {'"', 0xed, 0xa0, 0x80, '"'}} {
		if err := RawUnicode(raw); err == nil {
			t.Fatal("accepted malformed UTF-8")
		}
	}
	for _, value := range []any{
		map[json.Number]int{json.Number(string([]byte{0xff})): 1},
		string([]byte{0xff}), map[string]any{"nested": []any{string([]byte{0xff})}},
		map[string]int{string([]byte{0xff}): 1}, json.RawMessage(`"\uD800"`),
		unicodeRaw(`"\uD800"`),
	} {
		if _, err := MarshalJSON(value); err == nil {
			t.Errorf("encoded malformed value %T", value)
		}
	}
}

type unicodeRaw string

func (v unicodeRaw) MarshalJSON() ([]byte, error) { return []byte(v), nil }
