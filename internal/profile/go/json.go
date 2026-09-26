package runtime

import (
	"encoding/json"

	"github.com/Bitspark/nightseam/internal/scalarjson"

	jsonv2 "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
	jsonv1 "github.com/go-json-experiment/json/v1"
)

// ValidateUnicodeJSON checks original JSON text for malformed Unicode,
// including strings a decoder would discard as duplicate members. It does
// not replace JSON syntax or envelope validation performed by its caller.
func ValidateUnicodeJSON(data []byte) error { return scalarjson.Raw(data) }

// The external implementation has its own v1 Number type. Preserve the
// encoding/json.Number used by this runtime and by its existing consumers.
var wireMarshalers = jsonv2.MarshalToFunc(func(encoder *jsontext.Encoder, value json.Number) error {
	// With jsonv2 enabled the external Number aliases the standard type.
	// Disable this adapter in the inner call so that alias cannot recurse.
	return jsonv2.MarshalEncode(encoder, jsonv1.Number(value), jsonv2.WithMarshalers(nil))
})

// MarshalObject writes an object whose members are already encoded, in the
// order given. It is what a generated live codec builds its value with: a
// live value is assembled member by member, because each callable in it has
// to become a binding first, and a map would write the members in another
// order than the record declares them.
func MarshalObject(order []string, members map[string]json.RawMessage) ([]byte, error) {
	var out []byte
	out = append(out, '{')
	first := true
	for _, name := range order {
		member, present := members[name]
		if !present {
			continue
		}
		if !first {
			out = append(out, ',')
		}
		first = false
		key, err := MarshalJSON(name)
		if err != nil {
			return nil, err
		}
		out = append(out, key...)
		out = append(out, ':')
		out = append(out, member...)
	}
	out = append(out, '}')
	if err := ValidateUnicodeJSON(out); err != nil {
		return nil, err
	}
	return out, nil
}

// MarshalJSON encodes a wire value without replacing malformed Unicode.
// Generated codecs use it before validation, while invalid UTF-8 in Go
// strings and unpaired surrogate escapes in raw encodings are still visible.
func MarshalJSON(value any) ([]byte, error) {
	// Preserve the standard encoder's field, omission, number and method
	// rules, changing only Unicode replacement. Validating custom text
	// afterward is too late; invoking its encoder twice changes its behavior.
	return jsonv2.Marshal(value, jsonv1.DefaultOptionsV1(), jsontext.AllowInvalidUTF8(false), jsonv2.WithMarshalers(wireMarshalers))
}
