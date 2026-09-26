package profile

import (
	"encoding/json"

	jsonv2 "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
	jsonv1 "github.com/go-json-experiment/json/v1"
)

// ValidateUnicodeJSON checks original JSON text for malformed Unicode,
// including strings a decoder would discard as duplicate members. It does
// not replace JSON syntax or envelope validation performed by its caller.
func ValidateUnicodeJSON(data []byte) error { return RawUnicode(data) }

// The external implementation has its own v1 Number type. Preserve the
// encoding/json.Number used by this runtime and its consumers.
var wireMarshalers = jsonv2.MarshalToFunc(func(encoder *jsontext.Encoder, value json.Number) error {
	// With jsonv2 enabled the external Number aliases the standard type.
	// Disable this adapter in the inner call so that alias cannot recurse.
	return jsonv2.MarshalEncode(encoder, jsonv1.Number(value), jsonv2.WithMarshalers(nil))
})

// MarshalJSON encodes a wire value without replacing malformed Unicode, so
// invalid UTF-8 in Go strings and unpaired surrogate escapes in raw encodings
// are refused rather than silently repaired.
func MarshalJSON(value any) ([]byte, error) {
	// Preserve the standard encoder's field, omission, number and method
	// rules, changing only Unicode replacement. Validating custom text
	// afterward is too late; invoking its encoder twice changes its behavior.
	return jsonv2.Marshal(value, jsonv1.DefaultOptionsV1(), jsontext.AllowInvalidUTF8(false), jsonv2.WithMarshalers(wireMarshalers))
}
