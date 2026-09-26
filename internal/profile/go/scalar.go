package profile

import (
	"errors"
	"unicode/utf8"
)

var errUnicode = errors.New("invalid Unicode: expected Unicode scalar strings")

// RawUnicode checks original JSON text, including overwritten members. JSON syntax
// remains the decoder's job; this scan only rules out lossy string decoding.
func RawUnicode(data []byte) error {
	if !utf8.Valid(data) {
		return errUnicode
	}
	inString := false
	for i := 0; i < len(data); i++ {
		if data[i] == '"' {
			inString = !inString
			continue
		}
		if !inString || data[i] != '\\' {
			continue
		}
		i++
		if i >= len(data) || data[i] != 'u' {
			continue
		}
		unit, ok := hexUnit(data, i+1)
		if !ok {
			continue
		}
		i += 4
		if unit >= 0xdc00 && unit <= 0xdfff {
			return errUnicode
		}
		if unit < 0xd800 || unit > 0xdbff {
			continue
		}
		if i+2 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
			return errUnicode
		}
		low, ok := hexUnit(data, i+3)
		if !ok || low < 0xdc00 || low > 0xdfff {
			return errUnicode
		}
		i += 6
	}
	return nil
}

func hexUnit(data []byte, start int) (uint16, bool) {
	if start+4 > len(data) {
		return 0, false
	}
	var unit uint16
	for _, digit := range data[start : start+4] {
		unit <<= 4
		switch {
		case digit >= '0' && digit <= '9':
			unit |= uint16(digit - '0')
		case digit >= 'a' && digit <= 'f':
			unit |= uint16(digit - 'a' + 10)
		case digit >= 'A' && digit <= 'F':
			unit |= uint16(digit - 'A' + 10)
		default:
			return 0, false
		}
	}
	return unit, true
}
