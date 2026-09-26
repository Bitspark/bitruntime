// Package scalarjson guards Unicode before encoding/json can replace it.
// It is independent of the declaration language and the wire profile.
package scalarjson

import (
	"errors"
	"reflect"
	"unicode/utf8"
)

var errUnicode = errors.New("invalid Unicode: expected Unicode scalar strings")

// Raw checks original JSON text, including overwritten members. JSON syntax
// remains the decoder's job; this scan only rules out lossy string decoding.
func Raw(data []byte) error {
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

// Value checks in-memory descriptor and type-expression strings. It invokes
// no encoding hooks: a descriptor's spelling is what the interpreter reads.
func Value(value any) error { return check(reflect.ValueOf(value), map[visit]bool{}) }

type visit struct {
	typ reflect.Type
	ptr uintptr
}

func check(value reflect.Value, seen map[visit]bool) error {
	if !value.IsValid() {
		return nil
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return nil
		}
		return check(value.Elem(), seen)
	case reflect.Pointer, reflect.Map, reflect.Slice:
		if value.IsNil() {
			return nil
		}
		key := visit{value.Type(), value.Pointer()}
		if seen[key] {
			return nil
		}
		seen[key] = true
		defer delete(seen, key)
	}
	switch value.Kind() {
	case reflect.String:
		if !utf8.ValidString(value.String()) {
			return errUnicode
		}
	case reflect.Pointer:
		return check(value.Elem(), seen)
	case reflect.Map:
		iter := value.MapRange()
		for iter.Next() {
			if err := check(iter.Key(), seen); err != nil {
				return err
			}
			if err := check(iter.Value(), seen); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if err := check(value.Index(i), seen); err != nil {
				return err
			}
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			if !field.IsExported() {
				continue
			}
			if err := check(value.Field(i), seen); err != nil {
				return err
			}
		}
	}
	return nil
}
