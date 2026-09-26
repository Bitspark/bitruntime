package profile

import (
	"slices"
	"testing"
)

func TestPathEncodingIsCanonical(t *testing.T) {
	for _, path := range [][]string{{}, {""}, {"a/b"}, {"a", "b"}, {"é", "", "0:"}} {
		encoded, err := EncodePath(path)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodePath(encoded)
		if err != nil || !slices.Equal(decoded, path) {
			t.Fatalf("%q -> %q -> %q, %v", path, encoded, decoded, err)
		}
	}
	for _, encoded := range []string{"1", ":", "01:a", "2:a", "1:\xff", "x:", "-1:a"} {
		if _, err := DecodePath(encoded); err == nil {
			t.Fatalf("accepted %q", encoded)
		}
	}
	if _, err := EncodePath([]string{"\xff"}); err == nil {
		t.Fatal("encoded invalid UTF-8")
	}
	if got, _ := EncodePath([]string{"echo"}); got != "4:echo" {
		t.Fatal(got)
	}
}
