package couchbase

import (
	"fmt"
	"math/rand"
	"testing"
)


// checks if two bytes arrays are the same
func testEq(a, b []byte) bool {
	// If one is nil, the other must also be nil.
	if (a == nil) != (b == nil) {
		return false;
	} else if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestCustomRawBinaryTranscoderDecodeToByteArray(t *testing.T) {
	transcoder := CustomRawBinaryTranscoder{}
	input := "ABC€"
	in := []byte(input)
	flags := rand.Uint32(); //random uint32
	var out []byte
	err := transcoder.Decode(in, flags, &out)

	if err != nil {
		t.Errorf("Error is \"%v\"; want nil", err)
	} else if !testEq(in, out) {
		t.Errorf("out is %v; want %v", out, in)
	}
}

func TestCustomRawBinaryTranscoderDecodeToString(t *testing.T) {
	transcoder := CustomRawBinaryTranscoder{}
	input := "ABC€"
	in := []byte(input)
	flags := rand.Uint32(); //random uint32
	var out string
	err := transcoder.Decode(in, flags, &out)

	if err != nil {
		t.Errorf("Error is \"%v\"; want nil", err)
	} else if input != fmt.Sprintf("%v", out) {
		t.Errorf("out is \"%v\"; want \"%v\"", out, input)
	}
}
