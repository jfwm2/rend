package couchbase

import (
	"fmt"
	"math/rand"
	"testing"
)

var input = "ABC€"
var in = []byte(input)
var transcoder = CustomRawBinaryTranscoder{}

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
	flags := rand.Uint32()	//random uint32
	var out []byte
	err := transcoder.Decode(in, flags, &out)

	if err != nil {
		t.Errorf("Error is \"%v\"; want nil", err)
	} else if !testEq(in, out) {
		t.Errorf("out is %v; want %v", out, in)
	}
}

func TestCustomRawBinaryTranscoderDecodeToString(t *testing.T) {
	flags := rand.Uint32()	//random uint32
	var out string
	err := transcoder.Decode(in, flags, &out)

	if err != nil {
		t.Errorf("Error is \"%v\"; want nil", err)
	} else if input != fmt.Sprintf("%v", out) {
		t.Errorf("out is \"%v\"; want \"%v\"", out, input)
	}
}
func TestCustomRawBinaryTranscoderDecodeUnsupportedType(t *testing.T) {
	flags := rand.Uint32()	//random uint32
	var out []int8
	err := transcoder.Decode(in, flags, &out)

	if err == nil {
		t.Error("Error is nil; want \"to raise an error with unsupported type\"")
	}
}

func TestCustomRawBinaryTranscoderEncodeByte(t *testing.T) {
	out, typeCode, err := transcoder.Encode(in)

	if err != nil {
		t.Errorf("Error is \"%v\"; want nil", err)
	} else if typeCode != customRawBinaryTypeCode {
		t.Errorf("typeCode is is %v; want %v", typeCode, customRawBinaryTypeCode)
	} else if !testEq(in, out) {
		t.Errorf("out is %v; want %v", out, in)
	}
}

func TestCustomRawBinaryTranscoderEncodeBytePointer(t *testing.T) {
	out, typeCode, err := transcoder.Encode(&in)

	if err != nil {
		t.Errorf("Error is \"%v\"; want nil", err)
	} else if typeCode != customRawBinaryTypeCode {
		t.Errorf("typeCode is is %v; want %v", typeCode, customRawBinaryTypeCode)
	} else if !testEq(in, out) {
		t.Errorf("out is %v; want %v", out, in)
	}
}

func TestCustomRawBinaryTranscoderEncodeString(t *testing.T) {
	out, typeCode, err := transcoder.Encode(input)

	if err != nil {
		t.Errorf("Error is \"%v\"; want nil", err)
	} else if typeCode != customRawBinaryTypeCode {
		t.Errorf("typeCode is is %v; want %v", typeCode, customRawBinaryTypeCode)
	} else if !testEq(in, out) {
		t.Errorf("out is %v; want %v", out, in)
	}
}

func TestCustomRawBinaryTranscoderEncodeStringPointer(t *testing.T) {
	out, typeCode, err := transcoder.Encode(&input)

	if err != nil {
		t.Errorf("Error is \"%v\"; want nil", err)
	} else if typeCode != customRawBinaryTypeCode {
		t.Errorf("typeCode is is %v; want %v", typeCode, customRawBinaryTypeCode)
	} else if !testEq(in, out) {
		t.Errorf("out is %v; want %v", out, in)
	}
}

func TestCustomRawBinaryTranscoderEncodeUnsupported(t *testing.T) {
	transcoder := CustomRawBinaryTranscoder{}
	_, _, err := transcoder.Encode([]int8{1,2,3})

	if err == nil {
		t.Error("Error is nil; want \"to raise an error with unsupported type\"")
	}
}
