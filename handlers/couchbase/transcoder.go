package couchbase

import (
	"errors"
)

const customRawBinaryTypeCode = 0x4352544f

type CustomRawBinaryTranscoder struct {
}

func (t CustomRawBinaryTranscoder) Decode(bytes []byte, flags uint32, out interface{}) error {
	switch typedOut := out.(type) {
	case *[]byte:
		*typedOut = bytes
		return nil
	case *string:
		*typedOut = string(bytes)
		return nil
	default:
		return errors.New("custom raw binary format must be encoded in a byte array or string")
	}
}

func (t CustomRawBinaryTranscoder) Encode(value interface{}) ([]byte, uint32, error) {
	// Since it is not intended to write to couchbase at the present time, the CustomRawBinaryTranscoder
	// Encode method is disabled as a defensive programming action until it could be tested and validated.
	return nil, customRawBinaryTypeCode,
		errors.New("encoding is disabled for CustomRawBinaryTranscoder")
	/*
	var bytes []byte

	switch typeValue := value.(type) {
	case []byte:
		bytes = typeValue
	case *[]byte:
		bytes = *typeValue
	case string:
		bytes = []byte(typeValue)
	case *string:
		bytes = []byte(*typeValue)
	default:
		return nil, customRawBinaryTypeCode,
			errors.New("raw binary custom format must be encoded from a byte array or string")
	}

	return bytes, customRawBinaryTypeCode, nil*/
}
