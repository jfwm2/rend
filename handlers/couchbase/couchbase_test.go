package couchbase

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/netflix/rend/common"
)

type testClient struct {
	Open        bool
	Data        map[string]string
	raiseErrSet bool
	raiseErrGet bool
}

func (t *testClient) close() error {
	t.Open = false
	return nil
}

func (t *testClient) get(key []byte, data *[]byte) error {
	if t.raiseErrGet {
		return errors.New("dummyError")
	}
	getData, ok := t.Data[string(key)]
	*data = []byte(getData)
	if !ok {
		return errors.New(errNotFound)
	}
	return nil
}

func (t *testClient) set(key []byte, data []byte, exptime uint32) error {
	t.Data[string(key)] = string(data)
	if t.raiseErrSet {
		return errors.New("dummyError")
	}
	return nil
}

func TestCloseIsProperlyClosing(t *testing.T) {
	client := testClient{Open: true}
	testhandler := Handler{client: &client}
	testhandler.Close()
	if client.Open != false {
		t.Error("Error close is not propely closing handler")
	}
}

func TestSetIsProperlySetting(t *testing.T) {
	key := "test"
	data := "test"
	client := testClient{Open: true, Data: make(map[string]string)}
	testhandler := Handler{client: &client}
	testhandler.Set(common.SetRequest{Key: []byte(key), Data: []byte(data), Exptime: 100})
	if client.Data[key] != data {
		t.Error("Error set is not properly setting")
	}
}

func TestSetIsRaisingErros(t *testing.T) {
	key := "test"
	data := "test"
	client := testClient{Open: true, Data: make(map[string]string), raiseErrSet: true}
	testhandler := Handler{client: &client}
	err := testhandler.Set(common.SetRequest{Key: []byte(key), Data: []byte(data), Exptime: 100})
	if err.Error() != "dummyError" {
		t.Error("Error in set are not correctly propagated")
	}
}

func TestGetIsProperlySetting(t *testing.T) {
	key := "test"
	data := "test"
	dataStore := make(map[string]string)
	dataStore[key] = data
	client := testClient{Open: true, Data: dataStore}
	testhandler := Handler{client: &client}

	dataOut, errOut := testhandler.Get(common.GetRequest{Keys: [][]byte{[]byte(key)}, Opaques: []uint32{0}, Quiet: []bool{false}})

	select {
	case getData := <-dataOut:
		if string(getData.Data) != data {
			t.Errorf("Data mismatch from get: %s != %s", string(getData.Data), data)
		}
	case err := <-errOut:
		t.Errorf("Unexpected error on Get: %s", err)
	}
}

func TestGetIsHandlingMisses(t *testing.T) {
	key := "test"

	client := testClient{Open: true, Data: make(map[string]string)}
	testhandler := Handler{client: &client}

	dataOut, errOut := testhandler.Get(common.GetRequest{Keys: [][]byte{[]byte(key)}, Opaques: []uint32{0}, Quiet: []bool{false}})

	select {
	case getData := <-dataOut:
		if !getData.Miss {
			t.Errorf("Not found error are not handled")
		}
	case err := <-errOut:
		t.Errorf("Unexpected error on Get: %s", err)
	}
}

func TestGetIsHandlingErrors(t *testing.T) {
	key := "test"

	client := testClient{Open: true, Data: make(map[string]string), raiseErrGet: true}
	testhandler := Handler{client: &client}

	dataOut, errOut := testhandler.Get(common.GetRequest{Keys: [][]byte{[]byte(key)}, Opaques: []uint32{0}, Quiet: []bool{false}})

	select {
	case <-dataOut:
		t.Errorf("Unexpected errors are not handled")
	case err := <-errOut:
		if err.Error() != "dummyError" {
			t.Error("Error in set are not correctly propagated")
		}
	}
}

func TestMalformattedGetRequestReturnError(t *testing.T) {
	key := "test"

	client := testClient{Open: true, Data: make(map[string]string), raiseErrGet: true}
	testhandler := Handler{client: &client}

	dataOut, errOut := testhandler.Get(common.GetRequest{Keys: [][]byte{[]byte(key)}, Opaques: []uint32{}, Quiet: []bool{false}})

	select {
	case <-dataOut:
		t.Errorf("Unexpected errors are not handled")
	case <-errOut:
		return
	}
}

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