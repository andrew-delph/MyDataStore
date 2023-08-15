package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	pb "github.com/andrew-delph/my-key-store/proto"
)

func testValue(key, value string) *pb.Value {
	unixTimestamp := time.Now().Unix()
	setReqMsg := &pb.Value{Key: key, Value: value, Epoch: int64(2), UnixTimestamp: unixTimestamp}
	return setReqMsg
}

func TestGoCacheStore(t *testing.T) {

	conf, delegate, events = GetConf()

	store = NewGoCacheStore()

	err := store.setValue(testValue("key1", "value1"))
	if err != nil {
		t.Error(fmt.Sprintf("setValue error: %v", err))
	}

	value, exists, err := store.getValue("key1")

	if !exists {
		t.Error(fmt.Sprintf("exists is false: %v", err))
		return
	}

	if err != nil {
		t.Error(fmt.Sprintf("error is not nil: %v", err))
		return
	}

	assert.Equal(t, "value1", value.Value, "Both should be SetMessage")
}

func TestLevelDbStore(t *testing.T) {

	conf, delegate, events = GetConf()

	store = NewLevelDbStore()

	defer store.Close()

	err := store.setValue(testValue("key1", "value1"))
	if err != nil {
		t.Error(fmt.Sprintf("setValue error: %v", err))
	}

	value, exists, err := store.getValue("key1")

	if !exists {
		t.Error(fmt.Sprintf("exists is false: %v", err))
		return
	}

	if err != nil {
		t.Error(fmt.Sprintf("error is not nil: %v", err))
		return
	}

	assert.Equal(t, "value1", value.Value, "Both should be SetMessage")
}
