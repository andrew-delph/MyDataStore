package main

import (
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

func TestStore(t *testing.T) {
	conf, delegate, events = GetConf()

	store = NewGoCacheStore()

	store.setValue(testValue("key1", "value1"))

	value, _, _ := store.getValue("key1")

	assert.Equal(t, value.Value, "value1", "Both should be SetMessage")
}
