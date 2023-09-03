package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"

	datap "github.com/andrew-delph/my-key-store/datap"
)

func testValue(key, value string, epoch int) *datap.Value {
	unixTimestamp := int64(0)
	setReqMsg := &datap.Value{Key: key, Value: value, Epoch: int64(epoch), UnixTimestamp: unixTimestamp}
	return setReqMsg
}

func TestGoCacheStore(t *testing.T) {
	t.Error("TODO FIX.")
	// conf, delegate, events = CreateMemberlistConf()

	// store = NewGoCacheStore()
	// defer store.Close()

	// err := store.SetValue(testValue("key1", "value1", 1))
	// if err != nil {
	// 	t.Error(fmt.Sprintf("SetValue error: %v", err))
	// }

	// value, exists, err := store.GetValue("key1")

	// if !exists {
	// 	t.Error(fmt.Sprintf("exists is false: %v", err))
	// 	return
	// }

	// if err != nil {
	// 	t.Error(fmt.Sprintf("error is not nil: %v", err))
	// 	return
	// }

	// assert.Equal(t, "value1", value.Value, "Both should be SetMessage")
}

var NumTestValues = 100

func TestGoCacheStoreSpeed(t *testing.T) {
	t.Error("TODO FIX.")

	// conf, delegate, events = CreateMemberlistConf()

	// store = NewGoCacheStore()
	// defer store.Close()

	// startTime := time.Now()

	// for i := 0; i < NumTestValues; i++ {
	// 	store.SetValue(testValue(fmt.Sprintf("keyz%d", i), fmt.Sprintf("value%d", i), 1))
	// }

	// elapsedTime := time.Since(startTime).Seconds()
	// fmt.Printf("TestGoCacheStoreSpeed Elapsed Time: %.2f seconds\n", elapsedTime)
}

func createTestKey(key string, bucket, epoch int) string {
	epochStr := epochString(epoch)
	return fmt.Sprintf("%04d_%s_%s", bucket, epochStr, key)
}

func reverseString(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func epochString(epoch int) string {
	epochStr := fmt.Sprintf("%d", epoch)

	keyStr := fmt.Sprintf("%s%s", strings.Repeat("0", 4-len(epochStr)), epochStr)

	return keyStr
}

func epochRange(start, end int) (string, string) {
	return epochString(start), epochString(end)
}

func TestMain22(t *testing.T) {
	logrus.Warn("HELLO1", raft.ServerID("HELLO"))
	logrus.Warn("HELLO2", raft.ServerID("HELLO"))
}
