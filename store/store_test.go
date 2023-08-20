package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"

	pb "github.com/andrew-delph/my-key-store/proto"
)

func testValue(key, value string, epoch int) *pb.Value {
	unixTimestamp := int64(0)
	setReqMsg := &pb.Value{Key: key, Value: value, Epoch: int64(epoch), UnixTimestamp: unixTimestamp}
	return setReqMsg
}

func TestGoCacheStore(t *testing.T) {
	conf, delegate, events = GetConf()

	store = NewGoCacheStore()
	defer store.Close()

	err := store.setValue(testValue("key1", "value1", 1))
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

func TestLevelDbStoreSingle(t *testing.T) {
	conf, delegate, events = GetConf()
	var err error
	store, err = NewLevelDbStore()
	if err != nil {
		t.Error(fmt.Sprintf("NewLevelDbStore: %v", err))
	}
	defer store.Close()

	err = store.setValue(testValue("keyz", "value1", 1))
	if err != nil {
		t.Error(fmt.Sprintf("setValue error: %v", err))
	}

	value, exists, err := store.getValue("keyz")

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

var NumTestValues = 100

func TestGoCacheStoreSpeed(t *testing.T) {
	conf, delegate, events = GetConf()

	store = NewGoCacheStore()
	defer store.Close()

	startTime := time.Now()

	for i := 0; i < NumTestValues; i++ {
		store.setValue(testValue(fmt.Sprintf("keyz%d", i), fmt.Sprintf("value%d", i), 1))
	}

	elapsedTime := time.Since(startTime).Seconds()
	fmt.Printf("TestGoCacheStoreSpeed Elapsed Time: %.2f seconds\n", elapsedTime)
}

func TestLevelDbStoreSpeed(t *testing.T) {
	hostname = randomString(5)

	conf, delegate, events = GetConf()

	var err error
	store, err = NewLevelDbStore()
	if err != nil {
		t.Error(fmt.Sprintf("NewLevelDbStore: %v", err))
	}
	defer store.Close()

	startTime := time.Now()

	for i := 0; i < NumTestValues; i++ {
		store.setValue(testValue(fmt.Sprintf("keyz%d", i), fmt.Sprintf("value%d", i), 1))
	}

	elapsedTime := time.Since(startTime).Seconds()

	fmt.Printf("TestLevelDbStoreSpeed Elapsed Time: %.2f seconds\n", elapsedTime)
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

func TestExampleLevelDbIndex(t *testing.T) {
	db, err := leveldb.OpenFile(randomString(10), nil)
	defer db.Close()
	if err != nil {
		fmt.Println("Error creating/opening database:", err)
	}

	insertNum := 14

	curr := 0

	for e := 0; e < 100; e++ {
		for i := 0; i < insertNum; i++ {
			key := []byte(createTestKey(fmt.Sprintf("key%d", curr), 1, e))
			curr++
			value := []byte(fmt.Sprintf("value%d", i))
			err = db.Put(key, value, nil)
			if err != nil {
				fmt.Println("Error adding data to database:", err)
			}
		}
	}

	for i := 0; i < insertNum+7; i++ {
		key := []byte(createTestKey(fmt.Sprintf("key%d", i), 2, 1))
		value := []byte(fmt.Sprintf("value%d", i))
		err = db.Put(key, value, nil)
		if err != nil {
			fmt.Println("Error adding data to database:", err)
		}
	}

	for i := 0; i < insertNum+7; i++ {
		key := []byte(createTestKey(fmt.Sprintf("key%d", i), 3, 1))
		value := []byte(fmt.Sprintf("value%d", i))
		err = db.Put(key, value, nil)
		if err != nil {
			fmt.Println("Error adding data to database:", err)
		}
	}

	// Define start and end keys
	// start := []byte(createTestKey("key", 2, 0))
	// start := []byte(fmt.Sprintf("%04d_%04d", 2, 1))

	// start := []byte(fmt.Sprintf("%04d_%s", 1, epochString(1)))
	// end := []byte(fmt.Sprintf("%04d_%s", 1, epochString(2)))

	startRange, endRange := epochRange(0, 2)

	start := []byte(fmt.Sprintf("%04d_%s", 1, startRange))
	end := []byte(fmt.Sprintf("%04d_%s", 1, endRange))

	// Create a range that spans from start key to a key just after end key
	// rng := util.BytesPrefix(start)
	rng := &util.Range{Start: start, Limit: end}

	// Create an Iterator to iterate through the keys within the range
	iter := db.NewIterator(rng, nil)
	defer iter.Release()

	// Iterate through the keys within the range
	count := 0
	for iter.Next() {
		// key := iter.Key()
		// value := iter.Value()
		// fmt.Printf("Key: %s, Value: %s\n", key, value)
		// fmt.Printf("Key: %s\n", key)
		count++
	}

	fmt.Printf("\ncount: %d\n\n", count)

	fmt.Printf("\nRANGE: start = %s end = %s\n\n", string(start), string(end))

	assert.Equal(t, 0, count%insertNum, "insertNum and count should be equal")
	if err := iter.Error(); err != nil {
		fmt.Println("Iterator error:", err)
	}
}

func TestLevelDbIndex(t *testing.T) {
	hostname = randomString(5)

	conf, delegate, events = GetConf()

	var err error
	store, err = NewLevelDbStore()
	if err != nil {
		t.Error(fmt.Sprintf("NewLevelDbStore: %v", err))
		return
	}
	defer store.Close()

	testInsertNum := 300

	for i := 0; i < testInsertNum; i++ {
		err = store.setValue(testValue(fmt.Sprintf("keyzds%d", i), fmt.Sprintf("value%d", i), 2))
		if err != nil {
			t.Errorf("setValue: %v", err)
			return
		}
	}

	for i := 0; i < 33; i++ {
		err = store.setValue(testValue(fmt.Sprintf("keyzx%d", i), fmt.Sprintf("value%d", i), 11))
		if err != nil {
			t.Errorf("setValue: %v", err)
			return
		}
	}

	partitions := make([]int, partitionCount)

	for i := 0; i < partitionCount; i++ {
		partitions[i] = i
	}

	startTime := time.Now()

	allItemsMap := make(map[string]*pb.Value)

	for bucket := 0; bucket < partitionBuckets; bucket++ {
		// for bucket := 0; bucket < 1; bucket++ {
		items := store.Items(partitions, bucket, 0, 3)
		logrus.Infof("bucket = %d Number of items: %d", bucket, len(items))

		for itemKey, itemValue := range items {
			allItemsMap[itemKey] = itemValue
		}
	}

	assert.Equal(t, testInsertNum, len(allItemsMap), "allItemsMap does not have correct amount")

	allItemsMap = make(map[string]*pb.Value)

	for bucket := 0; bucket < partitionBuckets; bucket++ {
		// for bucket := 0; bucket < 1; bucket++ {
		items := store.Items(partitions, bucket, 5, 20)
		logrus.Infof("bucket = %d Number of items: %d", bucket, len(items))

		for itemKey, itemValue := range items {
			allItemsMap[itemKey] = itemValue
		}
	}

	assert.Equal(t, 33, len(allItemsMap), "allItemsMap does not have correct amount")

	elapsedTime := time.Since(startTime).Seconds()

	logrus.Infof("TestLevelDbIndex Elapsed Time: %.2f seconds\n", elapsedTime)
}

func TestMain22(t *testing.T) {
	logrus.Warn("HELLO1", raft.ServerID("HELLO"))
	logrus.Warn("HELLO2", raft.ServerID("HELLO"))
}
