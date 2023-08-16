package main

import (
	"bytes"
	"crypto/md5"
	"errors"
	"sort"
	"time"

	"github.com/cbergoon/merkletree"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	// pb "github.com/andrew-delph/my-key-store/proto"
)

var hashMod = 999999

type CustomHash struct {
	value int
}

func (h *CustomHash) Add(b []byte) {
	sum := 0
	for _, value := range b {
		sum += int(value)
	}
	h.value += sum
	h.value = h.value % hashMod
}

func (h *CustomHash) Remove(b []byte) {
	sum := 0
	for _, value := range b {
		sum += int(value)
	}
	h.value -= sum
	for h.value < 0 {
		h.value += hashMod
	}
	h.value = h.value % hashMod
}

func (h *CustomHash) Hash() int {
	return h.value
}

var merkletreeStore *cache.Cache = cache.New(0*time.Minute, 1*time.Minute)

type MerkleContent struct {
	key   string
	value string
}

func (content MerkleContent) Equals(other MerkleContent) (bool, error) {
	return content.key == other.key && content.value == other.value, nil
}

func (content MerkleContent) CalculateHash() ([]byte, error) {
	h := md5.New()
	if _, err := h.Write([]byte(content.key + content.value)); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

type MerkleBucket struct {
	content []MerkleContent
	hash    []byte
}

func (content MerkleBucket) CalculateHash() ([]byte, error) {
	return content.hash, nil
}

func (content MerkleBucket) Equals(other merkletree.Content) (bool, error) {
	otherTC, ok := other.(MerkleBucket)
	if !ok {
		return false, errors.New("value is not of type MerkleContent")
	}
	return bytes.Equal(content.hash, otherTC.hash), nil
}

func PartitionMerkleTree(partitionEpoch uint64, partitionId int) (*merkletree.MerkleTree, error) {
	partition, err := store.getPartition(partitionId)
	if err != nil {

		logrus.Debug(err)
		return nil, err
	}

	items := partition.Items()
	// if len(items) == 0 {
	// 	return nil, fmt.Errorf("partition.Items() is %d", 0)
	// }

	// Extract keys and sort them
	var keys []string
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Build content list in sorted order of keys
	bucketList := make([]MerkleBucket, partitionBuckets)

	for i := range bucketList {
		bucketList[i] = MerkleBucket{content: []MerkleContent{}}
	}

	for _, key := range keys {
		value := items[key]
		if value.Epoch > int64(partitionEpoch) {
			continue
		}
		bucketHash := CalculateHash(key)
		bucketList[bucketHash%partitionBuckets].content = append(bucketList[bucketHash%partitionBuckets].content, MerkleContent{key: value.Key, value: value.Value})
	}

	var contentList []merkletree.Content

	for _, bucket := range bucketList {
		contentList = append(contentList, merkletree.Content(bucket))
	}

	tree, err := merkletree.NewTree(contentList)
	if err != nil {
		logrus.Debug(err)
		return nil, err
	}
	// merkletreeStore.Add(fmt.Sprintf("%d-%d", partitionEpoch, epoch), tree, 0)

	return tree, nil
}
