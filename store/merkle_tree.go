package main

import (
	"bytes"
	"crypto/md5"
	"errors"
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
	contentList []MerkleContent
	hash        []byte
	bucketId    int32
}

func (bucket MerkleBucket) CalculateHash() ([]byte, error) {
	h := md5.New()
	for _, content := range bucket.contentList {
		hash, err := content.CalculateHash()
		if err != nil {
			return nil, err
		}
		if _, err := h.Write(hash); err != nil {
			return nil, err
		}
	}
	return h.Sum(nil), nil
}

func (content MerkleBucket) Equals(other merkletree.Content) (bool, error) {
	otherTC, ok := other.(MerkleBucket)
	if !ok {
		return false, errors.New("value is not of type MerkleContent")
	}
	return bytes.Equal(content.hash, otherTC.hash), nil
}

func PartitionMerkleTree(treeEpoch uint64, partitionId int) (*merkletree.MerkleTree, error) {
	partition, err := store.getPartition(partitionId)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}

	// Build content list in sorted order of keys
	bucketList := make([]MerkleBucket, partitionBuckets)

	for i := range bucketList {
		bucket := MerkleBucket{contentList: []MerkleContent{}, bucketId: int32(i)}
		items := partition.Items(i, 0, int(treeEpoch))
		for _, item := range items {
			bucket.contentList = append(bucket.contentList, MerkleContent{key: item.Key, value: item.Value})
		}
		bucketList[i] = bucket
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
