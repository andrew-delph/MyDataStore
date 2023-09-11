package main

import (
	"github.com/cbergoon/merkletree"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/utils"
)

func testMT() {
	logrus.Info("hi")
}

var hashMod = int64(999999)

type CustomHash struct {
	value int64
}

func (h *CustomHash) Add(b []byte) {
	sum := int64(0)
	for _, value := range b {
		sum += int64(value)
	}
	h.value += sum
	h.value = h.value % hashMod
}

func (h *CustomHash) Remove(b []byte) {
	sum := int64(0)
	for _, value := range b {
		sum += int64(value)
	}
	h.value -= sum
	for h.value < 0 {
		h.value += hashMod
	}
	h.value = h.value % hashMod
}

func (h *CustomHash) Merge(other *CustomHash) {
	h.value += other.value
	for h.value < 0 {
		h.value += hashMod
	}
	h.value = h.value % hashMod
}

func (h *CustomHash) Hash() int64 {
	return h.value
}

type BaseMerkleBucket struct {
	bucketId int32
}

type RealMerkleBucket struct {
	BaseMerkleBucket
	hasher *CustomHash
}

func (bucket RealMerkleBucket) CalculateHash() ([]byte, error) {
	hash, err := utils.EncodeInt64ToBytes(bucket.hasher.Hash())
	if err != nil {
		logrus.Errorf("MerkleBucket CalculateHash err = %v", err)
		return nil, err
	}
	return hash, nil
}

func (bucket RealMerkleBucket) AddItem(bytes []byte) error {
	bucket.hasher.Add(bytes)
	return nil
}

func (content RealMerkleBucket) Equals(other merkletree.Content) (bool, error) {
	otherTC, ok := other.(RealMerkleBucket)
	if !ok {
		return false, errors.New("value is not of type MerkleContent")
	}
	return content.hasher.Hash() == otherTC.hasher.Hash(), nil
}

func (manager *Manager) RawPartitionMerkleTree(partitionId int, lowerEpoch, upperEpoch int64) (*merkletree.MerkleTree, error) {
	// Build content list in sorted order of keys
	bucketList := make([]merkletree.Content, manager.config.Manager.PartitionBuckets)

	for i := 0; i < manager.config.Manager.PartitionBuckets; i++ {
		bucket := RealMerkleBucket{hasher: &CustomHash{}, BaseMerkleBucket: BaseMerkleBucket{bucketId: int32(i)}}
		it := manager.db.NewIterator([]byte(EpochIndex(partitionId, i, int(lowerEpoch), "")), []byte(EpochIndex(partitionId, i, int(upperEpoch), "")))
		for !it.IsDone() {
			bucket.AddItem(it.Value())
			it.Next()
		}
		it.Release()

		bucketList[i] = &bucket
	}

	return merkletree.NewTree(bucketList)

	// contentList := make([]merkletree.Content, theManager.Config.Manager.PartitionBuckets)
	// for i := range bucketList {
	// 	contentList[i] = bucketList[i]
	// }

	// tree, err := merkletree.NewTree(contentList)
	// if err != nil {
	// 	logrus.Debug(err)
	// 	return nil, nil, err
	// }

	// return tree, bucketList, nil
}
