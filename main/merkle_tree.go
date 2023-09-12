package main

import (
	"reflect"

	"github.com/cbergoon/merkletree"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/rpc"
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

type MerkleBucket struct {
	bucketId int32
	hasher   *CustomHash
}

func (bucket MerkleBucket) CalculateHash() ([]byte, error) {
	hash, err := utils.EncodeInt64ToBytes(bucket.hasher.Hash())
	if err != nil {
		logrus.Errorf("MerkleBucket CalculateHash err = %v", err)
		return nil, err
	}
	return hash, nil
}

func (bucket MerkleBucket) AddItem(bytes []byte) error {
	bucket.hasher.Add(bytes)
	return nil
}

func (content MerkleBucket) Equals(other merkletree.Content) (bool, error) {
	otherTC, ok := other.(MerkleBucket)
	if !ok {
		return false, errors.New("value is not of type MerkleContent")
	}
	return content.hasher.Hash() == otherTC.hasher.Hash(), nil
}

func (manager *Manager) RawPartitionMerkleTree(partitionId int, lowerEpoch, upperEpoch int64) (*merkletree.MerkleTree, error) {
	// Build content list in sorted order of keys
	bucketList := make([]merkletree.Content, manager.config.Manager.PartitionBuckets)

	for i := 0; i < manager.config.Manager.PartitionBuckets; i++ {
		bucket := MerkleBucket{hasher: &CustomHash{}, bucketId: int32(i)}
		it := manager.db.NewIterator([]byte(EpochIndex(partitionId, i, int(lowerEpoch), "")), []byte(EpochIndex(partitionId, i, int(upperEpoch), "")))
		for !it.IsDone() {
			bucket.AddItem(it.Value())
			it.Next()
		}
		it.Release()

		bucketList[i] = &bucket
	}

	return merkletree.NewTree(bucketList)
}

func MerkleTreeToPartitionEpochObject(tree *merkletree.MerkleTree, partitionId int, lowerEpoch, upperEpoch int64) (*rpc.RpcEpochTreeObject, error) {
	bucketHashes := make([][]byte, 0)
	for _, leaf := range tree.Leafs {
		bucket, ok := leaf.C.(*MerkleBucket)
		if !ok {
			return nil, errors.Errorf("failed to decode MerkleBucket. type = %s", reflect.TypeOf(leaf))
		}
		hash, err := bucket.CalculateHash()
		if err != nil {
			return nil, errors.Wrap(err, "MerkleTreeToPartitionEpochObject")
		}
		bucketHashes = append(bucketHashes, hash)
	}

	epochTreeObject := &rpc.RpcEpochTreeObject{LowerEpoch: lowerEpoch, UpperEpoch: upperEpoch, Partition: int32(partitionId), Buckets: bucketHashes}
	return epochTreeObject, nil
}

func EpochTreeObjectToMerkleTree(partitionEpochObject *rpc.RpcEpochTreeObject) (*merkletree.MerkleTree, error) {
	contentList := make([]merkletree.Content, 0)
	for i, bucketHash := range partitionEpochObject.Buckets {
		hashInt64, err := utils.DecodeBytesToInt64(bucketHash)
		if err != nil {
			return nil, err
		}
		contentList = append(contentList, &MerkleBucket{hasher: &CustomHash{value: hashInt64}, bucketId: int32(i)})
	}
	return merkletree.NewTree(contentList)
}
