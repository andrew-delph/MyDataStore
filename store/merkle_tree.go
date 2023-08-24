package main

import (
	"bytes"
	"errors"
	"time"

	"github.com/cbergoon/merkletree"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"

	datap "github.com/andrew-delph/my-key-store/datap"
)

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

var merkletreeStore *cache.Cache = cache.New(0*time.Minute, 1*time.Minute)

type BaseMerkleBucket struct {
	bucketId int32
}

type SerializedMerkleBucket struct {
	BaseMerkleBucket
	hash []byte
}

func (bucket SerializedMerkleBucket) CalculateHash() ([]byte, error) {
	return bucket.hash, nil
}

func (bucket SerializedMerkleBucket) Equals(other merkletree.Content) (bool, error) {
	otherTC, ok := other.(SerializedMerkleBucket)
	if !ok {
		return false, errors.New("value is not of type MerkleContent")
	}
	return bytes.Equal(bucket.hash, otherTC.hash), nil
}

type RealMerkleBucket struct {
	BaseMerkleBucket
	hasher *CustomHash
}

func (bucket RealMerkleBucket) CalculateHash() ([]byte, error) {
	hash, err := EncodeInt64ToBytes(bucket.hasher.Hash())
	if err != nil {
		logrus.Errorf("MerkleBucket CalculateHash err = %v", err)
		return nil, err
	}
	return hash, nil
}

func (bucket RealMerkleBucket) AddValue(value *datap.Value) error {
	data, err := proto.Marshal(value)
	if err != nil {
		logrus.Error("Error: ", err)
		return err
	}
	bucket.hasher.Add(data)
	return nil
}

func (bucket RealMerkleBucket) RemoveValue(value *datap.Value) error {
	data, err := proto.Marshal(value)
	if err != nil {
		logrus.Error("Error: ", err)
		return err
	}
	bucket.hasher.Remove(data)
	return nil
}

func (bucket RealMerkleBucket) MergeBucket(other *RealMerkleBucket) error {
	bucket.hasher.Merge(other.hasher)
	return nil
}

func (content RealMerkleBucket) Equals(other merkletree.Content) (bool, error) {
	otherTC, ok := other.(RealMerkleBucket)
	if !ok {
		return false, errors.New("value is not of type MerkleContent")
	}
	return content.hasher.Hash() == otherTC.hasher.Hash(), nil
}

func RawPartitionMerkleTree(epoch int64, globalEpoch bool, partitionId int) (*merkletree.MerkleTree, []*RealMerkleBucket, error) {
	partition, err := store.getPartition(partitionId)
	if err != nil {
		logrus.Error(err)
		return nil, nil, err
	}
	var lowerEpoch int
	if globalEpoch {
		lowerEpoch = 0
	} else {
		lowerEpoch = int(epoch)
	}
	upperEpoch := int(epoch + 1)

	// Build content list in sorted order of keys
	bucketList := make([]*RealMerkleBucket, partitionBuckets)

	for i := range bucketList {
		bucket := RealMerkleBucket{hasher: &CustomHash{}, BaseMerkleBucket: BaseMerkleBucket{bucketId: int32(i)}}
		itemsMap := partition.Items(i, lowerEpoch, upperEpoch)
		for _, v := range itemsMap {
			bucket.AddValue(v)
		}

		bucketList[i] = &bucket
	}

	contentList := make([]merkletree.Content, partitionBuckets)
	for i := range bucketList {
		contentList[i] = bucketList[i]
	}

	tree, err := merkletree.NewTree(contentList)
	if err != nil {
		logrus.Debug(err)
		return nil, nil, err
	}
	// merkletreeStore.Add(fmt.Sprintf("%d-%d", partitionEpoch, epoch), tree, 0)

	return tree, bucketList, nil
}

func MerkleTreeToParitionEpochObject(tree *merkletree.MerkleTree, bucketList []*RealMerkleBucket, epoch int64, partitionId int) (*datap.ParitionEpochObject, error) {
	bucketHashes := make([][]byte, 0, partitionBuckets)
	for i, bucket := range bucketList {
		hash, err := bucket.CalculateHash()
		if err != nil {
			logrus.Errorf("bucket.CalculateHash err = %v", err)
			return nil, err
		}
		logrus.Debugf(">>> i = %d bucket = %d hash %v", i, bucket.bucketId, hash)
		bucketHashes = append(bucketHashes, hash)
	}

	bucketsReq := &datap.ParitionEpochObject{RootHash: tree.MerkleRoot(), Epoch: epoch, Partition: int32(partitionId), Buckets: bucketHashes}

	return bucketsReq, nil
}

func ParitionEpochObjectToMerkleTree(paritionEpochObject *datap.ParitionEpochObject) (*merkletree.MerkleTree, error) {
	contentList := make([]merkletree.Content, 0, partitionBuckets)
	for i, bucketHash := range paritionEpochObject.Buckets {
		contentList = append(contentList, &SerializedMerkleBucket{hash: bucketHash, BaseMerkleBucket: BaseMerkleBucket{bucketId: int32(i)}})
	}
	return merkletree.NewTree(contentList)
}

func DifferentMerkleTreeBuckets(tree1 *merkletree.MerkleTree, tree2 *merkletree.MerkleTree) []int32 {
	if tree1 == nil && tree2 == nil {
		return nil
	}

	return DifferentMerkleTreeBucketsDFS(tree1.Root, tree2.Root)
}

func DifferentMerkleTreeBucketsDFS(node1 *merkletree.Node, node2 *merkletree.Node) []int32 {
	differences := []int32{}

	if node1 == nil && node2 == nil {
		return differences
	}

	if !bytes.Equal(node1.Hash, node2.Hash) {
		if node1.Left == nil && node1.Right == nil {
			var bucketId1 int32
			switch bucket := node1.C.(type) {
			case *RealMerkleBucket:
				bucketId1 = bucket.BaseMerkleBucket.bucketId
			case *SerializedMerkleBucket:
				bucketId1 = bucket.BaseMerkleBucket.bucketId
			default:
				logrus.Fatalf("bucket type not found. %v", bucket)
			}

			var bucketId2 int32
			switch bucket2 := node2.C.(type) {
			case *RealMerkleBucket:
				bucketId2 = bucket2.BaseMerkleBucket.bucketId
				logrus.Warnf("RealMerkleBucket BUCKET DIFF: bucketId %d bucketId2 %d", bucketId1, bucketId2)
			case *SerializedMerkleBucket:
				bucketId2 = bucket2.BaseMerkleBucket.bucketId
				logrus.Warnf("SerializedMerkleBucket BUCKET DIFF: bucketId %d bucketId2 %d", bucketId1, bucketId2)
			default:
				logrus.Fatalf("bucket type not found. %v", bucket2)
			}

			if bucketId1 != bucketId2 {
				logrus.Fatalf("bucketIds dont match. bucketId1 %v bucketId2%v", bucketId1, bucketId2)
			}

			differences = append(differences, bucketId1)
		} else {
			// Recurse into child nodes
			differences = append(differences, DifferentMerkleTreeBucketsDFS(node1.Left, node2.Left)...)
			differences = append(differences, DifferentMerkleTreeBucketsDFS(node1.Right, node2.Right)...)
		}
	}

	return differences
}
