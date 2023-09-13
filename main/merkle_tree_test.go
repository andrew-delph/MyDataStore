package main

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/andrew-delph/my-key-store/config"
	"github.com/andrew-delph/my-key-store/rpc"
)

func TestMerkleTreeRaw(t *testing.T) {
	var err error

	writeValuesNum := 10
	if testing.Short() {
		writeValuesNum = 10
		// t.Skip("skipping test in short mode.")
	}

	tmpDir := t.TempDir()
	logrus.Info("Temporary Directory:", tmpDir)

	c := config.GetConfig()
	c.Storage.DataPath = tmpDir
	c.Manager.PartitionCount = 1
	c.Manager.PartitionBuckets = 30

	manager := NewManager(c)
	timestamp := int64(0)

	// write to epoch 1
	for i := 0; i < writeValuesNum; i++ {
		timestamp++
		k := fmt.Sprintf("key%d", i)
		v := fmt.Sprintf("val%d", i)
		setVal := &rpc.RpcValue{Key: k, Value: v, Epoch: 1, UnixTimestamp: timestamp}
		err = manager.SetValue(setVal)
		if err != nil {
			t.Error(err)
		}
		getVal, err := manager.GetValue(k)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, v, getVal.Value, "get value is wrong")
	}

	// write to epoch 2
	for i := 0; i < writeValuesNum; i++ {
		timestamp++
		k := fmt.Sprintf("keyz%d", i)
		v := fmt.Sprintf("valz%d", i)
		setVal := &rpc.RpcValue{Key: k, Value: v, Epoch: 2, UnixTimestamp: timestamp}
		err = manager.SetValue(setVal)
		if err != nil {
			t.Error(err)
		}
		getVal, err := manager.GetValue(k)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, v, getVal.Value, "get value is wrong")
	}

	// check iterator for both...

	tree1, err := manager.RawPartitionMerkleTree(0, 0, 3)
	if err != nil {
		t.Error(err)
	}
	tree2, err := manager.RawPartitionMerkleTree(0, 1, 3)
	if err != nil {
		t.Error(err)
	}

	tree3, err := manager.RawPartitionMerkleTree(0, 2, 3)
	if err != nil {
		t.Error(err)
	}

	assert.EqualValues(t, c.Manager.PartitionBuckets, len(tree1.Leafs), "confirm leafs num is the same as PartitionBuckets")
	assert.EqualValues(t, c.Manager.PartitionBuckets, len(tree2.Leafs), "confirm leafs num is the same as PartitionBuckets")
	assert.EqualValues(t, c.Manager.PartitionBuckets, len(tree3.Leafs), "confirm leafs num is the same as PartitionBuckets")

	assert.EqualValues(t, tree1.MerkleRoot(), tree2.MerkleRoot(), "hash should be the same")
	assert.NotEqualValues(t, tree1.MerkleRoot(), tree3.MerkleRoot(), "hash should be different")
	assert.NotEqualValues(t, tree2.MerkleRoot(), tree3.MerkleRoot(), "hash should be different")

	// verify the leafs of each tree to have correct bucketId
	for i, leaf := range tree1.Leafs {
		switch bucket := leaf.C.(type) {
		case *MerkleBucket:
			assert.EqualValues(t, i, bucket.bucketId, "wrong bucket id")
		default:
			t.Errorf("bucket type not found. %v", reflect.TypeOf(bucket))
		}
	}
	for i, leaf := range tree2.Leafs {
		switch bucket := leaf.C.(type) {
		case *MerkleBucket:
			assert.EqualValues(t, i, bucket.bucketId, "wrong bucket id")
		default:
			t.Errorf("bucket type not found. %v", reflect.TypeOf(bucket))
		}
	}
	for i, leaf := range tree3.Leafs {
		switch bucket := leaf.C.(type) {
		case *MerkleBucket:
			assert.EqualValues(t, i, bucket.bucketId, "wrong bucket id")
		default:
			t.Errorf("bucket type not found. %v", reflect.TypeOf(bucket))
		}
	}

	// create a rpc.RpcEpochTreeObject for each tree

	obj1, err := MerkleTreeToPartitionEpochObject(tree1, 0, 0, 3)
	if err != nil {
		t.Error(err)
	}
	obj2, err := MerkleTreeToPartitionEpochObject(tree2, 0, 1, 3)
	if err != nil {
		t.Error(err)
	}
	obj3, err := MerkleTreeToPartitionEpochObject(tree3, 0, 2, 3)
	if err != nil {
		t.Error(err)
	}

	// convert back to trees
	stree1, err := EpochTreeObjectToMerkleTree(obj1)
	if err != nil {
		t.Error(err)
	}
	stree2, err := EpochTreeObjectToMerkleTree(obj2)
	if err != nil {
		t.Error(err)
	}
	stree3, err := EpochTreeObjectToMerkleTree(obj3)
	if err != nil {
		t.Error(err)
	}

	assert.EqualValues(t, tree1.MerkleRoot(), stree1.MerkleRoot(), "hash should be the same")
	assert.EqualValues(t, tree2.MerkleRoot(), stree2.MerkleRoot(), "hash should be the same")
	assert.EqualValues(t, tree3.MerkleRoot(), stree3.MerkleRoot(), "hash should be the same")

	diff1, err := DifferentMerkleTreeBuckets(tree1, tree2)
	if err != nil {
		t.Error(err)
	}

	diff2, err := DifferentMerkleTreeBuckets(tree2, tree1)
	if err != nil {
		t.Error(err)
	}

	diff3, err := DifferentMerkleTreeBuckets(tree1, tree3)
	if err != nil {
		t.Error(err)
	}

	diff4, err := DifferentMerkleTreeBuckets(tree1, stree1)
	if err != nil {
		t.Error(err)
	}

	diff5, err := DifferentMerkleTreeBuckets(tree1, stree3)
	if err != nil {
		t.Error(err)
	}

	assert.EqualValues(t, diff1, diff2, "diff should be the same")
	assert.EqualValues(t, 0, len(diff1), "should be no diff")
	assert.EqualValues(t, diff1, diff4, "diff should be the same")

	assert.NotEqualValues(t, 0, len(diff3), "there should be a diff")
	assert.NotEqualValues(t, 0, len(diff5), "there should be a diff")
}
