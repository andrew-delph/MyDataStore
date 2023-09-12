package main

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/andrew-delph/my-key-store/config"
	"github.com/andrew-delph/my-key-store/rpc"
)

func TestMerkleTreeRaw(t *testing.T) {
	var err error
	// if testing.Short() {
	// 	t.Skip("skipping test in short mode.")
	// }

	c := config.GetConfig()
	c.Storage.DataPath = t.TempDir()
	c.Manager.PartitionCount = 1
	c.Manager.PartitionBuckets = 30

	manager := NewManager(c)

	writeValuesNum := 10

	// write to epoch 1
	for i := 0; i < writeValuesNum; i++ {
		k := fmt.Sprintf("key%d", i)
		v := fmt.Sprintf("val%d", i)
		setVal := &rpc.RpcValue{Key: k, Value: v, Epoch: 1}
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
		k := fmt.Sprintf("keyz%d", i)
		v := fmt.Sprintf("valz%d", i)
		setVal := &rpc.RpcValue{Key: k, Value: v, Epoch: 2}
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

	assert.EqualValues(t, tree1.MerkleRoot(), tree2.MerkleRoot(), "hash should be the same")

	tree3, err := manager.RawPartitionMerkleTree(0, 2, 3)
	if err != nil {
		t.Error(err)
	}

	assert.EqualValues(t, c.Manager.PartitionBuckets, len(tree3.Leafs), "checking content size")

	assert.NotEqualValues(t, tree1.MerkleRoot(), tree3.MerkleRoot(), "hash should be the same")

	for i, leaf := range tree1.Leafs {
		switch bucket := leaf.C.(type) {
		case *RealMerkleBucket:
			assert.EqualValues(t, i, bucket.bucketId, "wrong bucket id")
		default:
			t.Errorf("bucket type not found. %v", reflect.TypeOf(bucket))
		}
	}

	for i, leaf := range tree2.Leafs {
		switch bucket := leaf.C.(type) {
		case *RealMerkleBucket:
			assert.EqualValues(t, i, bucket.bucketId, "wrong bucket id")
		default:
			t.Errorf("bucket type not found. %v", reflect.TypeOf(bucket))
		}
	}

	for i, leaf := range tree3.Leafs {
		switch bucket := leaf.C.(type) {
		case *RealMerkleBucket:
			assert.EqualValues(t, i, bucket.bucketId, "wrong bucket id")
		default:
			t.Errorf("bucket type not found. %v", reflect.TypeOf(bucket))
		}
	}
}
