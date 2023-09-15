package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexManager(t *testing.T) {
	parition := 1
	bucket := uint64(2)
	epoch := int64(3)
	key := "zz"
	index1, err := BuildEpochIndex(parition, bucket, epoch, key)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "epoch_1_2_0000000003_zz", index1, "equal index")
	paritionParsed, bucketParsed, epochParsed, keyParsed, err := ParseEpochIndex(index1)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, parition, paritionParsed, "parsed parition")
	assert.Equal(t, bucket, bucketParsed, "parsed bucket")
	assert.Equal(t, epoch, epochParsed, "parsed epoch")
	assert.Equal(t, key, keyParsed, "parsed key")

	index2, err := BuildEpochTreeObjectIndex(1, 2)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "epochtree_1_0000000002", index2, "equal index")
}
