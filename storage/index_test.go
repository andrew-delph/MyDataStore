package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexStorage(t *testing.T) {
	index, err := NewIndex("test").
		AddColumn(CreateUnorderedColumn("partition", "part1")).
		AddColumn(CreateUnorderedColumn("bucket", "bucket1")).
		AddColumn(CreateOrderedColumn("epoch", "1", 4)).
		AddColumn(CreateUnorderedColumn("key", "mykey")).
		Build()
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, "test_part1_bucket1_0001_mykey", index, "index should be equal")

	index2 := NewIndex("zzz").
		AddColumn(CreateUnorderedColumn("partition", "part1")).
		AddColumn(CreateUnorderedColumn("bucket", "bucket1")).
		AddColumn(CreateOrderedColumn("epoch", "100", 4)).
		AddColumn(CreateUnorderedColumn("key", "mykey"))
	index2Str, err := index2.Build()
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, "zzz_part1_bucket1_0100_mykey", index2Str, "index2 should be equal")
	index2Parse, err := index2.Parse(index2Str)
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, "zzz", index2Parse["name"], "should be equal")
	assert.EqualValues(t, "part1", index2Parse["partition"], "should be equal")
	assert.EqualValues(t, "bucket1", index2Parse["bucket"], "should be equal")
	assert.EqualValues(t, "100", index2Parse["epoch"], "should be equal")
	assert.EqualValues(t, "mykey", index2Parse["key"], "should be equal")
}
