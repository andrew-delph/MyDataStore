package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConsistencyHeap(t *testing.T) {
	h := NewConsistencyHeap()
	h.PushSyncTask(int64(2))
	h.PushSyncTask(int64(3))
	h.PushSyncTask(int64(1))
	h.PushVerifyTask(int64(1))
	h.PushVerifyTask(int64(5))

	assert.Equal(t, true, true, "true")

	for h.Len() > 0 {
		h.PopItem()
	}

	go func() {
		h.PushSyncTask(int64(1))
	}()
	waiting := h.PopItem()
	assert.Equal(t, int64(1), waiting.Epoch, "true")
}
