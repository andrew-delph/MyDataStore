package main

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestConsistencyHeap(t *testing.T) {
	h := NewConsistencyHeap()

	h.PushSyncTask(1, int64(2))
	h.ManualPush(1, 2, false, 3)
	h.PushSyncTask(1, int64(3))
	h.ManualPush(1, 2, false, 2)
	h.PushSyncTask(1, int64(1))
	h.ManualPush(1, 2, false, 4)
	h.PushVerifyTask(1, int64(1))
	h.PushVerifyTask(1, int64(5))

	assert.Equal(t, true, true, "true")

	for h.Len() > 0 {
		popped := h.PopItem()
		logrus.Debugf("popped= %+v", popped)
	}

	go func() {
		h.PushSyncTask(1, int64(1))
	}()
	waiting := h.PopItem()
	assert.Equal(t, int64(1), waiting.Epoch, "true")
}
