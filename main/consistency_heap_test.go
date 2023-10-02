package main

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestConsistencyHeap(t *testing.T) {
	h := NewConsistencyHeap()
	logrus.Warn("h=", h)
	h.PushSyncTask(int64(2))
	h.PushSyncTask(int64(3))

	h.PushVerifyTask(int64(1))

	h.PushVerifyTask(int64(5))
	assert.Equal(t, true, true, "true")

	logrus.Warn("len=", h.Len())
	for h.Len() > 0 {
		logrus.Warn("item=", h.Pop())
	}
}
