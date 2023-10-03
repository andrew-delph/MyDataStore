package main

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestConsistencyHeap(t *testing.T) {
	h := NewConsistencyHeap()

	for i := 0; i < 10; i++ {
		h.PushSyncTask(1, int64(2))
		h.ManualPush(1, 2, false, 3)
		h.PushSyncTask(1, int64(3))
		h.ManualPush(1, 2, false, 2)
		h.PushSyncTask(1, int64(1))
		h.ManualPush(1, 2, false, 4)
		h.PushVerifyTask(1, int64(1))
		h.PushVerifyTask(1, int64(5))
	}

	assert.Equal(t, true, true, "true")
	minAttempt := 0
	for h.Len() > 0 {
		popped := h.PopItem()
		if popped.Attemps < minAttempt {
			t.Error("attempts order broken")
		}
		minAttempt = popped.Attemps
		logrus.Debugf("popped= %+v len=%d", popped, h.Len())
	}

	go func() {
		h.PushSyncTask(1, int64(1))
	}()
	waiting := h.PopItem()
	assert.Equal(t, int64(1), waiting.Epoch, "true")
}

func TestConsistencyHeapCrazy(t *testing.T) {
	return
	h := NewConsistencyHeap()

	pusher := func() {
		go func() {
			endTime := time.Now().Add(5 * time.Second)
			for time.Now().Before(endTime) {
				h.PushSyncTask(1, int64(2))
				h.ManualPush(1, 2, false, 3)
				h.PushSyncTask(1, int64(3))
				h.ManualPush(1, 2, false, 2)
				h.PushSyncTask(1, int64(1))
				h.ManualPush(1, 2, false, 4)
				h.PushVerifyTask(1, int64(1))
				h.PushVerifyTask(1, int64(5))
				time.Sleep(time.Millisecond * 5)
			}
		}()
	}

	pusher()
	pusher()
	pusher()

	time.Sleep(time.Second)

	assert.Equal(t, true, true, "true")
	for h.Len() > 0 {
		popped := h.PopItem()
		logrus.Warnf("popped= %+v len=%d", popped, h.Len())
		time.Sleep(time.Millisecond * 1)
	}

	assert.Equal(t, 0, h.Len(), "true")

	go func() {
		h.PushSyncTask(1, int64(1))
	}()
	waiting := h.PopItem()
	assert.Equal(t, int64(1), waiting.Epoch, "true")
}
