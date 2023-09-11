package main

import (
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestManagerDepsHolder(t *testing.T) {
	x := atomic.Bool{}
	logrus.Info("hi", x)
	logrus.Info(">", x.CompareAndSwap(false, false))
	logrus.Info(">", x.CompareAndSwap(false, false))
	// logrus.Info(">", x.CompareAndSwap(false, true))
	// logrus.Info(">", x.CompareAndSwap(true, false))
	// logrus.Info(">", x.CompareAndSwap(false, true))
	// logrus.Info(">", x.CompareAndSwap(false, false))
	assert.Equal(t, 1, 1, "always valid")
	t.Error("")
}
