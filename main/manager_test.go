package main

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestManagerDepsHolder(t *testing.T) {
	logrus.Debug("hi")
	assert.Equal(t, 1, 1, "always valid")
}
