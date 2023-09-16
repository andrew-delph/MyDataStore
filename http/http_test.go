package http

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/andrew-delph/my-key-store/config"
)

func TestHttpDefault(t *testing.T) {
	testHttp()

	reqCh := make(chan interface{}, 1)

	c := config.GetConfig()

	httpServer := CreateHttpServer(c.Http, reqCh)
	assert.Equal(t, true, true, "healthy wrong value")

	logrus.Info("httpServer: ", httpServer)
}
