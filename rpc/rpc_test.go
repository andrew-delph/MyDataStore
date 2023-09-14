package rpc

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gogo/status"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
)

func TestRpc(t *testing.T) {
	err := status.Errorf(codes.NotFound, "Resource not found")

	logrus.Info(err)

	st, ok := status.FromError(err)

	assert.Equal(t, true, ok, "ok valid")
	logrus.Warn(st.Code(), ok)

	assert.Equal(t, codes.NotFound, st.Code(), "code is NotFound")
}
