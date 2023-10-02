package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/andrew-delph/my-key-store/rpc"

	"github.com/sirupsen/logrus"
)

func TestClientManager(t *testing.T) {
	// TODO show this
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	logrus.Warn("Tests!!!")
	clientManager := NewClientManager()
	_, err := clientManager.GetClient("test")
	if err == nil {
		t.Error("Should have returned error")
	}
	_, client, err := rpc.CreateRawRpcClient("1", 2)
	if err != nil {
		t.Error(err)
	}
	clientManager.AddClient("test", client)

	getClient, err := clientManager.GetClient("test")
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, client, getClient, "clients equal")

	_, client2, err := rpc.CreateRawRpcClient("1", 2)
	if err != nil {
		t.Error(err)
	}
	clientManager.AddClient("test", client2)

	getClient2, err := clientManager.GetClient("test")
	if err != nil {
		t.Error(err)
	}

	assert.NotEqual(t, client, getClient2, "clients equal")

	clientManager.AddTempClient("temp1")

	_, err = clientManager.GetClient("temp1")

	assert.Equal(t, TEMP_CLIENT_ERROR, err, "clients equal")
}
