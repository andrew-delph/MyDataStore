package main

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/rpc"
)

type ClientManager struct {
	clientMap map[string]rpc.RpcClient
	rwLock    sync.RWMutex
}

func NewClientManager() *ClientManager {
	clientMap := make(map[string]rpc.RpcClient)
	return &ClientManager{clientMap: clientMap}
}

func (cm *ClientManager) AddClient(name string, rpcClient rpc.RpcClient) {
	cm.rwLock.Lock()
	defer cm.rwLock.Unlock()
	logrus.Debugf("AddClient %s", name)
	cm.clientMap[name] = rpcClient
}

func (cm *ClientManager) AddTempClient(name string) {
	cm.rwLock.Lock()
	defer cm.rwLock.Unlock()
	logrus.Debugf("AddTempClient %s", name)
	cm.clientMap[name] = nil
}

var TEMP_CLIENT_ERROR = errors.New("found temp client")

func (cm *ClientManager) GetClient(name string) (rpc.RpcClient, error) {
	cm.rwLock.RLock()
	defer cm.rwLock.RUnlock()
	logrus.Debugf("GetClient %s", name)
	client, ok := cm.clientMap[name]
	if !ok {
		return nil, fmt.Errorf("client not found: %s", name)
	}
	if client == nil {
		return nil, TEMP_CLIENT_ERROR
	}
	return client, nil
}
