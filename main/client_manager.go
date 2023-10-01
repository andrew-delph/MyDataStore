package main

import (
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
	logrus.Infof("AddClient %s", name)
	cm.clientMap[name] = rpcClient
}

func (cm *ClientManager) GetClient(name string) (rpc.RpcClient, error) {
	cm.rwLock.RLock()
	defer cm.rwLock.RUnlock()
	logrus.Infof("GetClient %s", name)
	client, ok := cm.clientMap[name]
	if !ok {
		return nil, errors.New("client not found")
	}
	return client, nil
}
