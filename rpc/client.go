package rpc

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	datap "github.com/andrew-delph/my-key-store/datap"
)

type RpcClient = datap.InternalNodeServiceClient

type (
	RpcValue                = datap.Value
	RpcGetRequestMessage    = datap.GetRequestMessage
	RpcStandardObject       = datap.StandardObject
	RpcEpochTreeObject      = datap.EpochTreeObject
	RpcStreamBucketsRequest = datap.StreamBucketsRequest
)

func (rpcWrapper *RpcWrapper) CreateRpcClient(ip string) (*grpc.ClientConn, RpcClient, error) {
	return CreateRawRpcClient(ip, rpcWrapper.rpcConfig.Port)
}

func CreateRawRpcClient(ip string, port int) (*grpc.ClientConn, RpcClient, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", ip, port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	internalClient := datap.NewInternalNodeServiceClient(conn)

	return conn, internalClient, nil
}
