package rpc

import (
	"fmt"

	"google.golang.org/grpc"

	datap "github.com/andrew-delph/my-key-store/datap"
)

type RpcClient = datap.InternalNodeServiceClient

type (
	RpcValue             = datap.Value
	RpcGetRequestMessage = datap.GetRequestMessage
	RpcStandardResponse  = datap.StandardResponse
	RpcEpochTreeObject   = datap.EpochTreeObject
)

func (rpcWrapper *RpcWrapper) CreateRpcClient(ip string) (*grpc.ClientConn, RpcClient, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", ip, rpcWrapper.rpcConfig.Port), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	internalClient := datap.NewInternalNodeServiceClient(conn)

	return conn, internalClient, nil
}
