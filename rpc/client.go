package rpc

import (
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/gogo/status"

	datap "github.com/andrew-delph/my-key-store/datap"
)

type RpcClient = datap.InternalNodeServiceClient

type (
	RpcValue                = datap.Value
	RpcGetRequestMessage    = datap.GetRequestMessage
	RpcStandardObject       = datap.StandardObject
	RpcEpochTreeObject      = datap.EpochTreeObject
	RpcStreamBucketsRequest = datap.StreamBucketsRequest
	RpcMembers              = datap.Members
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

func ExtractError(err error) error {
	st, ok := status.FromError(err)
	if ok {
		return errors.New(st.Message())
	}
	return err
}
