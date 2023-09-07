package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"

	"github.com/andrew-delph/my-key-store/config"
	datap "github.com/andrew-delph/my-key-store/datap"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type RpcWrapper struct {
	rpcConfig config.RpcConfig
	reqCh     chan interface{}
	grpc      *grpc.Server
	// datap.InternalNodeServiceServer
}

func CreateRpcWrapper(rpcConfig config.RpcConfig, reqCh chan interface{}) RpcWrapper {
	grpc := grpc.NewServer()
	rpcWrapper := RpcWrapper{rpcConfig: rpcConfig, grpc: grpc, reqCh: reqCh}
	datap.RegisterInternalNodeServiceServer(grpc, &rpcWrapper)
	return rpcWrapper
}

type SetValueTask struct {
	Key   string
	Value string
	Epoch int64
	ResCh chan interface{}
}

type GetValueTask struct {
	Key   string
	ResCh chan interface{}
}

type GetPartitionEpochObjectTask struct {
	Partition int32
	Epoch     int64
	ResCh     chan interface{}
}

func (rpcWrapper *RpcWrapper) StartRpcServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", rpcWrapper.rpcConfig.Port))
	if err != nil {
		logrus.Fatalf("failed to listen: %v", err)
	}
	logrus.Infof("server listening at %v", lis.Addr())
	if err := rpcWrapper.grpc.Serve(lis); err != nil {
		logrus.Fatalf("failed to serve: %v", err)
	}
}

func (rpcWrapper *RpcWrapper) SetRequest(ctx context.Context, value *datap.Value) (*datap.StandardResponse, error) {
	logrus.Debugf("SERVER Handling SetRequest: key=%s value=%s epoch=%d", value.Key, value.Value, value.Epoch)
	resCh := make(chan interface{})
	rpcWrapper.reqCh <- SetValueTask{Key: value.Key, Value: value.Value, Epoch: value.Epoch, ResCh: resCh}
	res := <-resCh
	logrus.Debug("SetRequest res ", res)
	return &datap.StandardResponse{Message: "Value set"}, nil
}

func (rpcWrapper *RpcWrapper) GetRequest(ctx context.Context, req *datap.GetRequestMessage) (*datap.Value, error) {
	logrus.Debugf("Handling GetRequest: key=%s ", req.Key)
	resCh := make(chan interface{})
	rpcWrapper.reqCh <- GetValueTask{Key: req.Key, ResCh: resCh}
	rawRes := <-resCh
	switch res := rawRes.(type) {
	case []byte:
		return &datap.Value{Value: fmt.Sprintf("%s", res)}, nil
	case error:
		return nil, res
	default:
		logrus.Panicf("http unkown res type: %v", reflect.TypeOf(res))
	}
	return nil, errors.New("?????")
}

func (rpcWrapper *RpcWrapper) StreamBuckets(req *datap.StreamBucketsRequest, stream datap.InternalNodeService_StreamBucketsServer) error {
	logrus.Debugf("SERVER StreamBuckets Buckets %v LowerEpoch %v UpperEpoch %v Partition %v", req.Buckets, req.LowerEpoch, req.UpperEpoch, req.Partition)
	panic("unimplemented")
	return nil
}

func (rpcWrapper *RpcWrapper) GetPartitionEpochObject(ctx context.Context, req *datap.PartitionEpochObject) (*datap.PartitionEpochObject, error) {
	logrus.Debugf("Handling GetPartitionEpochObject: Partition=%d Epoch=%d", req.Partition, req.Epoch)
	resCh := make(chan interface{})
	rpcWrapper.reqCh <- GetPartitionEpochObjectTask{Partition: req.Partition, Epoch: req.Epoch, ResCh: resCh}
	res := <-resCh
	logrus.Warn("res ", res)
	return nil, nil
}
