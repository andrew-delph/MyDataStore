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

func CreateRpcWrapper(rpcConfig config.RpcConfig, reqCh chan interface{}) *RpcWrapper {
	grpc := grpc.NewServer()
	rpcWrapper := &RpcWrapper{rpcConfig: rpcConfig, grpc: grpc, reqCh: reqCh}
	datap.RegisterInternalNodeServiceServer(grpc, rpcWrapper)
	return rpcWrapper
}

type SetValueTask struct {
	Value *RpcValue
	ResCh chan interface{}
}

type GetValueTask struct {
	Key   string
	ResCh chan interface{}
}

type GetEpochTreeObjectTask struct {
	PartitionId int32
	LowerEpoch  int64
	UpperEpoch  int64
	ResCh       chan interface{}
}

type GetEpochTreeLastValidObjectTask struct {
	PartitionId int32
	ResCh       chan interface{}
}

type StreamBucketsTask struct {
	PartitionId int32
	Buckets     []int32
	LowerEpoch  int64
	UpperEpoch  int64
	ResCh       chan interface{}
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
	rpcWrapper.reqCh <- SetValueTask{Value: value, ResCh: resCh}
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
	case *datap.Value:
		return res, nil
	case error:
		return nil, res
	default:
		logrus.Panicf("http unkown res type: %v", reflect.TypeOf(res))
	}
	return nil, errors.New("?????")
}

func (rpcWrapper *RpcWrapper) StreamBuckets(req *datap.StreamBucketsRequest, stream datap.InternalNodeService_StreamBucketsServer) error {
	logrus.Debugf("SERVER StreamBuckets Buckets %v LowerEpoch %v UpperEpoch %v Partition %v", req.Buckets, req.LowerEpoch, req.UpperEpoch, req.Partition)
	resCh := make(chan interface{})
	rpcWrapper.reqCh <- StreamBucketsTask{PartitionId: req.Partition, Buckets: req.Buckets, LowerEpoch: req.LowerEpoch, UpperEpoch: req.UpperEpoch, ResCh: resCh}
	for {
		select {
		case itemObj, ok := <-resCh:
			if !ok {
				fmt.Println("Channel is closed")
				return nil
			}
			switch item := itemObj.(type) {
			case *datap.Value:
				err := stream.Send(item)
				if err != nil {
					logrus.Errorf("SERVER StreamBuckets err = %v", err)
					return err
				}
			case error:
				return item
			default:
				logrus.Panicf("http unkown res type: %v", reflect.TypeOf(item))
			}
		}
	}
}

func (rpcWrapper *RpcWrapper) GetEpochTree(ctx context.Context, req *datap.EpochTreeObject) (*datap.EpochTreeObject, error) {
	logrus.Debugf("Handling GetEpochTree: Partition=%d LowerEpoch=%d UpperEpoch=%d", req.Partition, req.LowerEpoch, req.UpperEpoch)
	resCh := make(chan interface{})
	rpcWrapper.reqCh <- GetEpochTreeObjectTask{PartitionId: req.Partition, LowerEpoch: req.LowerEpoch, UpperEpoch: req.UpperEpoch, ResCh: resCh}
	rawRes := <-resCh
	switch res := rawRes.(type) {
	case *datap.EpochTreeObject:
		return res, nil
	case error:
		return nil, res
	default:
		logrus.Panicf("http unkown res type: %v", reflect.TypeOf(res))
	}
	return nil, nil
}

func (rpcWrapper *RpcWrapper) GetEpochTreeLastValid(ctx context.Context, req *datap.EpochTreeObject) (*datap.EpochTreeObject, error) {
	logrus.Debugf("Handling GetEpochTree: Partition=%d LowerEpoch=%d UpperEpoch=%d", req.Partition, req.LowerEpoch, req.UpperEpoch)
	resCh := make(chan interface{})
	rpcWrapper.reqCh <- GetEpochTreeLastValidObjectTask{PartitionId: req.Partition, ResCh: resCh}
	rawRes := <-resCh
	switch res := rawRes.(type) {
	case *datap.EpochTreeObject:
		return res, nil
	case error:
		return nil, res
	default:
		logrus.Panicf("http unkown res type: %v", reflect.TypeOf(res))
	}
	return nil, nil
}
