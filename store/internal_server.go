package main

import (
	"context"
	"fmt"
	"net"

	datap "github.com/andrew-delph/my-key-store/datap"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var port = 7070

type internalServer struct {
	manager Manager
	datap.InternalNodeServiceServer
}

func StartInterGrpcServer() {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("1Uncaught panic: %v", r)
			// Perform any necessary cleanup or error handling here
		}
	}()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logrus.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	datap.RegisterInternalNodeServiceServer(s, &internalServer{})

	logrus.Infof("server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		logrus.Fatalf("failed to serve: %v", err)
	}
}

func (s *internalServer) TestRequest(ctx context.Context, in *datap.StandardResponse) (*datap.StandardResponse, error) {
	logrus.Warnf("Received: %v", in.Message)
	return &datap.StandardResponse{Message: "This is the server."}, nil
}

func (s *internalServer) SetRequest(ctx context.Context, value *datap.Value) (*datap.StandardResponse, error) {
	logrus.Debugf("Handling SetRequest: key=%s value=%s epoch=%d", value.Key, value.Value, value.Epoch)

	if value.Epoch > globalEpoch+1 || value.Epoch < globalEpoch-1 {
		err := fmt.Errorf("Epoch out of sync. currEpoch = %d requested = %d", globalEpoch, value.Epoch)
		logrus.Error(err)
		return nil, err
	}

	err := store.SetValue(value)
	if err != nil {
		logrus.Errorf("failed to set %s : %s error= %v", value.Key, value.Value, err)
		return nil, err
	} else {
		return &datap.StandardResponse{Message: "Value set."}, nil
	}
}

func (s *internalServer) GetRequest(ctx context.Context, req *datap.GetRequestMessage) (*datap.Value, error) {
	logrus.Debugf("Handling GetRequest: key=%s ", req.Key)
	value, exists, err := store.GetValue(req.Key)

	if exists {
		return value, nil
	} else if err == nil {
		return &datap.Value{}, nil
	} else {
		return nil, err
	}
}

func (s *internalServer) StreamBuckets(req *datap.StreamBucketsRequest, stream datap.InternalNodeService_StreamBucketsServer) error {
	logrus.Debugf("SERVER StreamBuckets Buckets %v LowerEpoch %v UpperEpoch %v Partition %v", req.Buckets, req.LowerEpoch, req.UpperEpoch, req.Partition)
	lowerEpoch := int(req.LowerEpoch)
	upperEpoch := int(req.UpperEpoch)

	streamPartions := []int{int(req.Partition)}
	for _, bucket := range req.Buckets {
		items := store.Items(streamPartions, int(bucket), lowerEpoch, upperEpoch)

		for _, value := range items {
			err := stream.Send(value)
			if err != nil {
				logrus.Errorf("SERVER StreamBuckets err = %v", err)
				return err
			}
		}
	}

	return nil
}

func (s *internalServer) GetPartitionEpochObject(ctx context.Context, req *datap.EpochTreeObject) (*datap.EpochTreeObject, error) {
	logrus.Debugf("Handling GetPartitionEpochObject: Partition=%d Epoch=%d", req.Partition, req.Epoch)

	partition, err := store.getPartition(int(req.Partition))
	if err != nil {
		logrus.Debugf("SERVER GetPartitionEpochObject.getPartition err = %v", err)
		return nil, err
	}

	partitionEpochObject, err := partition.GetPartitionEpochObject(int(req.Epoch))
	if err != nil {
		logrus.Debugf("SERVER GetPartitionEpochObject.GetPartitionEpochObject Partition = %d Epoch = %d err = %v", req.Partition, req.Epoch, err)
		return nil, err
	}
	if partitionEpochObject == nil {
		logrus.Debugf("SERVER GetPartitionEpochObject partitionEpochObject is nil")
		return nil, fmt.Errorf("COULD NOT FIND!")
	}

	return partitionEpochObject, nil
}
