package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sort"

	pb "github.com/andrew-delph/my-key-store/proto"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var port = 7070

type internalServer struct {
	pb.InternalNodeServiceServer
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
	pb.RegisterInternalNodeServiceServer(s, &internalServer{})

	logrus.Warnf("server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		logrus.Fatalf("failed to serve: %v", err)
	}
}

func (s *internalServer) TestRequest(ctx context.Context, in *pb.StandardResponse) (*pb.StandardResponse, error) {
	logrus.Warnf("Received: %v", in.Message)
	return &pb.StandardResponse{Message: "This is the server."}, nil
}

func (s *internalServer) SetRequest(ctx context.Context, m *pb.Value) (*pb.StandardResponse, error) {
	logrus.Debugf("Handling SetRequest: key=%s value=%s ", m.Key, m.Value)

	err := store.setValue(m)
	if err != nil {
		logrus.Errorf("failed to set %s : %s error= %v", m.Key, m.Value, err)
		return nil, err
	} else {
		return &pb.StandardResponse{Message: "Value set."}, nil
	}
}

func (s *internalServer) GetRequest(ctx context.Context, m *pb.GetRequestMessage) (*pb.Value, error) {
	logrus.Debugf("Handling GetRequest: key=%s ", m.Key)
	value, exists, err := store.getValue(m.Key)

	if exists {
		return value, nil
	} else if err != nil {
		return nil, err
	} else {
		return nil, fmt.Errorf("Value not found for %s", m.Key)
	}
}

func (s *internalServer) SyncPartition(req *pb.SyncPartitionRequest, stream pb.InternalNodeService_SyncPartitionServer) error {
	partitionTree, err := PartitionMerkleTree(uint64(req.Epoch), int(req.Partition))
	if err != nil {
		logrus.Error(err)
		return err
	}
	areEqual := bytes.Equal(partitionTree.Root.Hash, req.Hash)

	if areEqual {
		logrus.Warn("SERVER HASHES ARE EQUAL.")
		return nil
	}

	partition, err := store.getPartition(int(req.Partition))
	if err != nil {
		logrus.Error(err)
		return err
	}

	items := partition.Items(int(req.Partition), 0, int(req.Epoch))
	// if len(items) == 0 {
	// 	return nil, fmt.Errorf("partition.Items() is %d", 0)
	// }

	// Extract keys and sort them
	var keys []string
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		value := items[key]
		if value.Epoch > req.Epoch {
			continue
		}
		if err := stream.Send(value); err != nil {
			logrus.Errorf("Sever send value. %v", err)
			return err
		}
	}

	return nil
}
