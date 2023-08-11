package main

import (
	"context"
	"fmt"
	"net"

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

func (s *internalServer) SetRequest(ctx context.Context, m *pb.SetRequestMessage) (*pb.StandardResponse, error) {
	logrus.Debugf("Handling SetRequest: key=%s value=%s ", m.Key, m.Value)
	partitionId := FindPartitionID(events.consistent, m.Key)
	err := setValue(partitionId, m.Key, m.Value)
	if err != nil {
		logrus.Errorf("failed to set %s : %s error= %v", m.Key, m.Value, err)
		return nil, err
	} else {
		return &pb.StandardResponse{Message: "Value set."}, nil
	}
}

func (s *internalServer) GetRequest(ctx context.Context, m *pb.GetRequestMessage) (*pb.GetResponseMessage, error) {
	logrus.Debugf("Handling GetRequest: key=%s ", m.Key)
	partitionId := FindPartitionID(events.consistent, m.Key)
	value, exists, err := getValue(partitionId, m.Key)

	if exists {
		return &pb.GetResponseMessage{Value: value}, nil
	} else if err != nil {
		return nil, err
	} else {
		return nil, fmt.Errorf("Value not found for %s", m.Key)
	}
}
