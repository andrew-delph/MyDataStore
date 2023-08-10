package main

import (
	"context"
	"fmt"
	"net"
	"time"

	pb "github.com/andrew-delph/my-key-store/proto"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var port = 7070

type internalServer struct {
	pb.InternalNodeServiceServer
}

type internalClient struct {
	pb.InternalNodeServiceClient
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

func Client() {
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", port), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		logrus.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	internalClient := pb.NewInternalNodeServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := internalClient.TestRequest(ctx, &pb.StandardResponse{Message: "This is the client."})
	if err != nil {
		logrus.Fatalf("could not greet: %v", err)
	}
	logrus.Warnf("Greeting: %s", r.Message)
}
