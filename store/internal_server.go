package main

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/cbergoon/merkletree"

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
	} else if err == nil {
		return &pb.Value{}, nil
	} else {
		return nil, err
	}
}

func (s *internalServer) SyncPartition(req *pb.SyncPartitionRequest, stream pb.InternalNodeService_SyncPartitionServer) error {
	// TODO stream all contents of partition.
	logrus.Warnf("Server SyncPartition")

	partition, err := store.getPartition(int(req.Partition))
	if err != nil {
		logrus.Error(err)
		return err
	}

	for i := 1; i <= partitionBuckets; i++ {
		items := partition.Items(i, 0, int(req.Epoch))
		for _, value := range items {
			if err := stream.Send(value); err != nil {
				logrus.Errorf("Sever send value. %v", err)
				return err
			}
		}
	}

	return nil
}

func (*internalServer) VerifyMerkleTree(stream pb.InternalNodeService_VerifyMerkleTreeServer) error {
	// defer logrus.Warn("server done.")
	// logrus.Warn("server start.")
	rootNode, err := stream.Recv()
	if err != nil {
		logrus.Error("SERVER ", err)
		return err
	}
	epoch := rootNode.Epoch
	partitionId := rootNode.Partition
	partitionTree, err := PartitionMerkleTree(uint64(epoch), int(partitionId))
	if err != nil {
		logrus.Error("SERVER ", err)
		return err
	}

	isEqual := bytes.Equal(partitionTree.Root.Hash, rootNode.Hash)
	nodeResponse := &pb.VerifyMerkleTreeNodeResponse{IsEqual: isEqual}

	err = stream.Send(nodeResponse)
	if err != nil {
		logrus.Error("SERVER ", err)
		return err
	}
	if isEqual {
		return nil
	}
	defer logrus.Debugf("SERVER COMPLETED SYNC")
	logrus.Debugf("server not equal.")

	nodesQueue := list.New()
	nodesQueue.PushFront(partitionTree.Root.Left)
	nodesQueue.PushFront(partitionTree.Root.Right)

	for nodesQueue.Len() > 0 {
		element := nodesQueue.Front()
		node, ok := element.Value.(*merkletree.Node)
		if !ok {
			logrus.Error("could not decode server.")
			return fmt.Errorf("could not decode node server.")
		}
		nodesQueue.Remove(element)
		logrus.Debugf("server waiting.")
		nodeRequest, err := stream.Recv()
		if err == io.EOF {
			logrus.Warn("Server VerifyMerkleTree Done.")
			return nil
		}
		if err != nil {
			logrus.Error("SERVER ", err)
			return err
		}

		isEqual := bytes.Equal(node.Hash, nodeRequest.Hash)
		logrus.Debugf("server isEqual %t", isEqual)

		nodeResponse := &pb.VerifyMerkleTreeNodeResponse{IsEqual: isEqual}

		err = stream.Send(nodeResponse)
		if err != nil {
			logrus.Error("SERVER ", err)
			return err
		}

		if !isEqual {
			if node.Left == nil && node.Right == nil {
				logrus.Debugf("SERVER the node is a leaf!")
				bucket, ok := node.C.(MerkleBucket)
				if !ok {
					logrus.Error("SERVER could not decode bucket")
					return errors.New("SERVER value is not of type MerkleContent")
				}
				logrus.Debugf("SERVER bucket.contentList length: %d", len(bucket.contentList))
				for _, content := range bucket.contentList {
					logrus.Debugf("SERVER bucket content: %s", content.key)
				}
			} else {
				if node.Left != nil {
					nodesQueue.PushFront(node.Left)
				}
				if node.Right != nil {
					nodesQueue.PushFront(node.Right)
				}
			}
		}
	}

	return io.EOF
}

func (s *internalServer) StreamBuckets(req *pb.StreamBucketsRequest, stream pb.InternalNodeService_StreamBucketsServer) error {
	logrus.Debugf("SERVER StreamBuckets Buckets %v Epoch %v Partition %v", req.Buckets, req.Epoch, req.Partition)
	streamPartions := []int{int(req.Partition)}
	for _, bucket := range req.Buckets {
		items := store.Items(streamPartions, int(bucket), 0, int(req.Epoch))

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
