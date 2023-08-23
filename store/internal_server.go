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

	datap "github.com/andrew-delph/my-key-store/datap"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var port = 7070

type internalServer struct {
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

	logrus.Warnf("server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		logrus.Fatalf("failed to serve: %v", err)
	}
}

func (s *internalServer) TestRequest(ctx context.Context, in *datap.StandardResponse) (*datap.StandardResponse, error) {
	logrus.Warnf("Received: %v", in.Message)
	return &datap.StandardResponse{Message: "This is the server."}, nil
}

func (s *internalServer) SetRequest(ctx context.Context, m *datap.Value) (*datap.StandardResponse, error) {
	logrus.Debugf("Handling SetRequest: key=%s value=%s epoch=%d", m.Key, m.Value, m.Epoch)

	err := store.SetValue(m)
	if err != nil {
		logrus.Errorf("failed to set %s : %s error= %v", m.Key, m.Value, err)
		return nil, err
	} else {
		return &datap.StandardResponse{Message: "Value set."}, nil
	}
}

func (s *internalServer) GetRequest(ctx context.Context, m *datap.GetRequestMessage) (*datap.Value, error) {
	logrus.Debugf("Handling GetRequest: key=%s ", m.Key)
	value, exists, err := store.GetValue(m.Key)

	if exists {
		return value, nil
	} else if err == nil {
		return &datap.Value{}, nil
	} else {
		return nil, err
	}
}

func (*internalServer) VerifyMerkleTree(stream datap.InternalNodeService_VerifyMerkleTreeServer) error {
	// defer logrus.Warn("server done.")
	// logrus.Warn("server start.")
	rootNode, err := stream.Recv()
	if err != nil {
		logrus.Error("SERVER ", err)
		return err
	}
	epoch := rootNode.Epoch
	partitionId := int(rootNode.Partition)

	var partitionTree *merkletree.MerkleTree

	partitionTree, _, err = RawPartitionMerkleTree(epoch, true, partitionId)
	if err != nil {
		err = fmt.Errorf("SERVER VerifyMerkleTree err = %v", err)
		logrus.Error(err)
		return err
	}

	isEqual := bytes.Equal(partitionTree.Root.Hash, rootNode.Hash)
	nodeResponse := &datap.VerifyMerkleTreeNodeResponse{IsEqual: isEqual}

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

		nodeResponse := &datap.VerifyMerkleTreeNodeResponse{IsEqual: isEqual}

		err = stream.Send(nodeResponse)
		if err != nil {
			logrus.Error("SERVER ", err)
			return err
		}

		if !isEqual {
			if node.Left == nil && node.Right == nil {
				logrus.Debugf("SERVER the node is a leaf!")
				hash, _ := node.C.CalculateHash()
				bucket, ok := node.C.(*RealMerkleBucket)
				if !ok {
					logrus.Errorf("SERVER could not decode bucket = %v hash = %v", bucket, hash)
					return errors.New("SERVER value is not of type MerkleContent")
				}
				logrus.Debugf("SERVER unsynced bucketId = %v", bucket.bucketId)

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

func (s *internalServer) GetParitionEpochObject(ctx context.Context, req *datap.ParitionEpochObject) (*datap.ParitionEpochObject, error) {
	logrus.Debugf("Handling GetParitionEpochObject: Partition=%d Epoch=%d", req.Partition, req.Epoch)
	tree, buckets, err := RawPartitionMerkleTree(req.Epoch, false, int(req.Partition))
	if err != nil {
		return nil, err
	}
	paritionEpochObject, err := MerkleTreeToParitionEpochObject(tree, buckets, req.Epoch, int(req.Partition))
	if err != nil {
		return nil, err
	}
	return paritionEpochObject, nil
}
