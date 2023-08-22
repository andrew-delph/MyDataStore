package main

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cbergoon/merkletree"

	pb "github.com/andrew-delph/my-key-store/proto"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func SendSetMessageNode(addr string, setReqMsg *pb.Value, responseCh chan *pb.StandardResponse, errorCh chan error) {
	conn, client, err := GetClient(addr)
	if err != nil {
		logrus.Errorf("GetClient for node %s", addr)
		errorCh <- err
	}
	defer conn.Close()

	// Create a new context for the goroutine
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	r, err := client.SetRequest(ctx, setReqMsg)
	if err != nil {
		logrus.Errorf("Failed SetRequest for node %s", addr)
		errorCh <- err
	} else {
		responseCh <- r
		logrus.Debugf("SetRequest %s worked. msg ='%s'", addr, r.Message)
	}
}

func SendSetMessage(key, value string) error {
	unixTimestamp := time.Now().Unix()
	setReqMsg := &pb.Value{Key: key, Value: value, Epoch: int64(currEpoch), UnixTimestamp: unixTimestamp}

	nodes, err := GetClosestN(events.consistent, key, N)
	if err != nil {
		return err
	}

	responseCh := make(chan *pb.StandardResponse, N)
	errorCh := make(chan error, N)

	for _, node := range nodes {
		go SendSetMessageNode(node.String(), setReqMsg, responseCh, errorCh)
	}

	timeout := time.After(defaultTimeout)
	responseCount := 0

	for responseCount < W {
		select {
		case <-responseCh:
			responseCount++
		case err := <-errorCh:
			logrus.Errorf("errorCh: %v", err)
			_ = err // Handle error if necessary
		case <-timeout:
			return fmt.Errorf("timed out waiting for responses")
		}
	}

	return nil
}

func SendGetMessage(key string) (string, error) {
	getReqMsg := &pb.GetRequestMessage{Key: key}

	nodes, err := GetClosestN(events.consistent, key, N)
	if err != nil {
		return "", err
	}

	type Res struct {
		Value *pb.Value
		Addr  string
	}
	test := Res{}
	logrus.Info(test)

	resCh := make(chan *Res, len(nodes))
	errorCh := make(chan error, len(nodes))

	for i, node := range nodes {
		go func(i int, addr string) {
			conn, client, err := GetClient(addr)
			if err != nil {
				errorCh <- err
				return
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			res, err := client.GetRequest(ctx, getReqMsg)

			if err != nil {
				errorCh <- err
			} else {
				resCh <- &Res{Value: res, Addr: addr}
			}
		}(i, node.String())
	}

	timeout := time.After(defaultTimeout)

	var recentValue *pb.Value

	var resList []*Res

	// defer Read Repair
	defer func() {
		logrus.Debugf("Read Repair. recentValue = %v len(resList) = %d len(nodes) = %d", recentValue, len(resList), len(nodes))
		for len(resList) < len(nodes) {
			select {
			case res := <-resCh:
				if recentValue == nil {
					logrus.Debugf("setting recentValue nil.")
					recentValue = res.Value
				} else if recentValue.Epoch <= res.Value.Epoch && recentValue.UnixTimestamp < res.Value.UnixTimestamp {
					logrus.Debugf("update recentValue addr = %s value = %v", res.Addr, res.Value.Value)
					recentValue = res.Value
				} else {
					logrus.Debugf("compare %v %v %v %v %v  %v %v....", recentValue.Epoch, res.Value.Epoch, recentValue.Epoch < res.Value.Epoch, recentValue.UnixTimestamp, res.Value.UnixTimestamp, recentValue.UnixTimestamp < res.Value.UnixTimestamp, res.Value.Value)
				}
				resList = append(resList, res)
			case err := <-errorCh:
				logrus.Debugf("GET ERROR = %v", err)
				resList = append(resList, nil) // avoid waiting on errors.
			}
		}
		if recentValue == nil {
			logrus.Warn("Nil Read Repair.")
			return
		}
		// logrus.Warnf("Read Repair value = %v", recentValue)
		for _, res := range resList {
			// logrus.Warnf("Read Repair addr = %s recentValue = %v value = %v", res.Addr, recentValue.Value, res.Value.Value)
			if proto.Equal(res.Value, recentValue) == false {
				logrus.Warnf("Read Repair addr = %s recentValue = %v value = %v", res.Addr, recentValue.Value, res.Value.Value)
				go SendSetMessageNode(res.Addr, recentValue, make(chan *pb.StandardResponse), make(chan error))
			}
		}
	}()

	for len(resList) < R {
		select {
		case res := <-resCh:

			if recentValue == nil {
				logrus.Debugf("setting recentValue nil.")
				recentValue = res.Value
			} else if recentValue.Epoch <= res.Value.Epoch && recentValue.UnixTimestamp < res.Value.UnixTimestamp {
				logrus.Debugf("update recentValue addr = %s value = %v", res.Addr, res.Value.Value)
				recentValue = res.Value
			} else {
				logrus.Debugf("compare %v %v %v %v %v  %v %v....", recentValue.Epoch, res.Value.Epoch, recentValue.Epoch < res.Value.Epoch, recentValue.UnixTimestamp, res.Value.UnixTimestamp, recentValue.UnixTimestamp < res.Value.UnixTimestamp, res.Value.Value)
			}
			resList = append(resList, res)
		case err := <-errorCh:
			logrus.Warnf("GET ERROR = %v !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", err)
			resList = append(resList, nil) // avoid waiting on errors.

		case <-timeout:
			return "", fmt.Errorf("timed out waiting for responses")
		}
	}
	if len(resList) >= R {
		if recentValue == nil {
			return "", fmt.Errorf("Value doesnt exist.")
		} else {
			return recentValue.Value, nil
		}
	}
	return "", fmt.Errorf("value not found. expected = %d recievedCount= %d", R, len(resList))
}

func SyncPartition(addr string, hash []byte, epoch int64, partitionId int) {
	syncPartReqMsg := &pb.SyncPartitionRequest{Epoch: int64(epoch), Partition: int32(partitionId), Hash: hash}

	conn, client, err := GetClient(addr)
	if err != nil {
		return
	}
	defer conn.Close()

	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()
	stream, err := client.SyncPartition(context.Background(), syncPartReqMsg)
	if err != nil {
		logrus.Warnf("CLIENT SyncPartition Failed to open stream: %v", err)
		return
	}

	for {
		value, err := stream.Recv()

		if err == io.EOF {
			logrus.Debug("Stream completed.")
			break
		}
		if err != nil {
			logrus.Errorf("CLIENT SyncPartition stream receive error: %v", err)
			break
		}

		err = store.SetValue(value)
		if err != nil {
			logrus.Warnf("CLIENT SyncPartition stream error store.SetValue: %v", err)
			continue
		}
	}
	logrus.Debugf("CLIENT COMPLETED SYNC")
}

func VerifyMerkleTree(addr string, epoch int64, globalEpoch bool, partitionId int) (map[int32]struct{}, error) {
	unsyncedBuckets := make(map[int32]struct{})
	var partitionTree *merkletree.MerkleTree
	var err error

	if globalEpoch {
		partitionTree, err = GlobalPartitionMerkleTree(partitionId)
		if err != nil {
			err = fmt.Errorf("CLIENT VerifyMerkleTree err = %v", err)
			logrus.Error(err)
			return nil, err
		}
	} else {
		partitionTree, err = CachePartitionMerkleTree(epoch, partitionId)
		if err != nil {
			err = fmt.Errorf("CLIENT VerifyMerkleTree err = %v", err)
			logrus.Error(err)
			return nil, fmt.Errorf("CLIENT VerifyMerkleTree err = %v", err)
		}
	}

	if err != nil {
		err = fmt.Errorf("CLIENT VerifyMerkleTree err = %v", err)
		logrus.Error(err)
		return nil, err
	}

	conn, client, err := GetClient(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	stream, err := client.VerifyMerkleTree(ctx)
	if err != nil {
		logrus.Warnf("CLIENT SyncPartition Failed to open stream: %v", err)
		return nil, err
	}

	nodesQueue := list.New()
	nodesQueue.PushFront(partitionTree.Root)

	for nodesQueue.Len() > 0 {
		element := nodesQueue.Front()
		node, ok := element.Value.(*merkletree.Node)
		if !ok {
			logrus.Error("client no decode.")
			return nil, fmt.Errorf("could not decode node")
		}
		nodesQueue.Remove(element)
		nodeRequest := &pb.VerifyMerkleTreeNodeRequest{Epoch: int64(epoch), Global: globalEpoch, Partition: int32(partitionId), Hash: node.Hash}
		err = stream.Send(nodeRequest)
		if err != nil {
			logrus.Error("CLIENT ", err)
			return unsyncedBuckets, err
		}
		nodeResponse, err := stream.Recv()
		if err == io.EOF {
			logrus.Debug("Client VerifyMerkleTree Done.")
			return unsyncedBuckets, nil
		}
		if err != nil {
			logrus.Error("client err = ", err)
			return unsyncedBuckets, err
		}

		if !nodeResponse.IsEqual {
			if node.Left == nil && node.Right == nil {
				logrus.Debugf("CLIENT the node is a leaf!")
				bucket, ok := node.C.(*MerkleBucket)
				if !ok {
					logrus.Error("CLIENT could not decode bucket")
					return unsyncedBuckets, errors.New("CLIENT value is not of type MerkleContent")
				}
				unsyncedBuckets[bucket.bucketId] = struct{}{}
				logrus.Debugf("CLIENT bucket.bucketId %d", bucket.bucketId)

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

	logrus.Debugf("CLIENT COMPLETED SYNC")
	return unsyncedBuckets, nil
}

func StreamBuckets(addr string, buckets []int32, epoch int64, globalEpoch bool, partitionId int) error {
	conn, client, err := GetClient(addr)
	if err != nil {
		logrus.Errorf("CLIENT StreamBuckets err = %v", err)
		return err
	}
	defer conn.Close()
	bucketsReq := &pb.StreamBucketsRequest{Buckets: buckets, Epoch: int64(epoch), Global: globalEpoch, Partition: int32(partitionId)}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	stream, err := client.StreamBuckets(ctx, bucketsReq)
	if err != nil {
		logrus.Errorf("CLIENT StreamBuckets err = %v", err)
		return err
	}
	for {
		value, err := stream.Recv()

		if err == io.EOF {
			logrus.Debug("CLIENT StreamBuckets completed.")
			return nil
		} else if err != nil {
			logrus.Errorf("CLIENT SyncPartition stream receive error: %v", err)
			return err
		}

		err = store.SetValue(value)
		if err != nil {
			logrus.Errorf("CLIENT StreamBuckets store.SetValue error: %v", err)
			continue
		} else {
			logrus.Debugf("CLIENT StreamBuckets SetValue SUCCESS key = %v", value.Key)
		}
	}
}

func GetClient(addr string) (*grpc.ClientConn, pb.InternalNodeServiceClient, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", addr, port), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	internalClient := pb.NewInternalNodeServiceClient(conn)

	return conn, internalClient, nil
}

func GlobalSync() error {
	myPartions, err := GetMemberPartions(events.consistent, conf.Name)
	if err != nil {
		logrus.Warn(err)
		return err
	}
	for _, partitionId := range myPartions {

		nodes, err := GetClosestNForPartition(events.consistent, partitionId, N)
		if err != nil {
			logrus.Error(err)
			return err
		}

		for _, node := range nodes {
			upperEpochRequest := currEpoch - 1
			unsyncedBuckets, err := VerifyMerkleTree(node.String(), upperEpochRequest, true, partitionId)
			if err != nil && err == io.EOF {
				logrus.Debugf("GlobalSync VerifyMerkleTree unsyncedBuckets = %v partitionId = %v err = %v ", unsyncedBuckets, partitionId, err)
			} else if err != nil {
				logrus.Warnf("GlobalSync VerifyMerkleTree unsyncedBuckets = %v partitionId = %v err = %v ", unsyncedBuckets, partitionId, err)
				return err
			} else {
			}

			if len(unsyncedBuckets) > 0 {
				logrus.Warnf("GlobalSync VerifyMerkleTree unsyncedBuckets = %v partitionId = %v ", unsyncedBuckets, partitionId)

				var requestBuckets []int32
				for b := range unsyncedBuckets {
					requestBuckets = append(requestBuckets, b)
				}

				logrus.Warnf("GlobalSync CLIENT requstBuckets: %v", requestBuckets)

				err = StreamBuckets(node.String(), requestBuckets, upperEpochRequest, true, partitionId)
				if err != nil && err == io.EOF {
					logrus.Debugf("GlobalSync StreamBuckets unsyncedBuckets = %v partitionId = %v err = %v ", unsyncedBuckets, partitionId, err)
				} else if err != nil {
					logrus.Errorf("GlobalSync StreamBuckets unsyncedBuckets = %v partitionId = %v err = %v ", unsyncedBuckets, partitionId, err)
				}
			}
		}
	}
	return nil
}

func RecentEpochSync() error {
	myPartions, err := GetMemberPartions(events.consistent, conf.Name)
	if err != nil {
		logrus.Warn(err)
		return err
	}
	for _, partitionId := range myPartions {

		nodes, err := GetClosestNForPartition(events.consistent, partitionId, N)
		if err != nil {
			logrus.Error(err)
			continue
		}

		for _, node := range nodes {
			upperEpochRequest := currEpoch - 1
			unsyncedBuckets, err := VerifyMerkleTree(node.String(), upperEpochRequest, false, partitionId)
			if err != nil && err == io.EOF {
				logrus.Debugf("RecentEpochSync VerifyMerkleTree unsyncedBuckets = %v partitionId = %v err = %v ", unsyncedBuckets, partitionId, err)
			} else if err != nil {
				logrus.Warnf("RecentEpochSync VerifyMerkleTree unsyncedBuckets = %v partitionId = %v err = %v ", unsyncedBuckets, partitionId, err)
				continue
			} else {
			}

			if len(unsyncedBuckets) > 0 {
				logrus.Warnf("RecentEpochSync VerifyMerkleTree unsyncedBuckets = %v partitionId = %v ", unsyncedBuckets, partitionId)

				var requestBuckets []int32
				for b := range unsyncedBuckets {
					requestBuckets = append(requestBuckets, b)
				}

				logrus.Warnf("RecentEpochSync CLIENT requstBuckets: %v", requestBuckets)

				err = StreamBuckets(node.String(), requestBuckets, upperEpochRequest, false, partitionId)
				if err != nil && err == io.EOF {
					logrus.Debugf("RecentEpochSync StreamBuckets unsyncedBuckets = %v partitionId = %v err = %v ", unsyncedBuckets, partitionId, err)
				} else if err != nil {
					logrus.Errorf("RecentEpochSync StreamBuckets unsyncedBuckets = %v partitionId = %v err = %v ", unsyncedBuckets, partitionId, err)
				}
			}
		}
	}
	return nil
}
