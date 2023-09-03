package main

import (
	"context"
	"fmt"
	"io"
	"time"

	datap "github.com/andrew-delph/my-key-store/datap"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func GetClient(addr string) (*grpc.ClientConn, *datap.InternalNodeServiceClient, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", addr, port), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	internalClient := datap.NewInternalNodeServiceClient(conn)

	return conn, &internalClient, nil
}

func (m Manager) SendSetMessageNode(nodeName string, setReqMsg *datap.Value, responseCh chan *datap.StandardResponse, errorCh chan error) {
	client, err := m.gossipCluster.GetNodeClient(nodeName)
	if err != nil {
		logrus.Errorf("GetClient for node %s", nodeName)
		errorCh <- err
	}

	// Create a new context for the goroutine
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	r, err := client.SetRequest(ctx, setReqMsg)
	if err != nil {
		logrus.Errorf("Failed SetRequest for node %s err = %v", nodeName, err)
		errorCh <- err
	} else {
		responseCh <- r
		logrus.Debugf("SetRequest %s worked. msg ='%s'", nodeName, r.Message)
	}
}

func (m Manager) SendSetMessage(key, value string) error {
	unixTimestamp := time.Now().Unix()
	setReqMsg := &datap.Value{Key: key, Value: value, Epoch: int64(globalEpoch), UnixTimestamp: unixTimestamp}

	nodes, err := m.hashRing.GetClosestN(key, m.Config.Manager.ReplicaCount)
	if err != nil {
		return err
	}

	responseCh := make(chan *datap.StandardResponse, m.Config.Manager.ReplicaCount)
	errorCh := make(chan error, m.Config.Manager.ReplicaCount)

	for _, node := range nodes {
		go m.SendSetMessageNode(node.name, setReqMsg, responseCh, errorCh)
	}

	timeout := time.After(m.Config.Manager.DefaultTimeout)
	responseCount := 0

	for responseCount < m.Config.Manager.WriteQuorum {
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

func (m Manager) SendGetMessage(key string) (string, error) {
	getReqMsg := &datap.GetRequestMessage{Key: key}

	nodes, err := m.hashRing.GetClosestN(key, m.Config.Manager.ReplicaCount)
	if err != nil {
		return "", err
	}

	type Res struct {
		Value *datap.Value
		Name  string
	}

	resCh := make(chan *Res, len(nodes))
	errorCh := make(chan error, len(nodes))

	for i, node := range nodes {
		go func(i int, node HashRingMember) {
			client, err := m.gossipCluster.GetNodeClient(node.name)
			if err != nil {
				errorCh <- err
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			res, err := client.GetRequest(ctx, getReqMsg)

			if err != nil {
				errorCh <- err
			} else {
				resCh <- &Res{Value: res, Name: node.name}
			}
		}(i, node)
	}

	timeout := time.After(m.Config.Manager.DefaultTimeout)

	var recentValue *datap.Value

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
					logrus.Debugf("update recentValue name = %s value = %v", res.Name, res.Value.Value)
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
		readRepairCount := 0
		for _, res := range resList {
			// logrus.Warnf("Read Repair addr = %s recentValue = %v value = %v", res.Addr, recentValue.Value, res.Value.Value)
			if proto.Equal(res.Value, recentValue) == false {
				readRepairCount++
				go m.SendSetMessageNode(res.Name, recentValue, make(chan *datap.StandardResponse), make(chan error))
			}
		}
		if readRepairCount > 0 {
			logrus.Debugf("Read Repair value = %v readRepairCount = %d", recentValue.Key, readRepairCount)
		}
	}()

	for len(resList) < m.Config.Manager.ReadQuorum {
		select {
		case res := <-resCh:

			if recentValue == nil {
				logrus.Debugf("setting recentValue nil.")
				recentValue = res.Value
			} else if recentValue.Epoch <= res.Value.Epoch && recentValue.UnixTimestamp < res.Value.UnixTimestamp {
				logrus.Debugf("update recentValue name = %s value = %v", res.Name, res.Value.Value)
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
	if len(resList) >= m.Config.Manager.ReadQuorum {
		if recentValue == nil {
			return "", fmt.Errorf("Value doesnt exist.")
		} else {
			return recentValue.Value, nil
		}
	}
	return "", fmt.Errorf("value not found. expected = %d recievedCount= %d", m.Config.Manager.ReadQuorum, len(resList))
}

func (m Manager) StreamBuckets(nodeName string, buckets *[]int32, lowerEpoch, upperEpoch int64, partitionId int) error {
	client, err := m.gossipCluster.GetNodeClient(nodeName)
	if err != nil {
		logrus.Errorf("CLIENT StreamBuckets err = %v", err)
		return err
	}

	req := &datap.StreamBucketsRequest{Buckets: *buckets, LowerEpoch: int64(lowerEpoch), UpperEpoch: int64(upperEpoch), Partition: int32(partitionId)}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	stream, err := client.StreamBuckets(ctx, req)
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
			logrus.Debugf("CLIENT StreamBuckets SetValue SUCCESS Key = %v Epoch = %v lowerEpoch = %v upperEpoch = %v", value.Key, value.Epoch, lowerEpoch, upperEpoch)
		}
	}
}

func (m Manager) GetPartitionEpochObject(nodeName string, epoch int64, partitionId int) (*datap.PartitionEpochObject, error) {
	client, err := m.gossipCluster.GetNodeClient(nodeName)
	if err != nil {
		return nil, err
	}

	// Create a new context for the goroutine
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &datap.PartitionEpochObject{Epoch: epoch, Partition: int32(partitionId)}

	res, err := client.GetPartitionEpochObject(ctx, req)
	if err != nil {
		return nil, err
	}

	return res, nil
}
