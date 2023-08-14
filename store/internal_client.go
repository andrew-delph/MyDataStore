package main

import (
	"context"
	"fmt"
	"time"

	pb "github.com/andrew-delph/my-key-store/proto"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func SendSetMessage(key, value string) error {
	unixTimestamp := time.Now().Unix()
	setReqMsg := &pb.Value{Key: key, Value: value, Epoch: int64(epoch), UnixTimestamp: unixTimestamp}

	nodes, err := GetClosestN(events.consistent, key, totalReplicas)
	if err != nil {
		return err
	}

	responseCh := make(chan *pb.StandardResponse, totalReplicas)
	errorCh := make(chan error, totalReplicas)

	for _, node := range nodes {
		go func(currNode HashRingMember) {
			conn, client, err := GetClient(currNode.String())
			if err != nil {
				logrus.Errorf("GetClient for node %s", currNode.String())
				errorCh <- err
			}
			defer conn.Close()

			// Create a new context for the goroutine
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			r, err := client.SetRequest(ctx, setReqMsg)
			if err != nil {
				logrus.Errorf("Failed SetRequest for node %s", currNode.String())
				errorCh <- err
			} else {
				responseCh <- r
				logrus.Debugf("SetRequest %s worked. msg ='%s'", currNode.String(), r.Message)
			}
		}(node)
	}

	timeout := time.After(defaultTimeout)
	responseCount := 0

	for responseCount < writeResponse {
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

	nodes, err := GetClosestN(events.consistent, key, totalReplicas)
	if err != nil {
		return "", err
	}

	getSet := make(map[string]int)
	responseCh := make(chan string, len(nodes))
	errorCh := make(chan error, len(nodes))

	for i, node := range nodes {
		go func(i int, node HashRingMember) {
			conn, client, err := GetClient(node.String())
			if err != nil {
				errorCh <- err
				return
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			r, err := client.GetRequest(ctx, getReqMsg)
			if err != nil {
				errorCh <- err
			} else {
				logrus.Debugf("GetRequest %s value='%s'", node.String(), r.Value)
				responseCh <- r.Value
			}
		}(i, node)
	}

	timeout := time.After(defaultTimeout)

	maxRecieved := 0

	for responseCount := 0; responseCount < len(nodes); responseCount++ {
		select {
		case value := <-responseCh:
			getSet[value]++
			if getSet[value] >= readResponse {
				return value, nil
			}
			maxRecieved = max(maxRecieved, getSet[value])
		case <-errorCh:
			// handle error if needed.
		case <-timeout:
			return "", fmt.Errorf("timed out waiting for responses")
		}
	}

	return "", fmt.Errorf("value not found. expected = %d maxRecieved= %d", readResponse, maxRecieved)
}

func GetClient(addr string) (*grpc.ClientConn, pb.InternalNodeServiceClient, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", addr, port), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	internalClient := pb.NewInternalNodeServiceClient(conn)

	return conn, internalClient, nil
}
