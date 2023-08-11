package main

import (
	"context"
	"fmt"
	"time"

	pb "github.com/andrew-delph/my-key-store/proto"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func (s *internalServer) TestRequest(ctx context.Context, in *pb.StandardResponse) (*pb.StandardResponse, error) {
	logrus.Warnf("Received: %v", in.Message)
	return &pb.StandardResponse{Message: "This is the server."}, nil
}

func SendSetMessage(key, value string) error {
	setReqMsg := &pb.SetRequestMessage{Key: key, Value: value}

	nodes, err := GetClosestN(events.consistent, key, totalReplicas)
	if err != nil {
		return err
	}

	for i, node := range nodes {

		conn, client, err := GetClient(node.String())

		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		r, err := client.SetRequest(ctx, setReqMsg)
		if err != nil {
			return err
		}
		logrus.Warnf("SetRequest %d worked. msg ='%s'", i, r.Message)
	}
	return nil
	// return fmt.Errorf("value not found.")

	// ackSet := make(map[string]bool)

	// timeout := time.After(defaultTimeout)

	// for {
	// 	select {
	// 	case ackMessageHolder := <-ackChannel:
	// 		ackSet[ackMessageHolder.SenderName] = true
	// 		if len(ackSet) >= writeResponse {
	// 			return nil
	// 		}
	// 	case <-timeout:
	// 		logrus.Debug("SET TIME OUT REACHED!!! len(ackSet) = ", len(ackSet))
	// 		return fmt.Errorf("timeout waiting for acknowledgements")
	// 	}
	// }
}

func SendGetMessage(key string) (string, error) {
	getReqMsg := &pb.GetRequestMessage{Key: key}

	nodes, err := GetClosestN(events.consistent, key, totalReplicas)
	if err != nil {
		return "", err
	}

	getSet := make(map[string]int)

	for i, node := range nodes {

		conn, client, err := GetClient(node.String())

		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		r, err := client.GetRequest(ctx, getReqMsg)
		if err != nil {
			continue
		}
		logrus.Warnf("GetRequest %d value='%s'", i, r.Value)
		getSet[r.Value]++

		if getSet[r.Value] >= readResponse {
			return r.Value, nil
		}
	}

	return "", fmt.Errorf("value not found.")

	// ackSet := make(map[string]int)

	// timeout := time.After(defaultTimeout)

	// for {
	// 	select {
	// 	case ackMessageHolder := <-ackChannel:

	// 		ackMessage := &AckMessage{}
	// 		err := ackMessage.Decode(ackMessageHolder.MessageBytes)
	// 		if err != nil {
	// 			return "", fmt.Errorf("failed to Decode AckMessage: %v", err)
	// 		}

	// 		ackValue := ackMessage.Value
	// 		ackSet[ackValue]++

	// 		if ackSet[ackValue] == readResponse {
	// 			return ackValue, nil
	// 		}
	// 	case <-timeout:
	// 		logrus.Warn("TIME OUT REACHED!!!")
	// 		return "", fmt.Errorf("timeout waiting for acknowledgements")
	// 	}
	// }
}

func GetClient(addr string) (*grpc.ClientConn, pb.InternalNodeServiceClient, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", addr, port), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	internalClient := pb.NewInternalNodeServiceClient(conn)

	return conn, internalClient, nil

	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()
	// r, err := internalClient.TestRequest(ctx, &pb.StandardResponse{Message: "This is the client."})
	// if err != nil {
	// 	logrus.Fatalf("could not greet: %v", err)
	// }
	// logrus.Warnf("Greeting: %s", r.Message)
}
