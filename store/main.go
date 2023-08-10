package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	pb "github.com/andrew-delph/cloud-video-call/common-messaging/proto"
	"github.com/hashicorp/memberlist"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// pb "github.com/andrew-delph/cloud-video-call/common-messaging/proto"
var port = flag.Int("port", 80, "The server port")

type server struct {
	pb.UnimplementedDataServiceServer
}

func grpcStart() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("Uncaught panic: %v", r)
			// Perform any necessary cleanup or error handling here
		}
	}()

	logrus.Info("STARTING !!!")

	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		logrus.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterDataServiceServer(s, &server{})
	logrus.Infof("server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

var (
	clusterNodes *memberlist.Memberlist
	delegate     *MyDelegate
	events       *MyEventDelegate
	conf         *memberlist.Config
	ackMap       sync.Map
)

func setAckChannel(key string, ch chan *MessageHolder) {
	ackMap.Store(key, ch)
}

func getAckChannel(key string) (chan *MessageHolder, bool) {
	if value, ok := ackMap.Load(key); ok {
		return value.(chan *MessageHolder), true
	}
	return nil, false
}

func deleteAckChannel(key string) {
	ackMap.Delete(key)
}

var (
	hostname   string
	myPartions []int
)

func main() {
	logrus.SetLevel(logrus.WarnLevel)
	// logrus.SetFormatter(&logrus.JSONFormatter{})

	InitStore()

	data, err2 := os.ReadFile("/etc/hostname")
	if err2 != nil {
		logrus.Errorf("Error reading /etc/hostname: %v", err2)
		return
	}

	hostname = strings.TrimSpace(string(data))

	logrus.Infof("starting! %s", hostname)

	conf, delegate, events = GetConf()

	var err error
	clusterNodes, err = memberlist.Create(conf)
	if err != nil {
		logrus.Panic("Failed to create memberlist: " + err.Error())
	}

	// Join an existing cluster by specifying at least one known member.
	n, err := clusterNodes.Join([]string{"store:8081"})
	if err != nil {
		logrus.Panic("Failed to join cluster: " + err.Error())
	}

	logrus.Info("n", n)

	// Ask for members of the cluster
	for _, member := range clusterNodes.Members() {
		logrus.Infof("Member: %s %s\n", member.Name, member.Addr)
	}

	tick := time.NewTicker(50000 * time.Millisecond)

	go startHttpServer()

	// verify partitions every x seconds

	partitionTimer := time.NewTicker(1000 * time.Millisecond)
	go func() {
		for range partitionTimer.C {
			myPartions, err := GetMemberPartions(events.consistent, hostname)
			if err != nil {
				logrus.Warn(err)
				continue
			}

			for _, partitionId := range myPartions {
				partitionTree, err := PartitionMerkleTree(partitionId)
				if err != nil {
					logrus.Debug(err)
					continue
				}
				events.SendRequestPartitionInfoMessage(partitionTree.Root.Hash, partitionId)
			}
		}
	}()

	run := true
	for run {
		select {
		case <-tick.C:
			// value := randomString(5)

			// key := randomString(5)

			logrus.Debug("TICK VALUE")

			// go events.SendSetMessage(key, value, 2)

		case data := <-delegate.msgCh:

			messageHolder, message, err := DecodeMessageHolder(data)
			if err != nil {
				logrus.Fatal(err)
			}

			message.Handle(messageHolder)

		}
	}

	// grpcStart()

	logrus.Info("bye..............................")
}
