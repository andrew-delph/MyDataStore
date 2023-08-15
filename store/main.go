package main

import (
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
)

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

var epoch = uint64(0)

func main() {
	go StartInterGrpcServer()
	logrus.SetLevel(logrus.WarnLevel)
	// logrus.SetFormatter(&logrus.JSONFormatter{})

	InitStore()

	SetupRaft()

	time.Sleep(10 * time.Second)

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

	go startHttpServer()

	// verify partitions every x seconds

	partitionTimer := time.NewTicker(60 * time.Second)

	tick := time.NewTicker(10 * time.Second)

	epochTick := time.NewTicker(5 * time.Second)

	// var count uint32

	logrus.Warn("starting run.")

	run := true
	for run {
		select {
		case <-partitionTimer.C:
			continue

		case <-epochTick.C:

			if raftNode.State() != raft.Leader {
				continue
			}
			epoch++

			logEntry := raftNode.Apply(Uint64ToBytes(epoch), 0*time.Second)

			err := logEntry.Error()
			if err == nil {
				// logrus.Debugf("update fsm! epoch = %d", epoch)
			} else {
				logrus.Debugf("update fsm Err= %v", err)
			}

		case <-tick.C:

			// if err != nil {
			// 	log.Printf("Error applying log entry: %v", err)
			// } else {
			// 	log.Printf("Value updated: %s", newValue)
			// 	log.Printf("Log Index: %d, Term: %d", logEntry.Index, logEntry.Term)
			// }
			// epoch = fsm.Epoch
			// if raftNode.State() == raft.Leader {
			logrus.Warnf("State = %s, Leader = %s, num_peers = %s, Epoch = %d", raftNode.State(), raftNode.Leader(), raftNode.Stats()["num_peers"], fsm.Epoch)
			// }

			// logrus.Warnf("Epoch = %d", fsm.Epoch)

		case data := <-delegate.msgCh:

			messageHolder, message, err := DecodeMessageHolder(data)
			if err != nil {
				logrus.Fatal(err)
			}

			message.Handle(messageHolder)

		case epochObservation := <-epochObserver:
			epoch = epochObservation
			logrus.Warnf("epochObservation %d %s", epoch, raftNode.State())
			myPartions, err := GetMemberPartions(events.consistent, conf.Name)
			if err != nil {
				logrus.Warn(err)
				continue
			}
			for _, partitionId := range myPartions {
				partitionTree, err := PartitionMerkleTree(epoch-1, partitionId)
				if err != nil {
					logrus.Error(err)
					continue
				}
				SyncPartition(partitionTree.Root.Hash, epoch-1, partitionId)
				// logrus.Warnf("SyncPartition CLIENT COMPLETED epoch = %d hash =  %x", epochObservation, partitionTree.Root.Hash)
				// events.SendRequestPartitionInfoMessage(partitionTree.Root.Hash, partitionId)
			}
		}
	}

	// grpcStart()

	logrus.Info("bye..............................")
}

var (
	raftDir      = "./raft-data"
	raftBindAddr = "127.0.0.1:7000" // 0.0.0.0:7000
)
