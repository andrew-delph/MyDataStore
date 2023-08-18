package main

import (
	"fmt"
	"io"
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

var store Store

func main() {
	// time.Sleep(20 * time.Second)

	defer func() {
		if r := recover(); r != nil {
			logrus.Warn("Recovered from panic:", r)
		}
	}()
	go StartInterGrpcServer()
	logrus.SetLevel(logrus.WarnLevel)
	// logrus.SetFormatter(&logrus.JSONFormatter{})

	data, err2 := os.ReadFile("/etc/hostname")
	if err2 != nil {
		logrus.Errorf("Error reading /etc/hostname: %v", err2)
		return
	}

	hostname = strings.TrimSpace(string(data))

	store = NewLevelDbStore()
	defer store.Close()

	store.InitStore()

	SetupRaft()

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

	tick := time.NewTicker(2 * time.Second)

	epochTick := time.NewTicker(epochTime)

	// var count uint32

	logrus.Warn("starting run.")

	run := true
	for run {
		select {

		case <-epochTick.C:

			if raftNode.State() != raft.Leader {
				continue
			}

			verifyErr := raftNode.VerifyLeader().Error()
			if verifyErr != nil {
				logrus.Warnf("verifyErr = %v", verifyErr)
			}

			raftLock.Lock()
			logEntry := raftNode.Apply(Uint64ToBytes(fsm.Epoch+1), 2*time.Second)

			err := logEntry.Error()

			if err == nil {
				logrus.Warnf("Leader Update Epoch. Epoch = %d", fsm.Epoch)
			} else {
				logrus.Debugf("update fsm Err= %v", err)
			}

			if response, ok := (logEntry.Response()).(string); ok {
				logrus.Debug("response = ", string(response), " ? ", hostname)
			} else {
				logrus.Warn("COULD NOT CAST. response = ", string(response))
			}

			raftLock.Unlock()

			err = raftNode.Snapshot().Error()
			if err != nil {
				logrus.Error("Snapshot Error ", err)
			}

		case <-tick.C:

			if raftNode.State() != raft.Leader && raftNode.State() != raft.Follower {
				logrus.Warnf("State = %s, Leader = %s, num_peers = %s, Epoch = %d hostname = %s", raftNode.State(), raftNode.Leader(), raftNode.Stats()["num_peers"], fsm.Epoch, conf.Name)
			}

			if raftNode.Leader() == "" {
				logrus.Warn("NO LEADER!")
				// possible bootstrap
			}

		case data := <-delegate.msgCh:

			messageHolder, message, err := DecodeMessageHolder(data)
			if err != nil {
				logrus.Fatal(err)
			}

			message.Handle(messageHolder)

		case epochObservation := <-epochObserver:
			logrus.Debugf("E= %d, S= %s", fsm.Epoch, raftNode.State())
			// logrus.Debugf("epochObservation %d %s", epoch, raftNode.State())
			myPartions, err := GetMemberPartions(events.consistent, conf.Name)
			if err != nil {
				logrus.Warn(err)
				continue
			}
			for _, partitionId := range myPartions {

				nodes, err := GetClosestNForPartition(events.consistent, partitionId, totalReplicas)
				if err != nil {
					logrus.Error(err)
					continue
				}

				for _, node := range nodes {
					epochRequest := epochObservation - 1
					unsyncedBuckets, err := VerifyMerkleTree(node.String(), epochRequest, partitionId)
					if err != nil && err != io.EOF {
						logrus.Debugf("VerifyMerkleTree unsyncedBuckets = %v err = %v ", unsyncedBuckets, err)
					} else if err != nil {
						logrus.Errorf("VerifyMerkleTree unsyncedBuckets = %v err = %v ", unsyncedBuckets, err)
					}

					if len(unsyncedBuckets) > 0 {
						var requstBuckets []int32
						for b := range unsyncedBuckets {
							requstBuckets = append(requstBuckets, b)
						}

						logrus.Warnf("CLIENT requstBuckets: %v", requstBuckets)

						StreamBuckets(node.String(), requstBuckets, epochRequest, partitionId)
					}
					// SyncPartition(partitionTree.Root.Hash, epochObservation-1, partitionId)

					// logrus.Warnf("SyncPartition CLIENT COMPLETED epoch = %d hash =  %x", epochObservation, partitionTree.Root.Hash)
					// events.SendRequestPartitionInfoMessage(partitionTree.Root.Hash, partitionId)
				}

			}
		case temp := <-raftNode.LeaderCh():
			logrus.Warnf("leader change. %t %s %d", temp, raftNode.State(), fsm.Epoch)

			if !temp {
				continue
			}

			succ := ""
			for i, node := range clusterNodes.Members() {
				// logrus.Warnf("node name = %s addr = %s", node.Name, node.Addr.String())
				// go func(node *memberlist.Node) {
				err := AddVoter(node.Name)
				if err != nil {
					logrus.Errorf("Testing AddVoter: %v", err)
					break
				} else {
					succ = fmt.Sprintf("%s,%d", succ, i)
				}
				// }(node)
			}
		}
	}

	// grpcStart()

	logrus.Warn("bye..............................")
}

var (
	raftDir      = "./raft-data"
	raftBindAddr = "127.0.0.1:7000" // 0.0.0.0:7000
)
