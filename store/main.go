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

var currEpoch uint64 = 0

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

	store, err2 = NewLevelDbStore()
	if err2 != nil {
		logrus.Fatalf("NewLevelDbStore: %v", err2)
	}
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

	// go func() {
	// 	tick := time.NewTicker(3 * time.Second)
	// 	for true {
	// 		select {
	// 		case <-tick.C:
	// 			// logrus.Warnf("Apply E = %d state = %s last = %d applied = %d", currEpoch, raftNode.State(), raftNode.LastIndex(), raftNode.AppliedIndex())
	// 			// if raftNode.State() != raft.Leader && raftNode.State() != raft.Follower {
	// 			// logrus.Warnf("E = %d state = %s last = %d applied = %d", currEpoch, raftNode.State(), raftNode.LastIndex(), raftNode.AppliedIndex())
	// 			// }

	// 			if raftNode.Leader() == "" {
	// 				logrus.Warnf("NO LEADER! state = %s  err = %v", raftNode.State(), RaftTryLead())
	// 				// RaftBootstrap()
	// 			}
	// 		}
	// 	}
	// }()

	epochTick := time.NewTicker(epochTime)

	// var count uint32

	logrus.Warn("starting run.")
	tick := time.NewTicker(3 * time.Second)
	run := true
	for run {
		select {
		case <-tick.C:
			// logrus.Warnf("Apply E = %d state = %s last = %d applied = %d", currEpoch, raftNode.State(), raftNode.LastIndex(), raftNode.AppliedIndex())
			// if raftNode.State() != raft.Leader && raftNode.State() != raft.Follower {
			// logrus.Warnf("E = %d state = %s last = %d applied = %d", currEpoch, raftNode.State(), raftNode.LastIndex(), raftNode.AppliedIndex())
			// }

			if raftNode.Leader() == "" {
				logrus.Warnf("NO LEADER! state = %s ME = %s err = %v", raftNode.State(), conf.Name, nil) // RaftTryLead()
				RaftTryLead()
			}

			if raftNode.State() != raft.Leader && raftNode.State() != raft.Follower {
				logrus.Warnf("BAD state = %s ME = %s", raftNode.State(), conf.Name)
				// RaftBootstrap()
			}

		case <-epochTick.C:

			if raftNode.State() != raft.Leader {
				continue
			}

			verifyErr := raftNode.VerifyLeader().Error()
			if verifyErr != nil {
				logrus.Warnf("verifyErr = %v", verifyErr)
				continue
			}

			// raftLock.Lock()
			UpdateEpoch()

			// if response, ok := (logEntry.Response()).(string); ok {
			// 	logrus.Debugf("response = %s ? %s ", string(response), hostname)
			// } else {
			// 	logrus.Debugf("COULD NOT CAST. response = %s", string(response))
			// }

			// raftLock.Unlock()

			// Snapshot()

		case data := <-delegate.msgCh:

			messageHolder, message, err := DecodeMessageHolder(data)
			if err != nil {
				logrus.Fatal(err)
			}

			message.Handle(messageHolder)

		case currEpoch = <-epochObserver:
			// continue

			// raft
			// raftNode

			// err := Snapshot()
			// if err != nil {
			// 	logrus.Warnf("Snapshot err = %v", err)
			// 	continue
			// }
			// logrus.Debugf("epochObservation %d %s", epoch, raftNode.State())
			continue
			myPartions, err := GetMemberPartions(events.consistent, conf.Name)
			if err != nil {
				logrus.Warn(err)
				continue
			}
			for _, partitionId := range myPartions {

				nodes, err := GetClosestNForPartition(events.consistent, partitionId, N)
				if err != nil {
					logrus.Error(err)
					continue
				}

				for _, node := range nodes {
					epochRequest := currEpoch - 1
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
				}

			}
		case temp := <-raftNode.LeaderCh():

			logrus.Warnf("leader change. %t %s %d", temp, raftNode.State(), currEpoch)

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
