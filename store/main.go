package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
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

func main() {
	go StartInterGrpcServer()
	logrus.SetLevel(logrus.WarnLevel)
	// logrus.SetFormatter(&logrus.JSONFormatter{})

	InitStore()

	SetupRaft()

	time.Sleep(5 * time.Second)

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

	tick := time.NewTicker(60 * time.Second)

	countTick := time.NewTicker(1 * time.Second)

	// var count uint32
	count := uint32(0)

	run := true
	for run {
		select {
		case <-countTick.C:

			if raftNode.State() == raft.Leader {
				count++
			}
			logEntry := raftNode.Apply(Uint32ToBytes(count), 0*time.Second)

			err := logEntry.Error()
			if err == nil {
				logrus.Debugf("update fsm! count = %d", count)
				// if count%50 == 0 {
				// 	logrus.Warnf("killing leader. count = %d", count)
				// 	return
				// }
			} else {
				logrus.Debug("update fsm Err= ", err)
				// if rand.Float64() < 0.05 {
				// 	logrus.Debugf("killing FOLLOWER. count = %d", count)
				// 	return
				// }
			}

			logrus.Debug("err: ", err)
			logrus.Debug("fsm: ", fsm)
		case <-tick.C:

			// if err != nil {
			// 	log.Printf("Error applying log entry: %v", err)
			// } else {
			// 	log.Printf("Value updated: %s", newValue)
			// 	log.Printf("Log Index: %d, Term: %d", logEntry.Index, logEntry.Term)
			// }
			count = fsm.Count
			if raftNode.State() == raft.Leader {
				logrus.Warnf("State = %s , num_peers = %s, term = %s, Count = %d", raftNode.State(), raftNode.Stats()["num_peers"], raftNode.Stats()["term"], fsm.Count)
			}

			logrus.Debugf("Count = %d", fsm.Count)

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

var (
	raftDir      = "./raft-data"
	raftBindAddr = "127.0.0.1:7000" // 0.0.0.0:7000
)

func main2() {
	logrus.SetLevel(logrus.WarnLevel)
	hostname, exists := os.LookupEnv("HOSTNAME")
	if !exists {
		logrus.Fatal("hostname env not set.")
	} else {
		logrus.Infof("hostname='%s'", hostname)
	}

	localIp, err := getLocalIP()
	if err != nil {
		logrus.Fatal("getLocalIP failed", err)
	} else {
		logrus.Warnf("localIp='%s'", localIp)
	}

	raftBindAddr = fmt.Sprintf("%s:7000", localIp)

	raftDir = fmt.Sprintf("./raft_%s", hostname)

	// Clean up the previous state
	os.RemoveAll(raftDir)

	// Create the 'raft-data' directory if it doesn't exist
	if err := os.MkdirAll(raftDir, 0o700); err != nil {
		log.Fatal(err)
	}

	// Create a network transport for raftNode1
	transport, err := getTransport(raftBindAddr)
	if err != nil {
		log.Fatal(err)
	}

	// Create a configuration for raftNode1
	raftConf := raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(fmt.Sprintf("%s:7000", localIp))

	raftConf.LogLevel = "ERROR"

	// Create a store to persist Raft log entries
	store, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		log.Fatal(err)
	}

	// Create a snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stdout)
	if err != nil {
		log.Fatal(err)
	}

	// Create raftNode1
	raftNode1, err := raft.NewRaft(raftConf, nil, store, store, snapshotStore, transport)
	if err != nil {
		log.Fatal(err)
	}

	// Bootstrap the leader (raftNode1)
	bootstrapFuture := raftNode1.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raftConf.LocalID,
				Address:  raft.ServerAddress(transport.LocalAddr()),
			},
		},
	})
	if err := bootstrapFuture.Error(); err != nil {
		log.Fatal(err)
	}

	logrus.Info("Cluster set up successfully!")

	for {

		// Get the configuration
		future := raftNode1.GetConfiguration()
		if err := future.Error(); err != nil {
			logrus.Error("Failed to get raft configuration: ", err)
			continue
		}

		// Print the number of servers
		configuration := future.Configuration()
		numServers := len(configuration.Servers)

		// Print the leader of each node
		logrus.Warnf("Stats: Leader = %s , State = %s , numServers = %d", raftNode1.Leader(), raftNode1.State(), numServers)
		time.Sleep(4 * time.Second)

		if raftNode1.State() != raft.Leader {
			continue
		}

		// logrus.Infof("SLEEPING %s", raftNode1.State())

		// transport2, err := raft.NewTCPTransport("store:7000", nil, 3, 10*time.Second, os.Stdout)
		// if err != nil {
		// 	logrus.Fatal("transport2 error", err)
		// }

		// if err := raftNode1.AddVoter(raft.ServerID("new-voter-id"), raft.ServerAddress("store:7000"), 0, 0); err != nil {
		// 	log.Fatal(err)
		// }

		otherAddr := "172.26.0.3:7000"

		config2 := raft.DefaultConfig()
		config2.LocalID = raft.ServerID(otherAddr)

		addVoterFuture := raftNode1.AddVoter(config2.LocalID, raft.ServerAddress(otherAddr), 0, 0)
		if err := addVoterFuture.Error(); err != nil {
			logrus.Errorf("Error state: %s", raftNode1.State())
			logrus.Error("AddVoter error: ", err)
		}
	}

	// Keep the program running to maintain the Raft cluster
	select {}
}

func getTransport(bindAddr string) (*raft.NetworkTransport, error) {
	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	return raft.NewTCPTransport(bindAddr, addr, 3, 10*time.Second, os.Stderr)
}
