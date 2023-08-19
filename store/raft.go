package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/sirupsen/logrus"
)

var raftNode *raft.Raft

type FSM struct {
	Epoch uint64
}

var epochObserver = make(chan uint64, 1)

func (fsm *FSM) Apply(logEntry *raft.Log) interface{} {
	epoch := binary.BigEndian.Uint64(logEntry.Data)
	fsm.Epoch = epoch
	epochObserver <- epoch
	return nil
}

func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &FSMSnapshot{stateValue: fsm.Epoch}, nil
}

func (fsm *FSM) Restore(serialized io.ReadCloser) error {
	var snapshot FSMSnapshot
	if err := json.NewDecoder(serialized).Decode(&snapshot); err != nil {
		return err
	}

	fsm.Epoch = snapshot.stateValue

	return nil
}

type FSMSnapshot struct {
	stateValue uint64 `json:"value"`
}

func (fsmSnapshot *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		snapshotBytes, err := json.Marshal(fsmSnapshot)
		if err != nil {
			return err
		}

		if _, err := sink.Write(snapshotBytes); err != nil {
			return err
		}

		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()
	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (fsmSnapshot *FSMSnapshot) Release() {
}

var raftConf *raft.Config

var transport *raft.NetworkTransport

func SetupRaft() {
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

	raftDir = fmt.Sprintf("/store/raft/raft_%s", hostname)

	// Clean up the previous state
	// os.RemoveAll(raftDir)

	// Create the 'raft-data' directory if it doesn't exist
	if err := os.MkdirAll(raftDir, 0o700); err != nil {
		logrus.Fatal(err)
	}

	// Create a network transport for raftNode1
	if transport != nil {
		err = transport.Close()
		if err != nil {
			logrus.Fatal(err)
		}
	}

	raftBindAddr = fmt.Sprintf("%s:7000", localIp)
	advertiseAddr, err := net.ResolveTCPAddr("tcp", raftBindAddr)
	if err != nil {
		logrus.Fatal(err)
	}

	transport, err = raft.NewTCPTransport(raftBindAddr, advertiseAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a store to persist Raft logrus entries
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "log.db"))
	if err != nil {
		logrus.Fatal(err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "stable.db"))
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(raftDir, "snap"), 2, io.Discard)
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a configuration for raftNode1
	raftConf = raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(fmt.Sprintf("%s:7000", localIp))

	if !raftLogs {
		raftLogger := hclog.New(&hclog.LoggerOptions{
			Name:   "discard",
			Output: io.Discard,
			Level:  hclog.NoLevel,
		})
		raftConf.Logger = raftLogger
		raftConf.LogLevel = "ERROR"
	}

	logrus.Warnf("raftConf.SnapshotInterval %v ... %v", raftConf.SnapshotInterval)

	fsm := &FSM{} // Your state machine instance
	// Create raftNode
	raftNode, err = raft.NewRaft(raftConf, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		logrus.Fatal(err)
	}

	logrus.Warnf("after SetupRaft state = %s", raftNode.State())
}

func RaftBootstrap() error {
	bootstrapFuture := raftNode.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raftConf.LocalID,
				Address:  raft.ServerAddress(transport.LocalAddr()),
			},
		},
	})
	return bootstrapFuture.Error()
}

func RaftTryLead() error {
	err := RaftBootstrap()
	if err != nil {
		return err
	}
	succ := ""
	for i, node := range clusterNodes.Members() {
		// logrus.Warnf("node name = %s addr = %s", node.Name, node.Addr.String())
		// go func(node *memberlist.Node) {
		err := AddVoter(node.Name)
		if err != nil {
			logrus.Errorf("Testing AddVoter: %v", err)
			return nil
		} else {
			succ = fmt.Sprintf("%s,%d", succ, i)
		}
		// }(node)
	}
	return nil
}

// var raftLock sync.Mutex

func AddVoter(otherAddr string) error {
	err := raftNode.VerifyLeader().Error()
	if err != nil {
		return err
	}

	// raftLock.Lock()         // Lock the critical section
	// defer raftLock.Unlock() // Ensure the lock is released once the function completes

	otherAddr = fmt.Sprintf("%s:7000", otherAddr)

	config2 := raft.DefaultConfig()
	config2.LocalID = raft.ServerID(otherAddr)
	// if raftNode.AppliedIndex() > 20 {
	// 	addVoterFuture := raftNode.AddVoter(config2.LocalID, raft.ServerAddress(otherAddr), raftNode.AppliedIndex(), time.Second)
	// 	return addVoterFuture.Error()
	// } else {
	// 	addVoterFuture := raftNode.AddVoter(config2.LocalID, raft.ServerAddress(otherAddr), 0, time.Second)
	// 	return addVoterFuture.Error()
	// }
	addVoterFuture := raftNode.AddVoter(config2.LocalID, raft.ServerAddress(otherAddr), 0, time.Second)
	return addVoterFuture.Error()
}

func RemoveServer(otherAddr string) error {
	err := raftNode.VerifyLeader().Error()
	if err != nil {
		return err
	}

	// raftLock.Lock()         // Lock the critical section
	// defer raftLock.Unlock() // Ensure the lock is released once the function completes

	removeServerFuture := raftNode.RemoveServer(raft.ServerID(otherAddr), 0, time.Second)

	return removeServerFuture.Error()
}
