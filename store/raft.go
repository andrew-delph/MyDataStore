package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/sirupsen/logrus"
)

var raftNode *raft.Raft

var trans *raft.NetworkTransport

var applyLock sync.RWMutex

var snapshotLock sync.RWMutex

type FSM struct {
	Epoch int64
}

var (
	validFSM         = false
	validFSMObserver = make(chan bool, 1)
)

var epochObserver = make(chan int64, 1)

func MakeRaftConf(localIp string) *raft.Config {
	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(fmt.Sprintf("%s:7000", localIp))
	// conf.SnapshotInterval = time.Second * 1
	// conf.SnapshotThreshold = 1

	if !raftLogs {
		raftLogger := hclog.New(&hclog.LoggerOptions{
			Name:   "discard",
			Output: io.Discard,
			Level:  hclog.NoLevel,
		})
		conf.Logger = raftLogger
		conf.LogLevel = "ERROR"
	}
	return conf
}

func (fsm *FSM) Apply(logEntry *raft.Log) interface{} {
	applyLock.Lock()
	defer applyLock.Unlock()
	epoch, err := DecodeBytesToInt64(logEntry.Data)
	if err != nil {
		logrus.Error("DecodeBytesToInt64 Error on Apply: %v", err)
		return nil
	}
	fsm.Epoch = epoch

	logrus.Warnf("E = %d state = %s fsm.index = %d last = %d applied = %d name = %s", epoch, raftNode.State(), logEntry.Index, raftNode.LastIndex(), raftNode.AppliedIndex(), conf.Name)

	if logEntry.Index == raftNode.AppliedIndex() && !validFSM {
		validFSM = true
		validFSMObserver <- validFSM
	} else if logEntry.Index != raftNode.AppliedIndex() && validFSM {
		validFSM = true
		validFSMObserver <- validFSM
	}
	epochObserver <- epoch
	return epoch
}

func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	// logrus.Warnf("Snapshot start")
	// defer logrus.Warnf("Snapshot done")

	snapshotLock.Lock()
	defer snapshotLock.Unlock()

	return &FSMSnapshot{stateValue: fsm.Epoch}, nil
}

func (fsm *FSM) Restore(serialized io.ReadCloser) error {
	var snapshot FSMSnapshot
	if err := json.NewDecoder(serialized).Decode(&snapshot); err != nil {
		return err
	}

	fsm.Epoch = snapshot.stateValue

	return serialized.Close()
}

type FSMSnapshot struct {
	stateValue int64 `json:"value"`
}

func (fsmSnapshot *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	// logrus.Warn("Persist!!!!!!!!!!!!!!!!!!!!!!!!1")
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
		logrus.Warnf("Persist ERROR = %v", err)
		sink.Cancel()
		return err
	}

	return nil
}

func (fsmSnapshot *FSMSnapshot) Release() {
}

var raftConf *raft.Config

var (
	logStore      *raftboltdb.BoltStore
	stableStore   *raftboltdb.BoltStore
	snapshotStore *raft.FileSnapshotStore
)

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

	raftBindAddr = fmt.Sprintf("%s:7000", localIp)
	advertiseAddr, err := net.ResolveTCPAddr("tcp", raftBindAddr)
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a store to persist Raft logrus entries
	logStore, err = raftboltdb.NewBoltStore(filepath.Join(raftDir, "log.db"))
	if err != nil {
		logrus.Fatal(err)
	}

	stableStore, err = raftboltdb.NewBoltStore(filepath.Join(raftDir, "stable.db"))
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a snapshot store
	snapshotStore, err = raft.NewFileSnapshotStore(filepath.Join(raftDir, "snap"), 2, io.Discard)
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a configuration for raftNode1
	raftConf = MakeRaftConf(localIp)

	logrus.Warnf("raftConf.SnapshotInterval %v", raftConf.SnapshotInterval)

	if trans != nil {
		err = trans.Close()
		if err != nil {
			logrus.Fatal(err)
		}
	}
	trans, err = raft.NewTCPTransport(raftBindAddr, advertiseAddr, 20, 0, os.Stderr)
	if err != nil {
		logrus.Fatal(err)
	}

	// tm := transport.New(raft.ServerAddress(raftBindAddr), []grpc.DialOption{grpc.WithInsecure()})

	fsm := &FSM{} // Your state machine instance
	// Create raftNode
	raftNode, err = raft.NewRaft(raftConf, fsm, logStore, stableStore, snapshotStore, trans)
	if err != nil {
		logrus.Fatal(err)
	}

	// updates := make(chan raft.Observation, 10) // Buffered channel
	// // 2. Create a goroutine to listen to this channel
	// go func() {
	// 	for update := range updates {
	// 		// Do something with the update
	// 		// For now, just print it
	// 		logrus.Warn("Received update:", update)
	// 	}
	// }()
	// obs := raft.NewObserver(updates, true, nil)
	// raftNode.RegisterObserver(obs)

	logrus.Warnf("after SetupRaft state = %s", raftNode.State())
}

func RaftBootstrap() error {
	bootstrapFuture := raftNode.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raftConf.LocalID,
				Address:  raft.ServerAddress(trans.LocalAddr()),
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

	otherRaftAddr := fmt.Sprintf("%s:7000", otherAddr)
	logrus.Warn("AddVoter otherRaftAddr ", otherRaftAddr)

	// if raftNode.AppliedIndex() > 20 {
	// 	addVoterFuture := raftNode.AddVoter(config2.LocalID, raft.ServerAddress(otherAddr), raftNode.AppliedIndex(), time.Second)
	// 	return addVoterFuture.Error()
	// } else {
	// 	addVoterFuture := raftNode.AddVoter(config2.LocalID, raft.ServerAddress(otherAddr), 0, time.Second)
	// 	return addVoterFuture.Error()
	// }
	addVoterFuture := raftNode.AddVoter(raft.ServerID(otherRaftAddr), raft.ServerAddress(otherRaftAddr), 0, 0)
	return addVoterFuture.Error()
}

func RemoveServer(otherAddr string) error {
	err := raftNode.VerifyLeader().Error()
	if err != nil {
		return err
	}

	otherRaftAddr := fmt.Sprintf("%s:7000", otherAddr)
	logrus.Warn("RemoveServer otherRaftAddr ", otherRaftAddr)

	// raftLock.Lock()         // Lock the critical section
	// defer raftLock.Unlock() // Ensure the lock is released once the function completes

	// err = raftNode.DemoteVoter(raft.ServerID(otherRaftAddr), raftNode.GetConfiguration().Index(), defaultTimeout).Error()
	// if err != nil {
	// 	for i := 0; i < 100; i++ {
	// 		logrus.Warn("DemoteVoter ", err)
	// 	}
	// 	return err
	// }

	// raftNode.GetConfiguration().Index()
	err = raftNode.RemoveServer(raft.ServerID(otherRaftAddr), 0, 0).Error()
	if err != nil {
		for i := 0; i < 100; i++ {
			logrus.Warn("RemoveServer ", err)
		}
		return err
	}

	// err = raftNode.ReloadConfig(raftNode.ReloadableConfig())
	// if err != nil {
	// 	for i := 0; i < 100; i++ {
	// 		logrus.Warn("ReloadConfig ", err)
	// 	}
	// 	return err
	// }

	// servers := raftNode.GetConfiguration().Configuration()

	// for i := 0; i < 100; i++ {
	// 	logrus.Warn("servers ", servers)
	// }

	// err = raft.RecoverCluster(raftConf, &FSM{}, logStore, stableStore, snapshotStore, trans, raftNode.GetConfiguration().Configuration())
	// if err != nil {
	// 	for i := 0; i < 100; i++ {
	// 		logrus.Warn("RecoverCluster ", err)
	// 	}
	// 	return err
	// }

	return err
}

func Snapshot() error {
	// if raftNode.LastIndex() != raftNode.AppliedIndex() {
	// 	logrus.Warnf("INDEX NOT EQUAL %d %d currEpoch = %d", raftNode.LastIndex(), raftNode.AppliedIndex(), currEpoch)
	// 	return nil
	// }
	err := raftNode.Snapshot().Error()
	if err != nil {
		logrus.Error("Snapshot Error ", err)
	}
	return err
}

func UpdateEpoch() error {
	logrus.Warnf("Leader Update Epoch. Epoch = %d", currEpoch+1)

	epochBytes, err := EncodeInt64ToBytes(currEpoch + 1)
	if err != nil {
		return err
	}
	logEntry := raftNode.Apply(epochBytes, defaultTimeout)
	err = logEntry.Error()

	if err == nil {
		// logrus.Warnf("Leader Update Epoch. Epoch = %d", currEpoch)
	} else {
		logrus.Warnf("update fsm Err= %v", err)
	}
	logrus.Warnf("Done.")
	return err
}
