package consensus

import (
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

	"github.com/andrew-delph/my-key-store/config"
	"github.com/andrew-delph/my-key-store/utils"
)

func consensusTest() {
	logrus.Warn("CONSENSUS")
}

type ConsensusCluster struct {
	raftConf        *raft.Config
	reqCh           chan interface{}
	consensusConfig config.ConsensusConfig
	raftNode        *raft.Raft
}

type LeaderChangeTask struct {
	IsLeader bool
}

func CreateConsensusCluster(consensusConfig config.ConsensusConfig, reqCh chan interface{}) ConsensusCluster {
	raftConf := raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(consensusConfig.Name)
	// conf.SnapshotInterval = time.Second * 1
	// conf.SnapshotThreshold = 1
	// logrus.Infof("conf.ElectionTimeout %v", raftConf.ElectionTimeout)
	// logrus.Infof("conf.HeartbeatTimeout %v", raftConf.HeartbeatTimeout)
	// logrus.Infof("conf.LeaderLeaseTimeout %v", raftConf.LeaderLeaseTimeout)
	// logrus.Infof("conf.CommitTimeout %v", raftConf.CommitTimeout)
	// logrus.Infof("conf.SnapshotInterval %v", raftConf.SnapshotInterval)
	// logrus.Infof("conf.SnapshotThreshold %v", raftConf.SnapshotThreshold)

	if !consensusConfig.EnableLogs {
		raftLogger := hclog.New(&hclog.LoggerOptions{
			Name:   "discard",
			Output: io.Discard,
			Level:  hclog.NoLevel,
		})
		raftConf.Logger = raftLogger
		raftConf.LogLevel = "ERROR"
	}
	// logrus.Warn("consensusConfig ", consensusConfig.DataPath)
	return ConsensusCluster{consensusConfig: consensusConfig, reqCh: reqCh, raftConf: raftConf, raftNode: new(raft.Raft)}
}

func (consensusCluster *ConsensusCluster) StartConsensusCluster() error {
	logrus.Warn("StartConsensusCluster")

	raftDir := fmt.Sprintf("%s/%s", consensusCluster.consensusConfig.DataPath, consensusCluster.consensusConfig.Name)
	// Create the 'raft-data' directory if it doesn't exist
	if err := os.MkdirAll(raftDir, 0o700); err != nil {
		logrus.Fatal(err)
	}

	raftBindAddr := fmt.Sprintf("%s:7000", consensusCluster.consensusConfig.Name)
	advertiseAddr, err := net.ResolveTCPAddr("tcp", raftBindAddr)
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

	trans, err := raft.NewTCPTransport(raftBindAddr, advertiseAddr, 20, 0, os.Stderr)
	if err != nil {
		logrus.Fatal(err)
	}

	fsm := &FSM{} // Your state machine instance
	// Create raftNode
	raftNode, err := raft.NewRaft(consensusCluster.raftConf, fsm, logStore, stableStore, snapshotStore, trans)
	if err != nil {
		logrus.Fatal(err)
	}

	consensusCluster.raftNode = raftNode

	// start the consensus worker
	go consensusCluster.startWorker()

	if consensusCluster.consensusConfig.AutoBootstrap {
		go func() {
			time.Sleep(time.Duration(consensusCluster.consensusConfig.BootstrapTimeout) * time.Second)
			if (raftNode.State() != raft.Leader && raftNode.State() != raft.Follower) || raftNode.Leader() == "" {
				err := consensusCluster.RaftBootstrap()
				if err != nil {
					logrus.Errorf("RaftBootstrap timeout err = %v", err)
				}
			} else {
				return
			}
		}()
	}

	return nil
}

func (consensusCluster *ConsensusCluster) startWorker() {
	for true {
		select {
		case isLeader := <-consensusCluster.raftNode.LeaderCh():
			logrus.Debugf("leader change. %t %s", isLeader, consensusCluster.raftNode.State())
			consensusCluster.reqCh <- LeaderChangeTask{IsLeader: isLeader}
		}
	}
}

func (consensusCluster *ConsensusCluster) RaftBootstrap() error {
	// TODO dont create the advertiseAddr again...
	raftBindAddr := fmt.Sprintf("%s:7000", consensusCluster.consensusConfig.Name)
	advertiseAddr, err := net.ResolveTCPAddr("tcp", raftBindAddr)
	if err != nil {
		logrus.Fatal(err)
	}
	bootstrapFuture := consensusCluster.raftNode.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       consensusCluster.raftConf.LocalID,
				Address:  raft.ServerAddress(advertiseAddr.String()),
			},
		},
	})
	return bootstrapFuture.Error()
}

func (consensusCluster *ConsensusCluster) AddVoter(nodeName, nodeIP string) error {
	// err := consensusCluster.raftNode.VerifyLeader().Error()
	// if err != nil {
	// 	logrus.Warn("ERRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR")
	// 	return err
	// }
	// logrus.Warn("leader")
	noderRaftAddr := fmt.Sprintf("%s:7000", nodeIP)
	logrus.Debugf("AddVoter! STATE = %s nodeName = %s noderRaftAddr = %s", consensusCluster.raftNode.State(), nodeName, noderRaftAddr)

	addVoterFuture := consensusCluster.raftNode.AddVoter(raft.ServerID(nodeName), raft.ServerAddress(noderRaftAddr), 0, 0)
	return addVoterFuture.Error()
}

func (consensusCluster *ConsensusCluster) RemoveServer(nodeName string) error {
	err := consensusCluster.raftNode.VerifyLeader().Error()
	if err != nil {
		return nil
	}

	return consensusCluster.raftNode.RemoveServer(raft.ServerID(nodeName), 0, 0).Error()
}

var globalEpoch = int64(1)

func (consensusCluster *ConsensusCluster) UpdateEpoch() error {
	err := consensusCluster.raftNode.VerifyLeader().Error()
	if err != nil {
		return nil
	}
	globalEpoch++
	logrus.Warnf("Leader Update Epoch. Epoch = %d", globalEpoch)

	epochBytes, err := utils.EncodeInt64ToBytes(globalEpoch)
	if err != nil {
		logrus.Errorf("EncodeInt64ToBytes Err= %v", err)
		return err
	}
	logEntry := consensusCluster.raftNode.Apply(epochBytes, 0)
	err = logEntry.Error()

	if err == nil {
		// logrus.Warnf("Leader Update Epoch. Epoch = %d", currEpoch)
	} else {
		logrus.Warnf("update fsm Err= %v", err)
	}
	logrus.Debugf("Done.")
	return err
}
