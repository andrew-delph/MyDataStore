package main

import (
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
)

var hostname string

var (
	ReplicaCount     int           = 3
	WriteQuorum      int           = 2
	ReadQuorum       int           = 2
	saveInterval     time.Duration = 30 * time.Second
	defaultTimeout   time.Duration = 2 * time.Second
	partitionBuckets int           = 500
	partitionCount   int           = 100
	epochTime        time.Duration = 100 * time.Second
	dataPath         string        = "/store"
	raftLogs         bool          = false
	autoBootstrap    bool          = true
	bootstrapTimeout time.Duration = 30 * time.Second
	clusterJoinList                = []string{"store:8081"}
)

func Init() {
	var err error

	hostname, err = os.Hostname()
	if err != nil {
		logrus.Fatalf("hostname err = %v", err)
	}

	dataPathValue, exists := os.LookupEnv("DATA_PATH")
	if exists {
		dataPath = dataPathValue
	}

	clusterJoinListValue, exists := os.LookupEnv("CLUSTER_JOIN_LIST")
	if exists {
		clusterJoinList = strings.Split(clusterJoinListValue, ",")
	}

	replicaCountValue, exists := os.LookupEnv("REPLICA_COUNT")
	if exists {
		ReplicaCount, err = strconv.Atoi(replicaCountValue)
		if err != nil {
			logrus.Fatal(err)
		}
	}

	writeQuorumValue, exists := os.LookupEnv("WRITE_QUORUM")
	if exists {
		WriteQuorum, err = strconv.Atoi(writeQuorumValue)
		if err != nil {
			logrus.Fatal(err)
		}
	}

	readQuorumValue, exists := os.LookupEnv("READ_QUORUM")
	if exists {
		ReadQuorum, err = strconv.Atoi(readQuorumValue)
		if err != nil {
			logrus.Fatal(err)
		}
	}

	epochTimeValue, exists := os.LookupEnv("EPOCH_TIME")
	if exists {
		seconds, err := strconv.Atoi(epochTimeValue)
		if err != nil {
			logrus.Fatal(err)
		}
		epochTime = time.Duration(seconds) * time.Second
	}

	autoBootstrap = os.Getenv("AUTO_BOOTSTRAP") != "false"

	bootstrapTimeoutValue, exists := os.LookupEnv("BOOTSTRAP_TIMEOUT")
	if exists {
		seconds, err := strconv.Atoi(bootstrapTimeoutValue)
		if err != nil {
			logrus.Fatal(err)
		}
		bootstrapTimeout = time.Duration(seconds) * time.Second
	}
}

func GetMemberlistConf() (*memberlist.Config, *MyDelegate, *MyEventDelegate) {
	delegate := GetMyDelegate()
	events := GetMyEventDelegate()

	logrus.Infof("clusterJoinListValue = %s", clusterJoinList)
	logrus.Infof("dataPath = %s", dataPath)

	conf := memberlist.DefaultLocalConfig()
	conf.Logger = log.New(io.Discard, "", 0)
	conf.BindPort = 8081
	conf.AdvertisePort = 8081
	conf.Delegate = delegate
	conf.Events = events

	conf.Name = hostname

	return conf, delegate, events
}

func GetRaftConf() *raft.Config {
	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(hostname)
	// conf.SnapshotInterval = time.Second * 1
	// conf.SnapshotThreshold = 1
	logrus.Infof("conf.ElectionTimeout %v", conf.ElectionTimeout)
	logrus.Infof("conf.HeartbeatTimeout %v", conf.HeartbeatTimeout)
	logrus.Infof("conf.LeaderLeaseTimeout %v", conf.LeaderLeaseTimeout)
	logrus.Infof("conf.CommitTimeout %v", conf.CommitTimeout)
	logrus.Infof("conf.SnapshotInterval %v", conf.SnapshotInterval)
	logrus.Infof("conf.SnapshotThreshold %v", conf.SnapshotThreshold)

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
