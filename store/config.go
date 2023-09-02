package main

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
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
	dataPath         string        = "/tmp/store" // this will stop k8 from persist
	raftLogs         bool          = false
	autoBootstrap    bool          = true
	bootstrapTimeout time.Duration = 30 * time.Second
	clusterJoinList                = []string{"store:8081"}
)

type ManagerConfig struct {
	ReplicaCount     int `mapstructure:"REPLICA_COUNT"`
	WriteQuorum      int `mapstructure:"WRITE_QUORUM"`
	ReadQuorum       int `mapstructure:"READ_QUORUM"`
	SaveInterval     time.Duration
	DefaultTimeout   time.Duration
	PartitionBuckets int `mapstructure:"PARTITION_BUCKETS"`
	PartitionCount   int `mapstructure:"PARTITION_COUNT"`
}

type RaftConfig struct {
	DataPath         string `mapstructure:"DATA_PATH"`
	EpochTime        int    `mapstructure:"EPOCH_TIME"`
	EnableLogs       bool   `mapstructure:"ENABLE_LOGS"`
	AutoBootstrap    bool   `mapstructure:"AUTO_BOOTSTRAP"`
	BootstrapTimeout int    `mapstructure:"BOOTSTRAP_TIMEOUT"`
}

type MemberListConfig struct {
	InitMembers string `mapstructure:"INIT_MEMBERS"`
}

type Config struct {
	Manager    ManagerConfig    `mapstructure:"MANAGER"`
	Raft       RaftConfig       `mapstructure:"RAFT"`
	MemberList MemberListConfig `mapstructure:"MEMBERLIST"`
}

func GetConfig() Config {
	viper.SetConfigName(filepath.Join(os.Getenv("CONFIG_PATH"), "default-config"))

	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		logrus.Fatalf("Error reading default config file, %s", err)
	}

	// Override with configuration from config.yaml
	viper.SetConfigName(filepath.Join(os.Getenv("CONFIG_PATH"), "config"))
	if err := viper.MergeInConfig(); err != nil {
		logrus.Infof("No config file found. Using Defaults.")
	} else {
		logrus.Infof("Config file found.")
	}

	var config Config
	err := viper.Unmarshal(&config)
	if err != nil {
		logrus.Fatalf("unable to decode into struct, %v", err)
	}

	return config
}

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

func CreateMemberlistConf() (*memberlist.Config, *MyDelegate, *MyEventDelegate) {
	delegate := GetMyDelegate()
	events := GetMyEventDelegate()

	conf := memberlist.DefaultLocalConfig()
	conf.Logger = log.New(io.Discard, "", 0)
	conf.BindPort = 8081
	conf.AdvertisePort = 8081
	conf.Delegate = delegate
	conf.Events = events

	conf.Name = hostname

	return conf, delegate, events
}

func CreateRaftConf() *raft.Config {
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
