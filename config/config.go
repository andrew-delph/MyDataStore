package config

import (
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// var (
// 	ReplicaCount     int           = 3
// 	WriteQuorum      int           = 2
// 	ReadQuorum       int           = 2
// 	saveInterval     time.Duration = 30 * time.Second
// 	defaultTimeout   time.Duration = 2 * time.Second
// 	partitionBuckets int           = 500
// 	partitionCount   int           = 100
// 	epochTime        time.Duration = 100 * time.Second
// 	dataPath         string        = "/tmp/store" // this will stop k8 from persist
// 	raftLogs         bool          = false
// 	autoBootstrap    bool          = true
// 	bootstrapTimeout time.Duration = 30 * time.Second
// 	clusterJoinList                = []string{"store:8081"}
// )

type ManagerConfig struct {
	ReplicaCount     int `mapstructure:"REPLICA_COUNT"`
	WriteQuorum      int `mapstructure:"WRITE_QUORUM"`
	ReadQuorum       int `mapstructure:"READ_QUORUM"`
	SaveInterval     time.Duration
	DefaultTimeout   time.Duration
	PartitionBuckets int    `mapstructure:"PARTITION_BUCKETS"`
	PartitionCount   int    `mapstructure:"PARTITION_COUNT"`
	DataPath         string `mapstructure:"DATA_PATH"`
	Hostname         string
}

type RaftConfig struct {
	DataPath         string `mapstructure:"DATA_PATH"`
	EpochTime        int    `mapstructure:"EPOCH_TIME"`
	EnableLogs       bool   `mapstructure:"ENABLE_LOGS"`
	AutoBootstrap    bool   `mapstructure:"AUTO_BOOTSTRAP"`
	BootstrapTimeout int    `mapstructure:"BOOTSTRAP_TIMEOUT"`
}

type MemberListConfig struct {
	InitMembers []string `mapstructure:"INIT_MEMBERS"`
}

type StorageConfig struct {
	DataPath string `mapstructure:"DATA_PATH"`
}

type Config struct {
	Manager    ManagerConfig    `mapstructure:"MANAGER"`
	Raft       RaftConfig       `mapstructure:"RAFT"`
	MemberList MemberListConfig `mapstructure:"MEMBERLIST"`
	Storage    StorageConfig    `mapstructure:"STORAGE"`
}

func GetConfig() Config {
	return getConfigOverride(true)
}

func GetDefaultConfig() Config {
	return getConfigOverride(false)
}

func getConfigOverride(allow_override bool) Config {
	viper.SetConfigName(filepath.Join(os.Getenv("CONFIG_PATH"), "default-config"))

	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		logrus.Fatalf("Error reading default config file, %s", err)
	}
	// Override with configuration from config.yaml
	if allow_override {
		viper.SetConfigName(filepath.Join(os.Getenv("CONFIG_PATH"), "config"))
		if err := viper.MergeInConfig(); err != nil {
			logrus.Errorf("Error reading user defined config file, %s", err)
		}
	}

	var config Config
	err := viper.Unmarshal(&config)
	if err != nil {
		logrus.Fatalf("unable to decode into struct, %v", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		logrus.Fatal(err)
	}
	config.Manager.Hostname = hostname

	return config
}
