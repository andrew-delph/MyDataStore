package config

import (
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

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

type ConsensusConfig struct {
	DataPath         string `mapstructure:"DATA_PATH"`
	EpochTime        int    `mapstructure:"EPOCH_TIME"`
	EnableLogs       bool   `mapstructure:"ENABLE_LOGS"`
	AutoBootstrap    bool   `mapstructure:"AUTO_BOOTSTRAP"`
	BootstrapTimeout int    `mapstructure:"BOOTSTRAP_TIMEOUT"`
	Name             string
}

type GossipConfig struct {
	InitMembers []string `mapstructure:"INIT_MEMBERS"`
	Name        string
}

type StorageConfig struct {
	DataPath string `mapstructure:"DATA_PATH"`
}

type HashringConfig struct{}

type Config struct {
	Manager   ManagerConfig   `mapstructure:"MANAGER"`
	Consensus ConsensusConfig `mapstructure:"CONSENSUS"`
	Gossip    GossipConfig    `mapstructure:"GOSSIP"`
	Storage   StorageConfig   `mapstructure:"STORAGE"`
	Hashring  HashringConfig  `mapstructure:"HASHRING"`
}

func GetConfig() Config {
	return getConfigOverride(true)
}

func GetDefaultConfig() Config {
	return getConfigOverride(false)
}

func getConfigOverride(allow_override bool) Config {
	viper.AddConfigPath(os.Getenv("CONFIG_PATH"))

	viper.SetConfigName("default-config")
	if err := viper.ReadInConfig(); err != nil {
		logrus.Fatalf("Error reading default config file, %s", err)
	}
	// Override with configuration from config.yaml
	if allow_override {
		viper.SetConfigName("config")
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
	config.Gossip.Name = hostname
	config.Consensus.Name = hostname

	return config
}
