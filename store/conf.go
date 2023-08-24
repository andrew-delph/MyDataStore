package main

import (
	"io"
	"log"
	"os"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/sirupsen/logrus"
)

var (
	N                int           = 3
	W                int           = 2
	R                int           = 3
	saveInterval     time.Duration = 30 * time.Second
	defaultTimeout   time.Duration = 2 * time.Second
	partitionBuckets int           = 500
	partitionCount   int           = 500
	epochTime        time.Duration = 20 * time.Second
	dataPath         string        = "/store"
	raftLogs         bool          = false
	autoBootStrap    bool          = true
)

func GetConf() (*memberlist.Config, *MyDelegate, *MyEventDelegate) {
	delegate := GetMyDelegate()
	events := GetMyEventDelegate()

	dataPathValue, exists := os.LookupEnv("DATA_PATH")
	if exists {
		dataPath = dataPathValue
	}

	logrus.Infof("dataPath = %s", dataPath)

	// files, err := os.ReadDir(dataPath)
	// if err != nil {
	// 	logrus.Fatalf("Failed reading directory: %s", err)
	// }
	// for _, file := range files {
	// 	logrus.Warnf("File/Dir: %s", file.Name())
	// }

	conf := memberlist.DefaultLocalConfig()
	conf.Logger = log.New(io.Discard, "", 0)
	conf.BindPort = 8081
	conf.AdvertisePort = 8081
	conf.Delegate = delegate
	conf.Events = events

	localIp, err := getLocalIP()
	if err != nil {
		logrus.Fatal(err)
	}
	conf.Name = localIp

	return conf, delegate, events
}
