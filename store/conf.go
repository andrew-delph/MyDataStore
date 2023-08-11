package main

import (
	"io"
	"log"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/sirupsen/logrus"
)

var (
	totalReplicas  int           = 3
	writeResponse  int           = 2
	readResponse   int           = 3
	saveInterval   time.Duration = 30 * time.Second
	defaultTimeout time.Duration = 2 * time.Second
)

func GetConf() (*memberlist.Config, *MyDelegate, *MyEventDelegate) {
	delegate := GetMyDelegate()
	events := GetMyEventDelegate()

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
