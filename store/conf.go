package main

import (
	"io"
	"log"
	"time"

	"github.com/hashicorp/memberlist"
)

var (
	totalReplicas  int           = 1
	writeResponse  int           = 1
	readResponse   int           = 1
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
	conf.Name = hostname

	return conf, delegate, events
}
