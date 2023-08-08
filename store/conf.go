package main

import (
	"io"
	"log"

	"github.com/hashicorp/memberlist"
)

var (
	totalReplicas int
	writeResponse int
	readResponse  int
)

func GetConf() (*memberlist.Config, *MyDelegate, *MyEventDelegate) {

	totalReplicas = 4
	writeResponse = 3
	readResponse = 1

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
