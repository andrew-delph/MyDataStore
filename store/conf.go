package main

import (
	"io"
	"log"

	"github.com/hashicorp/memberlist"
)

func GetConf() (*memberlist.Config, *MyDelegate, *MyEventDelegate) {

	delegate := GetMyDelegate()
	events := GetMyEventDelegate()

	conf := memberlist.DefaultLocalConfig()
	conf.Logger = log.New(io.Discard, "", 0)
	conf.BindPort = 8080
	conf.AdvertisePort = 8080
	conf.Delegate = delegate
	conf.Events = events

	return conf, delegate, events
}
