package main

import (
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/sirupsen/logrus"
)

func StartProfileServer() {
	if os.Getenv("PROFILE_SERVER") == "true" {
		logrus.Warn("STARTING PROFILE SERVER")
		go func() {
			http.ListenAndServe(":6060", nil)
		}()
	}
}
