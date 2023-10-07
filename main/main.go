package main

import (
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/config"
)

func main() {
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		logrus.Warnf("Recovered from panic: %+v", r)
	// 	}
	// }()
	logrus.Info("starting")

	// logrus.SetFormatter(&logrus.TextFormatter{
	// 	DisableTimestamp: true,
	// })

	c := config.GetConfig()
	initMetrics(c.Manager.Hostname)

	logrus.Warnf("OPERATOR MODE: %v", c.Manager.Operator)

	manager := NewManager(c)
	manager.StartManager()
}
