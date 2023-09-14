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

	logrus.SetFormatter(&logrus.TextFormatter{
		DisableTimestamp: true,
	})

	c := config.GetConfig()
	manager := NewManager(c)
	manager.StartManager()
}
