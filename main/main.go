package main

import "github.com/sirupsen/logrus"

func main() {
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		logrus.Warnf("Recovered from panic: %+v", r)
	// 	}
	// }()
	logrus.Info("starting")
	manager := NewManager()
	manager.StartManager()
}
