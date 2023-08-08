package main

import (
	"math/rand"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/exp/constraints"
)

func min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func randomCrash() {
	for {
		// Sleep for a random duration between 2 to 5 minutes
		sleepDuration := randomDuration(30*time.Second, 2*time.Minute)
		logrus.Warn("sleepDuration ", sleepDuration)
		time.Sleep(sleepDuration)

		// Crash by causing a logrus.Panic
		causePanic()
	}
}

func randomDuration(min, max time.Duration) time.Duration {
	randomSeconds := rand.Int63n(int64(max-min)) + int64(min)
	return time.Duration(randomSeconds)
}

func causePanic() {
	logrus.Warn("Crash imminent!")
	logrus.Panic("Oops, this is a random crash!")
}
