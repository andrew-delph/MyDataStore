package main

import (
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
)

// Define a global cache variable
var storeCache = cache.New(0*time.Minute, 10*time.Minute)

// Function to set a value in the global cache
func setValue(key string, value string) {
	storeCache.Set(key, value, 0)
}

// Function to get a value from the global cache
func getValue(key string) (string, bool) {
	if value, found := storeCache.Get(key); found {
		if strValue, ok := value.(string); ok {
			return strValue, true
		}
	}
	return "", false
}

func initCache() {
	// Ticker to trigger saveCacheToFile every 5 minutes
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	// Goroutine to periodically save the cache to a file
	go func() {
		for range ticker.C {
			logrus.Info("saving cache to file")
			storeCache.SaveFile("cache.json")
		}
	}()
}
