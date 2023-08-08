package main

import (
	"fmt"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
)

// Define a global cache variable

var storeCache *cache.Cache

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

	cacheFileName := fmt.Sprintf("/store/%s.json", hostname)

	storeCache = cache.New(0*time.Minute, 1*time.Minute)
	err := storeCache.LoadFile(cacheFileName)
	if err != nil {
		logrus.Errorf("failed to load from file: %s : %v", cacheFileName, err)
	}

	ticker := time.NewTicker(10 * time.Second)

	// Goroutine to periodically save the cache to a file
	go func() {
		for range ticker.C {
			logrus.Warnf("saving cache to file %s", cacheFileName)
			storeCache.SaveFile(cacheFileName)
		}
	}()
}
