package main

import (
	"crypto/sha1"
	"encoding/hex"
	"sort"
)

func hashKey(key string) string {
	hasher := sha1.New()
	hasher.Write([]byte(key))
	return hex.EncodeToString(hasher.Sum(nil))
}

func getNodeForKey(key string) string {
	// Create a sorted slice of hashed node keys
	var hashedNodes []string
	for node := range nodeData {
		hashedNodes = append(hashedNodes, hashKey(node))
	}
	sort.Strings(hashedNodes)

	// Hash the input key
	hashedKey := hashKey(key)

	// Find the smallest hashed node key that is greater than or equal to the hashed input key
	for _, hashedNode := range hashedNodes {
		if hashedNode >= hashedKey {
			// Return the corresponding node data
			for node, _ := range nodeData {
				if hashKey(node) == hashedNode {
					return nodeData[node]
				}
			}
		}
	}

	// If no such hashed node key is found, wrap around to the first node
	if len(hashedNodes) > 0 {
		for node, _ := range nodeData {
			if hashKey(node) == hashedNodes[0] {
				return nodeData[node]
			}
		}
	}

	return "" // Return an empty string or an error if no suitable node is found
}
