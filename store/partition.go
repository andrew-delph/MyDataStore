package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"

	mapset "github.com/deckarep/golang-set/v2"
)

func hashKey(key string) string {
	hasher := sha1.New()
	hasher.Write([]byte(key))
	return hex.EncodeToString(hasher.Sum(nil))
}

type NodeHash struct {
	Name string
	Hash string
}

func hashRingGetN(key string, n int) ([]string, error) {
	// Create a sorted slice of hashed node keys
	var hashRing []NodeHash
	for node := range nodeData {
		hashRing = append(hashRing, NodeHash{Name: node, Hash: hashKey(node)})
	}

	sort.Slice(hashRing, func(i, j int) bool {
		return hashRing[i].Hash < hashRing[j].Hash
	})

	if len(hashRing) < n {
		nodes := make([]string, 0, len(nodeData))
		for k := range nodeData {
			nodes = append(nodes, k)
		}
		return nodes, fmt.Errorf("not enough nodes")
	}

	hashedKey := hashKey(key)

	// if the hashedKey is greater than the last value. return the first n
	if hashedKey > hashRing[len(hashRing)-1].Hash {
		var firstN []string
		for _, node := range hashRing[:min(n+1, len(hashRing)+1)] {
			firstN = append(firstN, node.Name)
		}
		return firstN, nil
	}

	// double the length to make a circle
	hashRing = append(hashRing, hashRing...)

	replicateSet := mapset.NewSet[string]()

	for i, hashedNode := range hashRing {
		if hashedNode.Hash >= hashedKey {
			for _, hashedNode := range hashRing[i:] {
				replicateSet.Add(hashedNode.Name)
				if replicateSet.Cardinality() >= n {
					return replicateSet.ToSlice(), nil
				}
			}
		}
	}

	return replicateSet.ToSlice(), fmt.Errorf("did not find all nodes")
}
