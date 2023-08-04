package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"crypto/sha1"
	"encoding/hex"
	"sort"

	"github.com/go-zookeeper/zk"
)

// Global map
var myMap = map[string]string{}

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

func addValue(key, value string) {
	myMap[key] = value
}

// HTTP handler for adding a value
func addHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	addValue(key, value)
	node := getNodeForKey(key)

	log.Printf("Added %s: %s to the map on node %s\n", key, value, node)

	fmt.Fprintf(w, "Added %s: %s to the map on node %s\n", key, value, node)
}

func getValue(key string) string {
	return myMap[key]
}

// HTTP handler for getting a value
func getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := getValue(key)
	fmt.Fprintf(w, "Value for %s: %s", key, value)
}

func listValues() map[string]string {
	return myMap
}

// HTTP handler for listing all values
func listHandler(w http.ResponseWriter, r *http.Request) {
	values := listValues()
	json.NewEncoder(w).Encode(values)
}

func nodesHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(nodeData)
}

func baseHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Server ID: %s\n", serverIP)
}

var nodeData = make(map[string]string)

var serverIP string

func watchNodes(conn *zk.Conn, path string) {
	for {
		children, _, watchChannel, err := conn.ChildrenW(path)
		if err != nil {
			log.Fatalf("Failed to list children for path %s: %v", path, err)
			return
		}

		// Temporary map to track current children
		currentChildren := make(map[string]bool)

		for _, child := range children {
			childPath := path + "/" + child
			data, _, err := conn.Get(childPath)
			if err != nil {
				log.Printf("Failed to get data for child %s: %v", childPath, err)
				continue
			}

			// Store child data in the global map
			nodeData[child] = string(data)
			currentChildren[child] = true
		}

		// Remove data for nodes that no longer exist
		for key := range nodeData {
			if !currentChildren[key] {
				delete(nodeData, key)
			}
		}

		// Wait for changes in the node
		<-watchChannel
	}
}

func getIPAddress() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, address := range addrs {
		// check the address type and if it is not a loopback, return the IP
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}

	return "", fmt.Errorf("No IPv4 address found")
}

func main() {
	log.Println("STARTING STARTING STARTING STARTING STARTING")

	servers := []string{"zookeeper:2181"}
	conn, _, err := zk.Connect(servers, time.Second)
	if err != nil {
		log.Fatalf("Unable to connect to ZooKeeper: %v", err)
	}
	defer conn.Close()

	groupPath := "/key-store"

	// Register the client to the group
	clientPath := groupPath + "/client-"

	serverIP, err := getIPAddress()
	if err != nil {
		log.Fatalf("Get Ip address error: %v", err)
	}

	data := []byte(serverIP)
	_, err = conn.CreateProtectedEphemeralSequential(clientPath, data, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Fatalf("Unable to register client to the group: %v", err)
	}

	log.Println("Successfully registered to the key-store group!")

	// Watch for changes in the group
	go watchNodes(conn, groupPath)

	fmt.Println("SessionID:", conn.State().String())

	// // Get the server ID from ZooKeeper
	// serverIDPath := "/server/id"
	// serverIDBytes, _, err := conn.Get(serverIDPath)
	// if err != nil {
	// 	log.Fatalf("Failed to get server ID from ZooKeeper: %v", err)
	// }
	// serverID = string(serverIDBytes)

	http.HandleFunc("/", baseHandler)

	http.HandleFunc("/add", addHandler)
	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/list", listHandler)
	http.HandleFunc("/nodes", nodesHandler)

	fmt.Println("Server is running on port 8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}
