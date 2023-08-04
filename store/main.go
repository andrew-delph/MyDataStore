package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"time"

	"github.com/go-zookeeper/zk"
)

// Global map
var myMap = map[string]string{}

func addValue(key, value string) {
	myMap[key] = value
}

// HTTP handler for adding a value
func addHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	addValue(key, value)
	fmt.Fprintf(w, "Added %s: %s to the map", key, value)
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

var serverID string

func baseHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Server ID: %s\n", serverID)
}

// func main() {

// 	rand.Seed(time.Now().UnixNano())
// 	serverID = fmt.Sprintf("Server-%d", rand.Int())

// 	http.HandleFunc("/", baseHandler)

// 	http.HandleFunc("/add", addHandler)
// 	http.HandleFunc("/get", getHandler)
// 	http.HandleFunc("/list", listHandler)

// 	fmt.Println("Server is running on port 8080...")
// 	if err := http.ListenAndServe(":8080", nil); err != nil {
// 		panic(err)
// 	}
// }

func watchNodes(conn *zk.Conn, path string) {
	for {
		children, _, watchChannel, err := conn.ChildrenW(path)
		if err != nil {
			log.Fatalf("Failed to list children for path %s: %v", path, err)
			return
		}

		for _, child := range children {
			childPath := path + "/" + child
			data, _, err := conn.Get(childPath)
			if err != nil {
				log.Printf("Failed to get data for child %s: %v", childPath, err)
				continue
			}
			fmt.Printf("Node: %s, Data: %s\n", child, string(data))
		}

		// Wait for changes in the node
		<-watchChannel
		log.Println("Node membership has changed!")
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

	myIp, err := getIPAddress()
	if err != nil {
		log.Fatalf("Get Ip address error: %v", err)
	}

	data := []byte(myIp)
	_, err = conn.CreateProtectedEphemeralSequential(clientPath, data, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Fatalf("Unable to register client to the group: %v", err)
	}

	log.Println("Successfully registered to the key-store group!")

	// Watch for changes in the group
	go watchNodes(conn, groupPath)

	// Start the http server
	rand.Seed(time.Now().UnixNano())
	serverID = fmt.Sprintf("Server-%d", rand.Int())

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

	fmt.Println("Server is running on port 8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}
