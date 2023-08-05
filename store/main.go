package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
)

var myMap sync.Map

func set(key string, value string) {
	myMap.Store(key, value)
}

func get(key string) (string, bool) {
	if value, ok := myMap.Load(key); ok {
		return value.(string), true
	}
	return "", false
}

var nodeData = make(map[string]string)

var serverIP string

func addValue(key, value string) {
	set(key, value)
}

func addRequest(ip, key, value string) {
	url := fmt.Sprintf("http://%s:8080/add?key=%s&value=%s&local=true", ip, key, value)

	// fmt.Println("addRequest URL", url)

	// Creating a GET request
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Failed to make the request: %v", err)
		return
	}
	defer resp.Body.Close()

	// Reading the response body
	_, err = io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Failed to read the response body: %v", err)
		return
	}

	// // Printing the response
	// fmt.Println("HTTP Status Code:", resp.StatusCode)
	// fmt.Println("Response Body:", string(body))
}

// HTTP handler for adding a value
func addHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	local := r.URL.Query().Get("local")
	log.Printf("TO ADD %s:%s LOCAL=%s\n", key, value, local)
	if local == "true" {
		addValue(key, value)
		fmt.Fprintf(w, "Added %s: %s to the map on LOCAL", key, value)

	} else {
		node := getNodeForKey(key)

		addRequest(node, key, value)
		fmt.Fprintf(w, "Added %s: %s to the map on %s", key, value, node)
	}
}

func getRequest(ip, key string) string {
	url := fmt.Sprintf("http://%s:8080/get?key=%s&local=true", ip, key)

	// Creating a GET request
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Failed to make the request: %v", err)
		return "ERROR1"
	}
	defer resp.Body.Close()

	// Reading the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Failed to read the response body: %v", err)
		return "ERROR2"
	}

	return string(body)
}

// HTTP handler for getting a value
func getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	local := r.URL.Query().Get("local")

	if local == "true" {
		value, exists := get(key)
		if exists {
			fmt.Fprintf(w, "Value for %s: %s", key, value)

		} else {
			fmt.Fprintf(w, "Value for %s does not exist.", key)
		}

	} else {
		node := getNodeForKey(key)
		response := getRequest(node, key)
		fmt.Fprint(w, response)
	}
}

func listValues() map[string]string {
	regularMap := make(map[string]string)
	myMap.Range(func(key, value interface{}) bool {
		regularMap[key.(string)] = value.(string)
		return true
	})
	return regularMap
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

	exists, _, err := conn.Exists(groupPath)
	if err != nil {
		log.Fatalf("Failed to check if group exists: %v", err)
		return
	}

	// Create the group path if it does not exist
	if !exists {
		_, err := conn.Create(groupPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			log.Fatalf("Failed to create group: %v", err)
			return
		}
	}

	serverIP, err = getIPAddress()
	if err != nil {
		log.Fatalf("Get Ip address error: %v", err)
	}

	fmt.Println("serverIP:", serverIP)

	data := []byte(serverIP)
	_, err = conn.CreateProtectedEphemeralSequential(clientPath, data, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Fatalf("Unable to register client to the group: %v", err)
	}

	log.Println("Successfully registered to the key-store group!")

	// Watch for changes in the group
	go watchNodes(conn, groupPath)

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
