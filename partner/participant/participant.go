package main

import (
	"flag"
	"github.com/potay/partner/participant"
	"github.com/potay/partner/server/socketio"
	"log"
	"strings"
)

var (
	ports      = flag.String("ports", "", "hostports for all paxos nodes")
	myport     = flag.String("myport", "", "hostport this paxos node should listen to")
	numNodes   = flag.Int("N", 1, "the number of nodes in the ring")
	nodeID     = flag.Int("id", 0, "node ID must match index of this node's port in the ports list")
	numRetries = flag.Int("retries", 5, "number of times a node should retry dialing another node")
	replace    = flag.Bool("replace", false, "if this node is a replacement node")
)

func init() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
}

func main() {
	flag.Parse()

	portStrings := strings.Split(*ports, ",")

	hostMap := make(map[int]string)
	for i, hostport := range portStrings {
		hostMap[i] = hostport
	}

	// Create and start the Participant.
	if *replace {
		log.Printf("Creating replacement participant %d at port %s", *nodeID, *myport)
	} else {
		log.Printf("Creating new participant at port %s", *myport)
	}
	_, err := participant.NewParticipant(*myport, hostMap, *numNodes, *nodeID, *numRetries, *replace, socketio.NewSocketIOServer)
	if err != nil {
		log.Fatalln("Failed to create participant:", err)
	}
	log.Printf("Created new participant at port %s", *myport)

	// Run the participant forever.
	select {}
}
