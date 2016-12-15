package main

import (
	"flag"
	"github.com/potay/partner/manager"
	"log"
)

var (
	numNodes   = flag.Int("N", 5, "the number of nodes in the ring")
	hostport   = flag.String("hostport", "", "the hostport to listen to for the http server")
	binaryPath = flag.String("binary", "", "participant binary filepath")
)

func init() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
}

func main() {
	flag.Parse()

	// Create and start the Manager.
	_, err := manager.NewManager(*numNodes, *hostport, *binaryPath)
	if err != nil {
		log.Fatalln("Failed to create manager:", err)
	}

	// Run the manager forever.
	select {}
}
