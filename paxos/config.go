package paxos

import (
	"time"
)

var PROPOSE_TIMEOUT = 15 * time.Second
var RETRY_INTERVAL = 1 * time.Second
var MAX_NODES = 20
