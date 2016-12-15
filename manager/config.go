package manager

import (
	"time"
)

const (
	RETRY_INTERVAL                  = 1 * time.Second
	NUM_CONNECTION_RETRIES          = 100
	WAIT_PARTICIPANT_STARTUP_PERIOD = 10 * time.Second

	NUM_HEARTBEAT_RETRIES = 10
	HEARTBEAT_INTERVAL    = 60 * time.Second

	DOMAIN = "localhost"
)
