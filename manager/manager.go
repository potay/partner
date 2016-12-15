package manager

import (
	"encoding/json"
	"fmt"
	"github.com/potay/partner/rpc/participantrpc"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

type Participant struct {
	ID       int
	HostPort string
	Client   *rpc.Client
	mutex    sync.RWMutex
}

// Create and start participant binary
func NewParticipant(binaryPath string, id int, hostports map[int]string, replace bool) (*Participant, error) {
	participant := &Participant{
		ID:       id,
		HostPort: hostports[id],
	}

	allHostports := make([]string, len(hostports))
	for id, hostport := range hostports {
		allHostports[id] = hostport
	}
	hostportString := strings.Join(allHostports, ",")

	args := []string{
		fmt.Sprintf("-myport=%s", hostports[id]),
		fmt.Sprintf("-ports=%s", hostportString),
		fmt.Sprintf("-N=%d", len(hostports)),
		fmt.Sprintf("-id=%d", id),
		fmt.Sprintf("-retries=%d", NUM_CONNECTION_RETRIES),
	}
	if replace {
		args = append(args, "-replace")
	}
	cmd := exec.Command(binaryPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		log.Print(err)
		return nil, err
	}
	log.Printf("Started Participant %d on hostport %s", id, participant.HostPort)

	return participant, nil
}

// Connect to participant
func (p *Participant) Connect(numRetries int) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// attempt to connect with participant
	var err error
	for i := 0; i < numRetries; i++ {
		// attempt to connect to node
		p.Client, err = rpc.DialHTTP("tcp", p.HostPort)
		if err != nil {
			// wait for one second before looping again
			timer := time.NewTimer(RETRY_INTERVAL)
			<-timer.C
			continue
		} else {
			break
		}
	}

	if p.Client == nil {
		finalErr := fmt.Errorf("Cannot connect to participant %d at %s: %s", p.ID, p.HostPort, err)
		log.Print(finalErr)
		return finalErr
	}

	log.Printf("Connected to participant %d at %s", p.ID, p.HostPort)

	return nil
}

// Checks if participant is alive
func (p *Participant) SendHeartbeat() error {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	var args participantrpc.HeartbeatArgs
	var reply participantrpc.HeartbeatReply
	if p.Client == nil {
		log.Print("Not connected to participant.")
		return nil
	}
	if err := p.Client.Call("Participant.Heartbeat", &args, &reply); err != nil {
		return err
	}
	return nil
}

// Manages participants
// Replaces dead participants
// Provides hostports for clients
type Manager struct {
	BinaryPath   string
	Participants map[int]*Participant
	HostPorts    map[int]string
	mutex        sync.RWMutex
}

// Creates a new manager and starts all the participants
func NewManager(numParticipants int, hostport string, binaryPath string) (*Manager, error) {
	manager := &Manager{
		BinaryPath:   binaryPath,
		Participants: make(map[int]*Participant),
		HostPorts:    make(map[int]string),
	}

	if numParticipants <= 0 {
		return nil, fmt.Errorf("Too few participants.")
	}

	// Generate hostports for participants
	for id := 0; id < numParticipants; id++ {
		hostport := manager.generateHostPort()
		manager.HostPorts[id] = hostport
	}

	// Start participants
	for id, _ := range manager.HostPorts {
		if participant, err := NewParticipant(binaryPath, id, manager.HostPorts, false); err != nil {
			return nil, err
		} else {
			manager.Participants[id] = participant
		}
	}

	// Wait for them to finish starting up
	timer := time.NewTimer(WAIT_PARTICIPANT_STARTUP_PERIOD)
	<-timer.C

	// Connect to participants
	log.Print("Connecting to participants...")
	for _, participant := range manager.Participants {
		if err := participant.Connect(NUM_CONNECTION_RETRIES); err != nil {
			return nil, err
		}
	}

	// Start goroutine to regularly send out heartbeats
	go func(manager *Manager) {
		for {
			log.Print("Sending heartbeats...")
			if err := manager.RequestHeartbeats(); err != nil {
				log.Print(err)
			}
			timer := time.NewTimer(HEARTBEAT_INTERVAL)
			<-timer.C
		}
	}(manager)

	// Create http endpoint for hostport requests
	log.Printf("Starting HTTP Server at hostport %s...", hostport)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		origin := r.Header.Get("Origin")
		w.Header().Set("Access-Control-Allow-Origin", origin)

		type data struct {
			Hostport string `json:"hostport"`
		}
		d := data{
			Hostport: manager.GetRandomParticipantHostPort(),
		}
		json.NewEncoder(w).Encode(d)
	})

	log.Fatal(http.ListenAndServe(hostport, nil))

	return manager, nil
}

func (m *Manager) generateHostPort() string {
	rand.Seed(time.Now().UnixNano())
	port := 10000 + rand.Intn(10000)
	return fmt.Sprintf("%s:%d", DOMAIN, port)
}

// Requests heartbeats from participants
func (m *Manager) RequestHeartbeats() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	type statusReply struct {
		id    int
		alive bool
	}
	statusCh := make(chan statusReply)

	// Ping all participants
	for id, participant := range m.Participants {
		go func(id int, p *Participant, statusCh chan statusReply) {
			for i := 0; i < NUM_HEARTBEAT_RETRIES; i++ {
				if err := p.SendHeartbeat(); err == nil {
					// Participant is alive
					statusCh <- statusReply{
						id:    id,
						alive: true,
					}
					return
				}
			}
			// Participant is dead
			statusCh <- statusReply{
				id:    id,
				alive: false,
			}
			return
		}(id, participant, statusCh)
	}

	// Get list of dead participant IDs
	var deadParticipants []int
	for i := 0; i < len(m.Participants); i++ {
		status := <-statusCh
		if !status.alive {
			deadParticipants = append(deadParticipants, status.id)
		}
	}

	// Generate new ports
	for _, id := range deadParticipants {
		hostport := m.generateHostPort()
		m.HostPorts[id] = hostport
	}

	// Create replacement participants
	for _, id := range deadParticipants {
		log.Printf("Could not hear Pariticipant %d heartbeat...restarting participant at %s", id, m.HostPorts[id])
		if participant, err := NewParticipant(m.BinaryPath, id, m.HostPorts, true); err != nil {
			return err
		} else {
			m.Participants[id] = participant
			go func() {
				timer := time.NewTimer(WAIT_PARTICIPANT_STARTUP_PERIOD)
				<-timer.C
				participant.Connect(NUM_CONNECTION_RETRIES)
			}()
		}
	}

	return nil
}

// Returns a hostport that belongs to a random participant
func (m *Manager) GetRandomParticipantHostPort() string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	rand.Seed(time.Now().UnixNano())
	return m.HostPorts[rand.Intn(len(m.HostPorts))]
}
