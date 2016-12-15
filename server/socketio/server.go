package socketio

import (
	"encoding/json"
	"fmt"
	"github.com/googollee/go-socket.io"
	"github.com/potay/partner/ot"
	"github.com/potay/partner/participant"
	"github.com/rs/xid"
	"log"
	"net/http"
	"strings"
	"sync"
)

// Socket.io server. Used to communicate with the participant
// Implements the ParticipantServer interface
type SocketIOServer struct {
	Server             *socketio.Server
	RemoteClients      map[xid.ID]participant.ParticipantClient // Clients that have been broadcasted
	remoteClientsMutex sync.Mutex
	Clients            map[xid.ID]*SocketIOClient
	clientMutex        sync.Mutex
}

// Returns a new socket.io server
func NewSocketIOServer(p participant.Participant) participant.ParticipantServer {
	sserver := &SocketIOServer{
		RemoteClients: make(map[xid.ID]participant.ParticipantClient),
		Clients:       make(map[xid.ID]*SocketIOClient),
	}

	// Create the server
	server, err := socketio.NewServer(nil)
	if err != nil {
		log.Fatal(err)
	}

	// Handle new connections
	server.On("connection", func(so socketio.Socket) {
		log.Println("New client connection")

		// Create client
		client, err := NewSocketIOClient(p, sserver, so)
		if err != nil {
			log.Print(err)
		} else {
			sserver.clientMutex.Lock()
			sserver.Clients[client.Client.ID] = client
			sserver.clientMutex.Unlock()
		}
	})

	// Handle server errors
	server.On("error", func(so socketio.Socket, err error) {
		log.Println("error:", err)
	})

	sserver.Server = server

	http.Handle("/socket.io/", sserver)

	return sserver
}

func (s *SocketIOServer) RemoveClient(clientID xid.ID) error {
	s.clientMutex.Lock()
	delete(s.Clients, clientID)
	s.clientMutex.Unlock()
	return nil
}

func (s *SocketIOServer) GetAllClientsInDoc(docID string) ([]participant.ParticipantClient, error) {
	var clients []participant.ParticipantClient
	s.remoteClientsMutex.Lock()
	for _, client := range s.RemoteClients {
		if client.DocID == docID {
			clients = append(clients, client)
		}
	}
	s.remoteClientsMutex.Unlock()

	return clients, nil
}

func (s *SocketIOServer) BroadcastDocChange(docID string, op ot.Operation) error {
	var errs []string
	s.clientMutex.Lock()
	for _, client := range s.Clients {
		if client.GetClient().DocID == docID {
			if err := client.ReceiveDocChange(op); err != nil {
				errs = append(errs, err.Error())
			}
		}
	}
	s.clientMutex.Unlock()
	if len(errs) > 0 {
		return fmt.Errorf(strings.Join(errs, "\n"))
	}
	return nil
}

func (s *SocketIOServer) BroadcastClientUpdate(clientID xid.ID, client participant.ParticipantClient) error {
	clientJSON, err := json.Marshal(client)
	if err != nil {
		return err
	}
	s.remoteClientsMutex.Lock()
	s.RemoteClients[clientID] = client
	s.remoteClientsMutex.Unlock()

	s.Server.BroadcastTo(client.DocID, "user update", string(clientJSON))
	return nil
}

func (s *SocketIOServer) BroadcastClientRemove(clientID xid.ID) error {
	s.remoteClientsMutex.Lock()
	client, ok := s.RemoteClients[clientID]
	if !ok {
		s.remoteClientsMutex.Unlock()
		return nil
	}
	delete(s.RemoteClients, clientID)
	s.remoteClientsMutex.Unlock()
	s.Server.BroadcastTo(client.DocID, "user remove", clientID)
	return nil
}

// Header handling, this is necessary to adjust security and/or header settings in general
// Access-Control-Allow-Origin will be set to whoever will call the server
func (s *SocketIOServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	origin := r.Header.Get("Origin")
	w.Header().Set("Access-Control-Allow-Origin", origin)
	s.Server.ServeHTTP(w, r)
}
