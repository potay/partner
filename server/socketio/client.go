package socketio

import (
	"encoding/json"
	"github.com/googollee/go-socket.io"
	"github.com/potay/partner/ot"
	"github.com/potay/partner/participant"
	"log"
	"sync"
)

// Client, specific for socket.io server implementation
type SocketIOClient struct {
	So socketio.Socket         // Does not change. No protection.
	P  participant.Participant // Does not change. No protection.
	S  *SocketIOServer         // Does not change. No protection.

	Client      participant.ParticipantClient // Client that is registered with participant
	clientMutex sync.RWMutex

	Buffer      ot.Operation // Buffered Outgoing Operation
	Pending     ot.Operation // Pending Outgoing Operation
	bufferMutex sync.Mutex
}

// Returns a new client with the socket.io socket
func NewSocketIOClient(p participant.Participant, s *SocketIOServer, so socketio.Socket) (*SocketIOClient, error) {
	client := &SocketIOClient{
		So: so,
		P:  p,
		S:  s,
	}

	// Register client
	clientID, err := p.RegisterClient(client.Client)

	if err != nil {
		so.Emit("rejected", "Server is full. Please try another server.")
		return nil, err
	}

	// Set the client's ID
	client.Client.ID = clientID

	// Handle edits from client
	so.On("edit", client.handleDocEdit())

	// Handle the client updates
	so.On("user update", client.handleUserUpdate())

	// Handle the client disconnecting
	so.On("disconnection", client.handleDisconnection())

	return client, nil
}

func (c *SocketIOClient) handleDocEdit() func(string) (bool, string) {
	return func(operationJSON string) (bool, string) {
		var op ot.Operation
		err := op.Unmarshal([]byte(operationJSON))
		if err != nil {
			log.Print(err)
			return false, err.Error()
		}

		c.clientMutex.RLock()
		op.Meta.Authors = []string{c.Client.Name}
		c.clientMutex.RUnlock()

		c.bufferMutex.Lock()
		switch {
		case !c.Buffer.Empty():
			if newOp, err := c.Buffer.Compose(op); err != nil {
				c.bufferMutex.Unlock()
				return false, err.Error()
			} else {
				c.Buffer = newOp
			}
			c.bufferMutex.Unlock()
		case !c.Pending.Empty():
			c.Buffer = op
			c.bufferMutex.Unlock()
		default:
			c.Pending = op
			c.bufferMutex.Unlock()
			c.clientMutex.RLock()
			if err := c.P.AddDocChange(c.Client.ID, c.Client.DocID, op); err != nil {
				c.clientMutex.RUnlock()
				return false, err.Error()
			} else {
				c.clientMutex.RUnlock()
				c.bufferMutex.Lock()
				c.Pending = ot.Operation{}
				c.bufferMutex.Unlock()
				go c.ackCallback()
			}
		}

		return true, ""
	}
}

func (c *SocketIOClient) handleUserUpdate() func(string) (bool, string) {
	return func(clientJSON string) (bool, string) {
		var clientData participant.ParticipantClient
		err := json.Unmarshal([]byte(clientJSON), &clientData)
		if err != nil {
			log.Print(err)
			return false, err.Error()
		}

		c.clientMutex.Lock()
		// Set client ID
		clientData.ID = c.Client.ID

		if err := c.P.UpdateClient(c.Client.ID, clientData); err != nil {
			c.clientMutex.Unlock()
			return false, err.Error()
		}

		oldClient := c.Client
		c.Client = clientData
		c.clientMutex.Unlock()

		// Check if client is joining a new document
		if clientData.DocID != oldClient.DocID {
			// Leave old document socket.io room
			c.So.Leave(oldClient.DocID)
			// Join the document socket.io room
			c.So.Join(clientData.DocID)

			// Catchup to get the current document state
			if operations, err := c.P.DocCatchUp(oldClient.ID, clientData.DocID); err != nil {
				log.Println(err)
				return false, err.Error()
			} else {
				for _, op := range operations {
					if operationJSON, err := op.Marshal(); err != nil {
						return false, err.Error()
					} else {
						c.So.Emit("edit", string(operationJSON))
					}
				}
			}

			// Catchup with all users in document
			if clients, err := c.S.GetAllClientsInDoc(clientData.DocID); err != nil {
				log.Println(err)
				return false, err.Error()
			} else {
				for _, client := range clients {
					clientJSON, err := json.Marshal(client)
					if err != nil {
						log.Println(err)
					} else {
						c.So.Emit("user update", string(clientJSON))
					}
				}
			}
		}

		return true, ""
	}
}

func (c *SocketIOClient) handleDisconnection() func() {
	return func() {
		log.Println("on disconnect")
		c.clientMutex.Lock()
		c.P.UnregisterClient(c.Client.ID)
		c.S.RemoveClient(c.Client.ID)
		c.clientMutex.Unlock()
	}
}

func (c *SocketIOClient) ackCallback() {
	for {
		c.bufferMutex.Lock()
		if !c.Buffer.Empty() {
			if c.Pending.Empty() {
				c.Pending, c.Buffer = c.Buffer, ot.Operation{}
				c.bufferMutex.Unlock()
				c.clientMutex.RLock()
				client := c.Client
				c.clientMutex.RUnlock()
				if err := c.P.AddDocChange(client.ID, client.DocID, c.Pending); err != nil {
					log.Print(err)
				} else {
					c.bufferMutex.Lock()
					c.Pending = ot.Operation{}
					c.bufferMutex.Unlock()
					go c.ackCallback()
				}
			} else {
				c.bufferMutex.Unlock()
				continue
			}
		} else {
			c.bufferMutex.Unlock()
			break
		}
	}
}

func (c *SocketIOClient) ReceiveDocChange(op ot.Operation) error {
	var err error
	c.bufferMutex.Lock()
	if !c.Pending.Empty() {
		if op, c.Pending, err = op.Transform(c.Pending); err != nil {
			c.bufferMutex.Unlock()
			return err
		}
	}
	if !c.Buffer.Empty() {
		if op, c.Buffer, err = op.Transform(c.Buffer); err != nil {
			c.bufferMutex.Unlock()
			return err
		}
	}
	c.bufferMutex.Unlock()

	if operationJSON, err := op.Marshal(); err != nil {
		return err
	} else {
		c.So.Emit("edit", string(operationJSON))
	}

	return nil
}

func (c *SocketIOClient) GetClient() participant.ParticipantClient {
	c.clientMutex.RLock()
	defer c.clientMutex.RUnlock()
	return c.Client
}
