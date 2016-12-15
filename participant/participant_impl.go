package participant

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/potay/partner/ot"
	"github.com/potay/partner/paxos"
	"github.com/potay/partner/rpc/participantrpc"
	"github.com/potay/partner/rpc/paxosrpc"
	"github.com/rs/xid"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

type participant struct {
	id              int
	hostPort        string
	hostMap         map[int]string
	numParticipants int
	listener        net.Listener
	paxosNode       paxos.PaxosNode
	Server          ParticipantServer
	clients         map[xid.ID]ParticipantClient
	clientMutex     sync.Mutex
}

func NewParticipant(myHostPort string, hostMap map[int]string, numParticipants, myID, numRetries int, replace bool, serverConstructor func(p Participant) ParticipantServer) (Participant, error) {
	p := new(participant)
	p.id = myID
	p.hostPort = myHostPort
	p.hostMap = hostMap
	p.numParticipants = numParticipants
	p.clients = make(map[xid.ID]ParticipantClient)

	// Create the server socket that will listen for incoming RPCs
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}
	p.listener = listener

	// handle RPC in separate routine
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	paxosNode, err := paxos.NewPaxosNode(myHostPort, hostMap, numParticipants, myID, numRetries, replace, listener, myHostPort)
	if err != nil {
		return nil, err
	}
	p.paxosNode = paxosNode

	p.Server = serverConstructor(p)

	// Wrap the participant before registering it for RPC.
	err = rpc.RegisterName("PaxosCallbacks", paxosrpc.WrapCallbacks(p))
	if err != nil {
		return nil, err
	}

	// Wrap the participant before registering it for RPC.
	err = rpc.RegisterName("Participant", participantrpc.Wrap(p))
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *participant) RegisterClient(client ParticipantClient) (xid.ID, error) {
	log.Print("Registering a new client")

	p.clientMutex.Lock()
	if len(p.clients) >= MAX_CLIENTS {
		p.clientMutex.Unlock()
		return xid.New(), fmt.Errorf("Server has reached max capacity.")
	}

	clientID := xid.New()
	client.ID = clientID
	p.clients[clientID] = client
	p.clientMutex.Unlock()

	log.Print("Registered client as ", clientID)

	p.UpdateClient(clientID, client)

	return clientID, nil
}

func (p *participant) UnregisterClient(clientID xid.ID) error {
	p.clientMutex.Lock()
	if _, ok := p.clients[clientID]; !ok {
		p.clientMutex.Unlock()
		return fmt.Errorf("ClientID is not registered")
	}
	p.clientMutex.Unlock()

	log.Print("Unregistering a client ", clientID)

	if err := p.RemoveClient(clientID); err != nil {
		return err
	}

	p.clientMutex.Lock()
	delete(p.clients, clientID)
	p.clientMutex.Unlock()

	return nil
}

func (p *participant) AddDocChange(clientID xid.ID, docID string, op ot.Operation) error {
	p.clientMutex.Lock()
	if _, ok := p.clients[clientID]; !ok {
		p.clientMutex.Unlock()
		return fmt.Errorf("ClientID is not registered")
	}
	p.clientMutex.Unlock()

	key := getDocumentKey(docID)
	value, err := op.Marshal()

	if err != nil {
		return err
	}

	for {
		// Get Proposal Number
		pnumArgs := &paxosrpc.ListProposalNumberArgs{Key: key}
		var pnumReply paxosrpc.ListProposalNumberReply
		if err := p.paxosNode.GetNextListProposalNumber(pnumArgs, &pnumReply); err != nil {
			log.Println(err)
			continue
		}

		// Send proposal
		propArgs := &paxosrpc.ProposeAppendToListArgs{N: pnumReply.N, Key: key, V: value}
		var propReply paxosrpc.ProposeAppendToListReply
		if err := p.paxosNode.ProposeAppendToList(propArgs, &propReply); err != nil {
			log.Println(err)
			continue
		}

		var other ot.Operation
		if err := other.Unmarshal(propReply.V.([]byte)); err != nil {
			return err
		}

		if !op.Equal(other) {
			op, _, err = op.Transform(other)
			if err != nil {
				return err
			}

			value, err = op.Marshal()
			if err != nil {
				return err
			}
		} else {
			break
		}
	}

	return nil
}

func (p *participant) UpdateClient(clientID xid.ID, client ParticipantClient) error {
	p.clientMutex.Lock()
	if _, ok := p.clients[clientID]; !ok {
		p.clientMutex.Unlock()
		return fmt.Errorf("ClientID is not registered")
	}
	p.clientMutex.Unlock()

	if client.ID != clientID {
		return fmt.Errorf("ClientIDs do not match")
	}

	key := getClientKey(clientID)
	value, err := json.Marshal(client)

	if err != nil {
		return err
	}

	for {
		// Get Proposal Number
		pnumArgs := &paxosrpc.ProposalNumberArgs{Key: key}
		var pnumReply paxosrpc.ProposalNumberReply
		if err := p.paxosNode.GetNextProposalNumber(pnumArgs, &pnumReply); err != nil {
			log.Println(err)
			continue
		}

		// Send proposal
		propArgs := &paxosrpc.ProposeArgs{N: pnumReply.N, Key: key, V: value}
		var propReply paxosrpc.ProposeReply
		if err := p.paxosNode.Propose(propArgs, &propReply); err != nil {
			log.Println(err)
			continue
		}

		if bytes.Compare(value, propReply.V.([]byte)) == 0 {
			break
		}
	}

	return nil
}

func (p *participant) RemoveClient(clientID xid.ID) error {
	p.clientMutex.Lock()
	if _, ok := p.clients[clientID]; !ok {
		p.clientMutex.Unlock()
		return fmt.Errorf("ClientID is not registered")
	}
	p.clientMutex.Unlock()

	key := getClientKey(clientID)
	var value interface{} = nil

	for {
		// Get Proposal Number
		pnumArgs := &paxosrpc.ProposalNumberArgs{Key: key}
		var pnumReply paxosrpc.ProposalNumberReply
		if err := p.paxosNode.GetNextProposalNumber(pnumArgs, &pnumReply); err != nil {
			log.Println(err)
			continue
		}

		// Send proposal
		propArgs := &paxosrpc.ProposeArgs{N: pnumReply.N, Key: key, V: value}
		var propReply paxosrpc.ProposeReply
		if err := p.paxosNode.Propose(propArgs, &propReply); err != nil {
			log.Println(err)
			continue
		}

		if propReply.V == nil {
			break
		}
	}

	return nil
}

func (p *participant) DocCatchUp(clientID xid.ID, docID string) ([]ot.Operation, error) {
	p.clientMutex.Lock()
	if _, ok := p.clients[clientID]; !ok {
		p.clientMutex.Unlock()
		return nil, fmt.Errorf("ClientID is not registered")
	}
	p.clientMutex.Unlock()

	key := getDocumentKey(docID)

	// Get Document Operations
	args := &paxosrpc.GetListArgs{Key: key}
	var reply paxosrpc.GetListReply
	if err := p.paxosNode.GetList(args, &reply); err != nil {
		return nil, err
	}

	// Change the types
	changes := make([]ot.Operation, len(reply.L))
	for i, value := range reply.L {
		var op ot.Operation
		op.Unmarshal(value.([]byte))
		changes[i] = op
	}

	return changes, nil
}

// Handles notifications regarding new commits in the paxos ring
func (p *participant) NotifyNewCommit(args *paxosrpc.NotifyNewCommitArgs, reply *paxosrpc.NotifyNewCommitReply) error {
	switch args.DataType {
	case paxosrpc.VALUE:
		clientID, err := getClientIDFromKey(args.Key)
		if err != nil {
			return err
		}

		if args.V != nil {
			// New client update and so we broadcast it out to the clients
			var client ParticipantClient
			err := json.Unmarshal(args.V.([]byte), &client)
			if err != nil {
				return err
			}
			if err := p.Server.BroadcastClientUpdate(clientID, client); err != nil {
				log.Println(err)
			}
		} else {
			// Client was removed and so we broadcast that to other clients
			if err := p.Server.BroadcastClientRemove(clientID); err != nil {
				log.Println(err)
			}
		}

	case paxosrpc.LIST:
		// New operation done for document so we broadcast it out to the clients
		var op ot.Operation
		op.Unmarshal(args.V.([]byte))
		docID, err := getDocIDFromKey(args.Key)
		if err != nil {
			return err
		}
		if err := p.Server.BroadcastDocChange(docID, op); err != nil {
			log.Println(err)
		}
	}
	return nil
}

func (p *participant) Heartbeat(*participantrpc.HeartbeatArgs, *participantrpc.HeartbeatReply) error {
	return nil
}
