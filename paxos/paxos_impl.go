package paxos

import (
	"errors"
	"fmt"
	"github.com/potay/partner/rpc/paxosrpc"
	"github.com/potay/partner/utils"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

type paxosNode struct {
	srvId        int
	numNodes     int
	quorum       *paxosQuorum
	hostPort     string
	storage      *paxosStorage
	numRetries   int
	notify       bool
	notifyClient *rpc.Client
}

// Desc:
// NewPaxosNode creates a new PaxosNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes numRetries times.
//
// Params:
// myHostPort: the hostport string of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than hostMap[srvId]
// hostMap: a map from all node IDs to their hostports.
//				Note: Please connect to hostMap[srvId] rather than myHostPort
//				when this node try to make rpc call to itself.
// numNodes: the number of nodes in the ring
// numRetries: if we can't connect with some nodes in hostMap after numRetries attempts, an error should be returned
// replace: a flag which indicates whether this node is a replacement for a node which failed.
func NewPaxosNode(myHostPort string, hostMap map[int]string, numNodes, srvId, numRetries int, replace bool, listener net.Listener, notifyHostPort string) (PaxosNode, error) {
	node := &paxosNode{
		srvId:      srvId,
		numNodes:   numNodes,
		quorum:     newPaxosQuorum(numNodes, srvId),
		hostPort:   myHostPort,
		storage:    newPaxosStorage(srvId),
		numRetries: numRetries,
		notify:     false,
	}

	// listen on port
	var err error
	if listener == nil {
		listener, err = net.Listen("tcp", node.hostPort)
		if err != nil {
			return nil, err
		}

		// handle RPC in separate routine
		rpc.HandleHTTP()
		go http.Serve(listener, nil)
	}

	if notifyHostPort != "" {
		cli, err := rpc.DialHTTP("tcp", notifyHostPort)
		if err != nil {
			return nil, fmt.Errorf("could not connect to notify node %s", notifyHostPort)
		}
		node.notify = true
		node.notifyClient = cli
	}

	// register server on RPC
	if err := rpc.RegisterName("PaxosNode", paxosrpc.Wrap(node)); err != nil {
		return nil, err
	}

	if err = node.quorum.AddAcceptors(hostMap, numRetries); err != nil {
		fmt.Println(err)
		return nil, err
	}

	if replace {
		node.quorum.SendReplaceServerToAll()
		data := node.quorum.SendReplaceCatchupToAll()
		if err = node.storage.Restore(data); err != nil {
			return nil, err
		}
	}

	return node, nil
}

// Desc:
// GetNextProposalNumber generates a proposal number which will be passed to
// Propose. Proposal numbers should not repeat for a key, and for a particular
// <node, key> pair, they should be strictly increasing.
//
// Params:
// args: the key to propose
// reply: the next proposal number for the given key
func (pn *paxosNode) GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error {
	reply.N = pn.storage.GetNextProposalNumber(VALUE, args.Key)
	return nil
}

// Desc:
// Propose initializes proposing a value for a key, and replies with the
// value that was committed for that key. Propose should not return until
// a value has been committed, or PROPOSE_TIMEOUT seconds have passed.
//
// Params:
// args: the key, value pair to propose together with the proposal number returned by GetNextProposalNumber
// reply: value that was actually committed for the given key
func (pn *paxosNode) Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error {
	proposeFn := func() interface{} {
		// Propose Stage
		var maxMutex sync.Mutex
		maxN := -1
		var maxV interface{} = nil
		processPair := func(n int, v interface{}) {
			maxMutex.Lock()
			if n > maxN {
				maxN = n
				maxV = v
			}
			maxMutex.Unlock()
		}
		if success := pn.quorum.SendProposeToAll(paxosrpc.VALUE, args.Key, args.N, args.V, processPair); !success {
			return errors.New("Proposal did not achieve a majority")
		}

		// Accept Stage
		maxMutex.Lock()
		if maxN == -1 {
			maxN = args.N
			maxV = args.V
		}
		maxMutex.Unlock()
		if success := pn.quorum.SendAcceptToAll(paxosrpc.VALUE, args.Key, args.N, maxV); !success {
			return errors.New("Accept did not achieve a majority")
		}

		// Commit Stage
		pn.quorum.SendCommitToAll(paxosrpc.VALUE, args.Key, maxV)

		reply.V = maxV
		return nil
	}

	// We wait for whichever comes first, proposal or timeout
	if result, err := utils.ExecuteWithinTimelimit(proposeFn, PROPOSE_TIMEOUT); err != nil {
		return err
	} else {
		if result != nil {
			return result.(error)
		} else {
			return nil
		}
	}
}

// Desc:
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	if value := pn.storage.GetValue(args.Key); value != nil {
		reply.V = value
		reply.Status = paxosrpc.KeyFound
	} else {
		reply.Status = paxosrpc.KeyNotFound
	}
	return nil
}

// Desc:
// GetNextListProposalNumber generates a proposal number for a list key which
// will be passed to Propose. Proposal numbers should not repeat for a key, and
// for a particular <node, key> pair, they should be strictly increasing.
//
// Params:
// args: the key to propose
// reply: the next proposal number for the given key
func (pn *paxosNode) GetNextListProposalNumber(args *paxosrpc.ListProposalNumberArgs, reply *paxosrpc.ListProposalNumberReply) error {
	reply.N = pn.storage.GetNextProposalNumber(LIST, args.Key)
	return nil
}

// Desc:
// ProposeAppendToList initializes proposing a value to append for a key, and
// replies with the value that was committed and appended for that key.
// ProposeAppendToList should not return until a value has been committed, or
// PROPOSE_TIMEOUT seconds have passed.
//
// Params:
// args: the key, value to append pair to propose together with the proposalnumber returned by GetNextProposalNumber
// reply: value that was actually committed for the given key
func (pn *paxosNode) ProposeAppendToList(args *paxosrpc.ProposeAppendToListArgs, reply *paxosrpc.ProposeAppendToListReply) error {
	proposeFn := func() interface{} {
		// Propose Stage
		var maxMutex sync.Mutex
		maxN := -1
		var maxV interface{} = nil
		processPair := func(n int, v interface{}) {
			maxMutex.Lock()
			if n > maxN {
				maxN = n
				maxV = v
			}
			maxMutex.Unlock()
		}
		if success := pn.quorum.SendProposeToAll(paxosrpc.LIST, args.Key, args.N, args.V, processPair); !success {
			return errors.New("Proposal did not achieve a majority")
		}

		// Accept Stage
		maxMutex.Lock()
		if maxN == -1 {
			maxN = args.N
			maxV = args.V
		}
		maxMutex.Unlock()
		if success := pn.quorum.SendAcceptToAll(paxosrpc.LIST, args.Key, args.N, maxV); !success {
			return errors.New("Accept did not achieve a majority")
		}

		// Commit Stage
		pn.quorum.SendCommitToAll(paxosrpc.LIST, args.Key, maxV)

		reply.V = maxV
		return nil
	}

	// We wait for whichever comes first, proposal or timeout
	if result, err := utils.ExecuteWithinTimelimit(proposeFn, PROPOSE_TIMEOUT); err != nil {
		return err
	} else {
		if result != nil {
			return result.(error)
		} else {
			return nil
		}
	}
}

// Desc:
// GetList looks up the list for a key, and replies with the list or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the list and status for this lookup of the given key
func (pn *paxosNode) GetList(args *paxosrpc.GetListArgs, reply *paxosrpc.GetListReply) error {
	if value := pn.storage.GetList(args.Key); value != nil {
		reply.L = value
		reply.Status = paxosrpc.KeyFound
	} else {
		reply.Status = paxosrpc.KeyNotFound
	}
	return nil
}

// Desc:
// Receive a Prepare message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the prepare
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Prepare Message, you must include RequesterId when you call this API
// reply: the Prepare Reply Message
func (pn *paxosNode) RecvPrepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error {
	var dType StorageDataType
	switch args.DataType {
	case paxosrpc.VALUE:
		dType = VALUE
	case paxosrpc.LIST:
		dType = LIST
	}

	if args.N < pn.storage.GetMaxSeenProposal(dType, args.Key) {
		reply.Status = paxosrpc.Reject
	} else {
		pn.storage.SetMaxSeenProposal(dType, args.Key, args.N)
		reply.N_a, reply.V_a = pn.storage.GetMaxPair(dType, args.Key)
		reply.Status = paxosrpc.OK
	}
	return nil
}

// Desc:
// Receive an Accept message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the accept
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Please Accept Message, you must include RequesterId when you call this API
// reply: the Accept Reply Message
func (pn *paxosNode) RecvAccept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error {
	var dType StorageDataType
	switch args.DataType {
	case paxosrpc.VALUE:
		dType = VALUE
	case paxosrpc.LIST:
		dType = LIST
	}

	if args.N < pn.storage.GetMaxSeenProposal(dType, args.Key) {
		reply.Status = paxosrpc.Reject
	} else {
		pn.storage.SetMaxPair(dType, args.Key, args.N, args.V)
		reply.Status = paxosrpc.OK
	}
	return nil
}

// Desc:
// Receive a Commit message from another Paxos Node. The message contains
// the key whose value was proposed by the node sending the commit
// message.
//
// Params:
// args: the Commit Message, you must include RequesterId when you call this API
// reply: the Commit Reply Message
func (pn *paxosNode) RecvCommit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error {
	switch args.DataType {
	case paxosrpc.VALUE:
		pn.storage.CommitValue(args.Key, args.V)
	case paxosrpc.LIST:
		pn.storage.CommitAppend(args.Key, args.V)
	}

	// Send NotifyNewCommit
	if pn.notify {
		args := &paxosrpc.NotifyNewCommitArgs{DataType: args.DataType, Key: args.Key, V: args.V}
		var reply paxosrpc.NotifyNewCommitReply
		if err := pn.notifyClient.Call("PaxosCallbacks.NotifyNewCommit", args, &reply); err != nil {
			return err
		}
	}

	return nil
}

// Desc:
// Notify another node of a replacement server which has started up. The
// message contains the Server ID of the node being replaced, and the
// hostport of the replacement node
//
// Params:
// args: the id and the hostport of the server being replaced
// reply: no use
func (pn *paxosNode) RecvReplaceServer(args *paxosrpc.ReplaceServerArgs, reply *paxosrpc.ReplaceServerReply) error {
	pn.quorum.NewAcceptor(args.SrvID, args.Hostport, pn.numRetries)
	return nil
}

// Desc:
// Request the value that was agreed upon for a particular round. A node
// receiving this message should reply with the data (as an array of bytes)
// needed to make the replacement server aware of the keys and values
// committed so far.
//
// Params:
// args: no use
// reply: a byte array containing necessary data used by replacement server to recover
func (pn *paxosNode) RecvReplaceCatchup(args *paxosrpc.ReplaceCatchupArgs, reply *paxosrpc.ReplaceCatchupReply) error {
	if data, err := pn.storage.Serialize(); err != nil {
		return err
	} else {
		reply.Data = data
		return nil
	}
}
