package paxos

import (
	"errors"
	"fmt"
	"github.com/potay/partner/rpc/paxosrpc"
	"log"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type paxosAcceptor struct {
	srvId    int
	hostPort string
	client   *rpc.Client
	mutex    sync.RWMutex
}

func newPaxosAcceptor(srvId int, hostPort string) *paxosAcceptor {
	return &paxosAcceptor{
		srvId:    srvId,
		hostPort: hostPort,
		client:   nil,
	}
}

func (pa *paxosAcceptor) Connect(numRetries int) error {
	pa.mutex.Lock()
	defer pa.mutex.Unlock()
	// attempt to connect with node
	var err error
	for i := 0; i < numRetries; i++ {
		// attempt to connect to node
		pa.client, err = rpc.DialHTTP("tcp", pa.hostPort)
		if err != nil {
			// wait for one second before looping again
			timer := time.NewTimer(RETRY_INTERVAL)
			<-timer.C
			continue
		} else {
			break
		}
	}

	if pa.client == nil {
		return errors.New(fmt.Sprintf("Cannot connect to acceptor node %d at %s", pa.srvId, pa.hostPort))
	}

	return nil
}

func (pa *paxosAcceptor) CallClient(serviceMethod string, args interface{}, reply interface{}) error {
	pa.mutex.RLock()
	if pa.client == nil {
		pa.mutex.RUnlock()
		if err := pa.Connect(1); err != nil {
			return err
		}
		pa.mutex.RLock()
	}
	defer pa.mutex.RUnlock()
	if err := pa.client.Call(serviceMethod, args, reply); err != nil {
		return err
	}
	return nil
}

type paxosQuorum struct {
	srvId     int
	numNodes  int
	acceptors map[int]*paxosAcceptor
	mutex     sync.RWMutex
}

func newPaxosQuorum(numNodes, srvId int) *paxosQuorum {
	return &paxosQuorum{
		srvId:     srvId,
		numNodes:  numNodes,
		acceptors: make(map[int]*paxosAcceptor),
	}
}

func (pq *paxosQuorum) AddAcceptors(hostMap map[int]string, numRetries int) error {
	var wg sync.WaitGroup
	var connectedCount uint32 = 0
	for nodeID, nodeHostPort := range hostMap {
		acceptor := newPaxosAcceptor(nodeID, nodeHostPort)
		pq.mutex.Lock()
		pq.acceptors[nodeID] = acceptor
		pq.mutex.Unlock()

		wg.Add(1)
		go func(acceptor *paxosAcceptor) {
			defer wg.Done()
			if err := acceptor.Connect(numRetries); err == nil {
				atomic.AddUint32(&connectedCount, 1)
			}
		}(acceptor)
	}
	wg.Wait()

	if int(connectedCount) == len(hostMap) {
		return nil
	} else {
		return errors.New("Could not connect to all the acceptor nodes")
	}
}

func (pq *paxosQuorum) NewAcceptor(nodeID int, hostPort string, numRetries int) error {
	acceptor := newPaxosAcceptor(nodeID, hostPort)
	pq.mutex.Lock()
	pq.acceptors[nodeID] = acceptor
	pq.mutex.Unlock()

	return acceptor.Connect(numRetries)
}

func (pq *paxosQuorum) sendToAll(sendFn func(acceptor *paxosAcceptor) bool) bool {
	retCh := make(chan bool)
	pq.mutex.RLock()
	count := len(pq.acceptors)
	for _, acceptor := range pq.acceptors {
		go func(acceptor *paxosAcceptor) {
			retCh <- sendFn(acceptor)
		}(acceptor)
	}
	pq.mutex.RUnlock()

	agreeCount := 0
	for i := 0; i < count; i++ {
		if <-retCh {
			agreeCount++
			// We have reached majority so we return success while handling the rest
			// in the background
			if agreeCount >= pq.numNodes/2+1 {
				go func() {
					for j := i + 1; j < count; j++ {
						<-retCh
					}
				}()
				return true
			}
		} else {
			// Not possible to reach majority so we return failure while handling the
			// rest in the background
			if count-i-1+agreeCount < pq.numNodes/2+1 {
				go func() {
					for j := i + 1; j < count; j++ {
						<-retCh
					}
				}()
				return false
			}
		}
	}
	return false
}

func (pq *paxosQuorum) SendProposeToAll(dataType paxosrpc.DataType, key string, n int, v interface{}, processPairFn func(n int, v interface{})) bool {
	sendFn := func(acceptor *paxosAcceptor) bool {
		args := &paxosrpc.PrepareArgs{
			DataType:    dataType,
			Key:         key,
			N:           n,
			RequesterId: pq.srvId,
		}
		var reply paxosrpc.PrepareReply
		if err := acceptor.CallClient("PaxosNode.RecvPrepare", args, &reply); err != nil {
			log.Printf("%d-%d (%s): %s", pq.srvId, acceptor.srvId, acceptor.hostPort, err)
			return false
		}
		if reply.Status == paxosrpc.OK {
			processPairFn(reply.N_a, reply.V_a)
			return true
		} else {
			return false
		}
	}

	return pq.sendToAll(sendFn)
}

func (pq *paxosQuorum) SendAcceptToAll(dataType paxosrpc.DataType, key string, n int, v interface{}) bool {
	sendFn := func(acceptor *paxosAcceptor) bool {
		args := &paxosrpc.AcceptArgs{
			DataType:    dataType,
			Key:         key,
			N:           n,
			V:           v,
			RequesterId: pq.srvId,
		}
		var reply paxosrpc.AcceptReply
		if err := acceptor.CallClient("PaxosNode.RecvAccept", args, &reply); err != nil {
			log.Printf("%d-%d (%s): %s", pq.srvId, acceptor.srvId, acceptor.hostPort, err)
			return false
		}
		if reply.Status == paxosrpc.OK {
			return true
		} else {
			return false
		}
	}

	return pq.sendToAll(sendFn)
}

func (pq *paxosQuorum) SendCommitToAll(dataType paxosrpc.DataType, key string, v interface{}) {
	var wg sync.WaitGroup
	pq.mutex.RLock()
	for _, acceptor := range pq.acceptors {
		wg.Add(1)
		go func(acceptor *paxosAcceptor) {
			defer wg.Done()
			args := &paxosrpc.CommitArgs{
				DataType:    dataType,
				Key:         key,
				V:           v,
				RequesterId: pq.srvId,
			}
			var reply paxosrpc.CommitReply
			if err := acceptor.CallClient("PaxosNode.RecvCommit", args, &reply); err != nil {
				log.Printf("%d-%d (%s): %s", pq.srvId, acceptor.srvId, acceptor.hostPort, err)
			}
		}(acceptor)
	}
	pq.mutex.RUnlock()
	wg.Wait()
	return
}

func (pq *paxosQuorum) SendReplaceServerToAll() {
	var wg sync.WaitGroup
	pq.mutex.RLock()
	hostPort := pq.acceptors[pq.srvId].hostPort
	for _, acceptor := range pq.acceptors {
		wg.Add(1)
		go func(acceptor *paxosAcceptor) {
			defer wg.Done()
			args := &paxosrpc.ReplaceServerArgs{
				SrvID:    pq.srvId,
				Hostport: hostPort,
			}
			var reply paxosrpc.ReplaceServerReply
			if err := acceptor.CallClient("PaxosNode.RecvReplaceServer", args, &reply); err != nil {
				log.Print(err)
			}
		}(acceptor)
	}
	pq.mutex.RUnlock()
	wg.Wait()
	return
}

func (pq *paxosQuorum) SendReplaceCatchupToAll() []byte {
	dataCh := make(chan []byte)
	pq.mutex.RLock()
	count := 0
	for _, acceptor := range pq.acceptors {
		// Skip over ourselves
		if acceptor.srvId == pq.srvId {
			continue
		}

		// Make rpc calls to other nodes and keep count
		count++
		go func(acceptor *paxosAcceptor) {
			var args paxosrpc.ReplaceCatchupArgs
			var reply paxosrpc.ReplaceCatchupReply
			if err := acceptor.CallClient("PaxosNode.RecvReplaceCatchup", &args, &reply); err != nil {
				log.Print(err)
				dataCh <- nil
			}
			dataCh <- reply.Data
		}(acceptor)
	}
	pq.mutex.RUnlock()

	var data []byte
	// Save the first result that is not faulty
	for data == nil && count > 0 {
		data = <-dataCh
		count--
	}

	// Discard all the other results
	go func() {
		for i := 0; i < count; i++ {
			<-dataCh
		}
	}()

	return data
}
