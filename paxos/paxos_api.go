package paxos

import (
	"github.com/potay/partner/rpc/paxosrpc"
)

// GetNextProposalNumber generates a proposal number which will be passed to Propose.
// Propose initializes proposing a value for a key, and replies with the value that was committed for that key.
// GetValue looks up the value for a key, and replies with the value or with KeyNotFound.
type PaxosNode interface {
	GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error
	Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error
	GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error
	GetNextListProposalNumber(args *paxosrpc.ListProposalNumberArgs, reply *paxosrpc.ListProposalNumberReply) error
	ProposeAppendToList(args *paxosrpc.ProposeAppendToListArgs, reply *paxosrpc.ProposeAppendToListReply) error
	GetList(args *paxosrpc.GetListArgs, reply *paxosrpc.GetListReply) error
}
