package paxosrpc

// Status represents the status of a RPC's reply.
type Status int
type Lookup int
type DataType int

const (
	OK     Status = iota + 1 // Paxos replied OK
	Reject                   // Paxos rejected the message
)

const (
	KeyFound    Lookup = iota + 1 // GetValue key found
	KeyNotFound                   // GetValue key not found
)

const (
	VALUE DataType = iota + 1 // Value type of data
	LIST                      // List type of data
)

type ProposalNumberArgs struct {
	Key string
}

type ProposalNumberReply struct {
	N int
}

type ProposeArgs struct {
	N   int // Proposal number
	Key string
	V   interface{} // Value for the Key
}

type ProposeReply struct {
	V interface{} // Value that was actually committed for that key
}

type GetValueArgs struct {
	Key string
}

type GetValueReply struct {
	V      interface{}
	Status Lookup
}

type ListProposalNumberArgs struct {
	Key string
}

type ListProposalNumberReply struct {
	N int
}

type ProposeAppendToListArgs struct {
	N   int // Proposal number
	Key string
	V   interface{} // Value to append for the Key
}

type ProposeAppendToListReply struct {
	V interface{} // Value that was actually committed and appended for that key
}

type GetListArgs struct {
	Key string
}

type GetListReply struct {
	L      []interface{}
	Status Lookup
}

type NotifyNewCommitArgs struct {
	DataType DataType
	Key      string
	V        interface{} // Value that was commited for the Key
}

type NotifyNewCommitReply struct {
	// No content necessary
}

type PrepareArgs struct {
	DataType    DataType
	Key         string
	N           int
	RequesterId int
}

type PrepareReply struct {
	Status Status
	N_a    int         // Highest proposal number accepted
	V_a    interface{} // Corresponding value
}

type AcceptArgs struct {
	DataType    DataType
	Key         string
	N           int
	V           interface{}
	RequesterId int
}

type AcceptReply struct {
	Status Status
}

type CommitArgs struct {
	DataType    DataType
	Key         string
	V           interface{}
	RequesterId int
}

type CommitReply struct {
	// No content, no reply necessary
}

type ReplaceServerArgs struct {
	SrvID    int // Server being replaced
	Hostport string
}

type ReplaceServerReply struct {
	// No content necessary
}

type ReplaceCatchupArgs struct {
	// No content necessary
}

type ReplaceCatchupReply struct {
	Data []byte
}
