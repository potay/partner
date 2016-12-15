package participantrpc

type RemoteParticipant interface {
	Heartbeat(*HeartbeatArgs, *HeartbeatReply) error
}

type Participant struct {
	// Embed all methods into the struct. See the Effective Go section about
	// embedding for more details: golang.org/doc/effective_go.html#embedding
	RemoteParticipant
}

// Wrap wraps p in a type-safe wrapper struct to ensure that only the desired
// CommitCallbacks methods are exported to receive RPCs.
func Wrap(p RemoteParticipant) RemoteParticipant {
	return &Participant{p}
}
