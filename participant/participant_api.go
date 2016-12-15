package participant

import (
	"github.com/potay/partner/ot"
	"github.com/rs/xid"
)

type ParticipantClient struct {
	ID        xid.ID
	Name      string
	DocID     string
	CursorPos int
}

// Participant is a reader/writer participating in a Partner workspace
type Participant interface {
	// Registers a client with this participant
	RegisterClient(client ParticipantClient) (xid.ID, error)

	// Unregisters a client with this participant
	UnregisterClient(clientID xid.ID) error

	// Adds a change to the doc via the operation
	AddDocChange(clientID xid.ID, docID string, op ot.Operation) error

	// Updates the stored client info
	UpdateClient(clientID xid.ID, client ParticipantClient) error

	// Responds with the necessary list of operations in order to catch up
	// to the current document state
	DocCatchUp(clientID xid.ID, docID string) ([]ot.Operation, error)
}

type ParticipantServer interface {
	BroadcastDocChange(docID string, op ot.Operation) error
	BroadcastClientUpdate(clientID xid.ID, client ParticipantClient) error
	BroadcastClientRemove(clientID xid.ID) error
}
