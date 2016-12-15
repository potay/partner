package participant

import (
	"fmt"
	"github.com/rs/xid"
	"strings"
)

// Key formats for storage in our paxos ring

// Returns the document key format
func getDocumentKey(docID string) string {
	return fmt.Sprintf("document-%s", docID)
}

// Returns the docID from the document key
func getDocIDFromKey(key string) (string, error) {
	subs := strings.SplitAfterN(key, "document-", 2)
	if len(subs) < 2 {
		return "", fmt.Errorf("Unable to parse key")
	} else {
		return subs[1], nil
	}
}

// Returns the client key format
func getClientKey(userID xid.ID) string {
	return fmt.Sprintf("user-%s", userID)
}

// Returns the ClientID from the client key
func getClientIDFromKey(key string) (xid.ID, error) {
	subs := strings.SplitAfterN(key, "user-", 2)
	if len(subs) < 2 {
		return xid.ID{}, fmt.Errorf("Unable to parse key")
	} else {
		if id, err := xid.FromString(subs[1]); err != nil {
			return xid.ID{}, fmt.Errorf("Unable to parse key")
		} else {
			return id, nil
		}
	}
}
