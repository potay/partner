package paxos

import (
	"bytes"
	"encoding/gob"
	"sync"
)

type StorageDataType int

const (
	VALUE StorageDataType = iota + 1 // Value type of data
	LIST                             // List type of data
)

// Contains all the information relevant to a stored key-value data
// Thread-safe if you lock it
type paxosData struct {
	HighestProposal int // Highest proposal number seen/given
	ProposalNumber  int // Highest proposal number with proposal value
	ProposalValue   interface{}
	Value           interface{}
	mutex           sync.Mutex
}

func newPaxosData(srvId int) *paxosData {
	return &paxosData{
		HighestProposal: -1,
		ProposalNumber:  -1,
		ProposalValue:   nil,
	}
}

func (pd *paxosData) Lock() {
	pd.mutex.Lock()
}

func (pd *paxosData) Unlock() {
	pd.mutex.Unlock()
}

// Contains all the information relevant to a stored key-list data
// Thread-safe if you lock it
type paxosListData struct {
	HighestProposal int // Highest proposal number seen/given
	ProposalNumber  int // Highest proposal number with proposal value
	ProposalValue   interface{}
	Value           []interface{}
	mutex           sync.Mutex
}

func newPaxosListData(srvId int) *paxosListData {
	return &paxosListData{
		HighestProposal: -1,
		ProposalNumber:  -1,
		ProposalValue:   nil,
		Value:           make([]interface{}, 0),
	}
}

func (pd *paxosListData) Lock() {
	pd.mutex.Lock()
}

func (pd *paxosListData) Unlock() {
	pd.mutex.Unlock()
}

// Paxos key-value data storage
// Thread-safe
type paxosStorage struct {
	srvId int

	Storage map[string]*paxosData
	mutex   sync.RWMutex

	ListStorage map[string]*paxosListData
	listMutex   sync.RWMutex
}

func newPaxosStorage(srvId int) *paxosStorage {
	ps := &paxosStorage{
		srvId:       srvId,
		Storage:     make(map[string]*paxosData),
		ListStorage: make(map[string]*paxosListData),
	}

	return ps
}

// Commit the key-value to the storage
func (ps *paxosStorage) CommitValue(key string, value interface{}) {
	ps.mutex.Lock()
	if data, ok := ps.Storage[key]; ok {
		ps.mutex.Unlock()
		data.Lock()
		data.Value = value
		data.ProposalNumber = -1
		data.ProposalValue = nil
		data.Unlock()
	} else {
		data = newPaxosData(ps.srvId)
		ps.Storage[key] = data
		data.Lock()
		ps.mutex.Unlock()
		data.Value = value
		data.Unlock()
	}
}

// Commit the key-list to the storage
func (ps *paxosStorage) CommitAppend(key string, value interface{}) {
	ps.listMutex.Lock()
	if data, ok := ps.ListStorage[key]; ok {
		ps.listMutex.Unlock()
		data.Lock()
		data.Value = append(data.Value, value)
		data.ProposalNumber = -1
		data.ProposalValue = nil
		data.Unlock()
	} else {
		data = newPaxosListData(ps.srvId)
		ps.ListStorage[key] = data
		data.Lock()
		ps.listMutex.Unlock()
		data.Value = append(data.Value, value)
		data.Unlock()
	}
}

// Get latest commited value for key
func (ps *paxosStorage) GetValue(key string) interface{} {
	ps.mutex.RLock()
	if data, ok := ps.Storage[key]; ok {
		ps.mutex.RUnlock()
		data.Lock()
		defer data.Unlock()
		return data.Value
	} else {
		ps.mutex.RUnlock()
		return nil
	}
}

// Get latest commited list for key
func (ps *paxosStorage) GetList(key string) []interface{} {
	ps.listMutex.RLock()
	if data, ok := ps.ListStorage[key]; ok {
		ps.listMutex.RUnlock()
		data.Lock()
		defer data.Unlock()
		return data.Value
	} else {
		ps.listMutex.RUnlock()
		return nil
	}
}

// Gets the next proposal number for key
func (ps *paxosStorage) GetNextProposalNumber(dataType StorageDataType, key string) int {
	switch dataType {
	case VALUE:
		ps.mutex.RLock()
		if data, ok := ps.Storage[key]; ok {
			ps.mutex.RUnlock()
			data.Lock()
			defer data.Unlock()
			if data.HighestProposal >= 0 {
				return (data.HighestProposal+MAX_NODES-1)/MAX_NODES*MAX_NODES + ps.srvId
			} else {
				return ps.srvId
			}
		} else {
			ps.mutex.RUnlock()
			return ps.srvId
		}
	case LIST:
		ps.listMutex.RLock()
		if data, ok := ps.ListStorage[key]; ok {
			ps.listMutex.RUnlock()
			data.Lock()
			defer data.Unlock()
			if data.HighestProposal >= 0 {
				return (data.HighestProposal+MAX_NODES-1)/MAX_NODES*MAX_NODES + ps.srvId
			} else {
				return ps.srvId
			}
		} else {
			ps.listMutex.RUnlock()
			return ps.srvId
		}
	default:
		panic("DataType not supported by paxosStorage.")
	}

}

// Set the max seen proposal number to num if num is bigger
func (ps *paxosStorage) SetMaxSeenProposal(dataType StorageDataType, key string, num int) {
	switch dataType {
	case VALUE:
		ps.mutex.Lock()
		if data, ok := ps.Storage[key]; ok {
			ps.mutex.Unlock()
			data.Lock()
			if data.HighestProposal < num {
				data.HighestProposal = num
			}
			data.Unlock()
		} else {
			data = newPaxosData(ps.srvId)
			ps.Storage[key] = data
			data.Lock()
			ps.mutex.Unlock()
			data.HighestProposal = num
			data.Unlock()
		}
	case LIST:
		ps.listMutex.Lock()
		if data, ok := ps.ListStorage[key]; ok {
			ps.listMutex.Unlock()
			data.Lock()
			if data.HighestProposal < num {
				data.HighestProposal = num
			}
			data.Unlock()
		} else {
			data = newPaxosListData(ps.srvId)
			ps.ListStorage[key] = data
			data.Lock()
			ps.listMutex.Unlock()
			data.HighestProposal = num
			data.Unlock()
		}
	default:
		panic("DataType not supported by paxosStorage.")
	}
}

// Gets the max seen proposal number
func (ps *paxosStorage) GetMaxSeenProposal(dataType StorageDataType, key string) int {
	switch dataType {
	case VALUE:
		ps.mutex.RLock()
		if data, ok := ps.Storage[key]; ok {
			ps.mutex.RUnlock()
			data.Lock()
			defer data.Unlock()
			return data.HighestProposal
		} else {
			ps.mutex.RUnlock()
			return -1
		}
	case LIST:
		ps.listMutex.RLock()
		if data, ok := ps.ListStorage[key]; ok {
			ps.listMutex.RUnlock()
			data.Lock()
			defer data.Unlock()
			return data.HighestProposal
		} else {
			ps.listMutex.RUnlock()
			return -1
		}
	default:
		panic("DataType not supported by paxosStorage.")
	}
}

// Sets the max seen proposal-value pair if num is bigger
func (ps *paxosStorage) SetMaxPair(dataType StorageDataType, key string, num int, value interface{}) {
	switch dataType {
	case VALUE:
		ps.mutex.Lock()
		if data, ok := ps.Storage[key]; ok {
			ps.mutex.Unlock()
			data.Lock()
			data.ProposalNumber = num
			data.ProposalValue = value
			data.Unlock()
		} else {
			data = newPaxosData(ps.srvId)
			ps.Storage[key] = data
			data.Lock()
			ps.mutex.Unlock()
			data.ProposalNumber = num
			data.ProposalValue = value
			data.Unlock()
		}
	case LIST:
		ps.listMutex.Lock()
		if data, ok := ps.ListStorage[key]; ok {
			ps.listMutex.Unlock()
			data.Lock()
			data.ProposalNumber = num
			data.ProposalValue = value
			data.Unlock()
		} else {
			data = newPaxosListData(ps.srvId)
			ps.ListStorage[key] = data
			data.Lock()
			ps.listMutex.Unlock()
			data.ProposalNumber = num
			data.ProposalValue = value
			data.Unlock()
		}
	default:
		panic("DataType not supported by paxosStorage.")
	}
}

// Gets the max seen proposal-value pair
func (ps *paxosStorage) GetMaxPair(dataType StorageDataType, key string) (int, interface{}) {
	switch dataType {
	case VALUE:
		ps.mutex.RLock()
		if data, ok := ps.Storage[key]; ok {
			ps.mutex.RUnlock()
			data.Lock()
			defer data.Unlock()
			return data.ProposalNumber, data.ProposalValue
		} else {
			ps.mutex.RUnlock()
			return -1, nil
		}
	case LIST:
		ps.listMutex.RLock()
		if data, ok := ps.ListStorage[key]; ok {
			ps.listMutex.RUnlock()
			data.Lock()
			defer data.Unlock()
			return data.ProposalNumber, data.ProposalValue
		} else {
			ps.listMutex.RUnlock()
			return -1, nil
		}
	default:
		panic("DataType not supported by paxosStorage.")
	}
}

// Serializes storage to byte array that can be used to restore storage state
func (ps *paxosStorage) Serialize() ([]byte, error) {
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)

	// Encoding the storage state
	ps.mutex.RLock()
	ps.listMutex.RLock()
	err := e.Encode(ps)
	ps.listMutex.RUnlock()
	ps.mutex.RUnlock()
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

// Restores a storage to a state from data bytes
func (ps *paxosStorage) Restore(data []byte) error {
	b := bytes.NewBuffer(data)
	d := gob.NewDecoder(b)

	// Decoding the serialized data
	ps.mutex.Lock()
	ps.listMutex.Lock()
	err := d.Decode(&ps)
	ps.listMutex.Unlock()
	ps.mutex.Unlock()
	if err != nil {
		return err
	}
	return nil
}
