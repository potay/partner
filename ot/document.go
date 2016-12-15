package ot

import (
	"encoding/json"
)

var noOp Operation

// An operation represents a change in the document
type Operation struct {
	Ops  Ops
	Meta OpMeta // Operation metadata
}

// Metadata for an operation
type OpMeta struct {
	Authors []string
}

// Marshals the operation to a []byte representation, for storage, etc.
func (op *Operation) Marshal() ([]byte, error) {
	return json.Marshal(op)
}

// Unmarshals the operation from a []byte representation
func (op *Operation) Unmarshal(raw []byte) error {
	return json.Unmarshal(raw, &op)
}

func (op *Operation) Empty() bool {
	return op.Ops == nil
}

func (op *Operation) Equal(other Operation) bool {
	return op.Ops.Equal(other.Ops)
}

func (op *Operation) Identical(other Operation) bool {
	if !op.Equal(other) {
		return false
	}

	if len(op.Meta.Authors) != len(other.Meta.Authors) {
		return false
	}

	for i := range op.Meta.Authors {
		if op.Meta.Authors[i] != other.Meta.Authors[i] {
			return false
		}
	}

	return true
}

func (op *Operation) Compose(otherOp Operation) (Operation, error) {
	newOps, err := Compose(op.Ops, otherOp.Ops)
	if err != nil {
		return noOp, err
	}
	return Operation{
		Ops: newOps,
		Meta: OpMeta{
			Authors: append(op.Meta.Authors, otherOp.Meta.Authors...),
		},
	}, nil
}

func (op *Operation) Transform(otherOp Operation) (Operation, Operation, error) {
	if ops1, ops2, err := Transform(op.Ops, otherOp.Ops); err != nil {
		return noOp, noOp, err
	} else {
		return Operation{
				Ops: ops1,
				Meta: OpMeta{
					Authors: otherOp.Meta.Authors,
				},
			},
			Operation{
				Ops: ops2,
				Meta: OpMeta{
					Authors: op.Meta.Authors,
				},
			},
			nil
	}
}
