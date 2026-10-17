// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sds

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Writer provides Layer 2 relational row-change and transaction writing
// over a structured data stream using OpRecord framework records.
type Writer struct {
	recordWriter *record.Writer
	nextTxID     atomic.Uint64
}

// NewWriter creates a new Layer 2 Writer wrapping the given Appender transport.
func NewWriter(appender record.Appender, opts ...record.WriterOption) *Writer {
	return &Writer{
		recordWriter: record.NewWriter(appender, opts...),
	}
}

// Registry returns the in-band type registry.
func (w *Writer) Registry() *record.Registry {
	return w.recordWriter.Registry()
}

// RecordWriter returns the underlying Layer 1 record.Writer.
func (w *Writer) RecordWriter() *record.Writer {
	return w.recordWriter
}

// AppendStreamHeader writes an explicit StreamHeader framework record (TypeID = 6).
func (w *Writer) AppendStreamHeader(ctx context.Context, hdr *sdsv1.StreamHeader) (uint64, error) {
	return w.recordWriter.AppendStreamHeader(ctx, hdr)
}

// RegisterType registers a Go proto.Message type with optional primary key field numbers.
func (w *Writer) RegisterType(msg proto.Message, keyFields ...int32) (uint32, error) {
	return w.recordWriter.RegisterType(msg, keyFields...)
}

// RegisterDescriptor registers a MessageDescriptor with optional primary key field numbers.
func (w *Writer) RegisterDescriptor(md protoreflect.MessageDescriptor, keyFields ...int32) (uint32, error) {
	return w.recordWriter.RegisterDescriptor(md, keyFields...)
}

func (w *Writer) resolveOrRegister(msg proto.Message) (*sdsv1.TypeDefinition, error) {
	if msg == nil {
		return nil, fmt.Errorf("nil message")
	}
	md := msg.ProtoReflect().Descriptor()
	name := string(md.FullName())

	reg := w.recordWriter.Registry()
	def, _, exists := reg.LookupByName(name)
	if exists && def != nil {
		return def, nil
	}

	return reg.RegisterMessage(msg)
}

func (w *Writer) writeOp(ctx context.Context, op sdsv1.OpRecord_Op, txID uint64, msg proto.Message) (uint64, error) {
	if msg == nil {
		return 0, fmt.Errorf("nil message")
	}

	def, err := w.resolveOrRegister(msg)
	if err != nil {
		return 0, err
	}

	keyBytes, valBytes, err := SplitKeyAndNonKey(msg, def.GetKeyFields())
	if err != nil {
		return 0, err
	}

	opRec := &sdsv1.OpRecord{
		Op:     op,
		TypeId: def.GetId(),
		TxId:   txID,
		Key:    keyBytes,
	}
	if op != sdsv1.OpRecord_DELETE {
		opRec.Value = valBytes
	}

	return w.recordWriter.AppendOp(ctx, opRec)
}

// Insert writes an autocommit Create row change (tx_id = 0, OpRecord_CREATE).
func (w *Writer) Insert(ctx context.Context, msg proto.Message) (uint64, error) {
	return w.writeOp(ctx, sdsv1.OpRecord_CREATE, 0, msg)
}

// Create writes an autocommit Create row change (tx_id = 0, OpRecord_CREATE).
func (w *Writer) Create(ctx context.Context, msg proto.Message) (uint64, error) {
	return w.writeOp(ctx, sdsv1.OpRecord_CREATE, 0, msg)
}

// Update writes an autocommit Update row change (tx_id = 0, OpRecord_UPDATE).
func (w *Writer) Update(ctx context.Context, msg proto.Message) (uint64, error) {
	return w.writeOp(ctx, sdsv1.OpRecord_UPDATE, 0, msg)
}

// Delete writes an autocommit Delete row change (tx_id = 0, OpRecord_DELETE).
// msg must contain at least the primary key fields.
func (w *Writer) Delete(ctx context.Context, msg proto.Message) (uint64, error) {
	return w.writeOp(ctx, sdsv1.OpRecord_DELETE, 0, msg)
}

// Begin starts a new multi-record transaction.
func (w *Writer) Begin() *Tx {
	txID := w.nextTxID.Add(1)
	return &Tx{
		writer: w,
		txID:   txID,
	}
}

// AppendSnapshotPointer writes a SnapshotPointer framework record (type ID = 3).
func (w *Writer) AppendSnapshotPointer(ctx context.Context, ptr *sdsv1.SnapshotPointer) (uint64, error) {
	return w.recordWriter.AppendSnapshotPointer(ctx, ptr)
}

// AppendPadding writes a Padding framework record (type ID = 4) with the specified length.
func (w *Writer) AppendPadding(ctx context.Context, length int) (uint64, error) {
	return w.recordWriter.AppendPadding(ctx, length)
}

// AppendPaddingBytes writes a Padding framework record (type ID = 4) with arbitrary bytes.
func (w *Writer) AppendPaddingBytes(ctx context.Context, body []byte) (uint64, error) {
	return w.recordWriter.AppendPaddingBytes(ctx, body)
}

type readSetKey struct {
	typeName string
	key      Key
}

type readPrefixKey struct {
	typeName string
	prefix   string
}

// Tx represents an in-progress transaction.
type Tx struct {
	writer       *Writer
	txID         uint64
	finished     bool
	changes      []Change
	firstWriteOp string
	opName       string
	readSet      map[readSetKey]proto.Message
	readPrefixes map[readPrefixKey]bool
}

// TxID returns the transaction ID.
func (tx *Tx) TxID() uint64 {
	return tx.txID
}

// SetOpName sets the logical operation name for debug reporting.
func (tx *Tx) SetOpName(name string) {
	tx.opName = name
}

// OpName returns the logical operation name, or "" if unset.
func (tx *Tx) OpName() string {
	return tx.opName
}

// FirstWrite returns the description of the first write buffered in this transaction, or "" if none.
func (tx *Tx) FirstWrite() string {
	return tx.firstWriteOp
}

// HasWrites returns true if this transaction has buffered at least one write.
func (tx *Tx) HasWrites() bool {
	return tx.firstWriteOp != ""
}

// RecordRead records a row read by this transaction. A nil msg indicates the row was absent.
func (tx *Tx) RecordRead(typeName string, key Key, msg proto.Message) {
	if tx.readSet == nil {
		tx.readSet = make(map[readSetKey]proto.Message)
	}
	tx.readSet[readSetKey{typeName: typeName, key: key}] = msg
}

// RecordReadPrefix records that all rows matching prefix were scanned for typeName.
func (tx *Tx) RecordReadPrefix(typeName string, prefix []byte) {
	if tx.readPrefixes == nil {
		tx.readPrefixes = make(map[readPrefixKey]bool)
	}
	tx.readPrefixes[readPrefixKey{typeName: typeName, prefix: string(prefix)}] = true
}

// HasReadPrefix returns true if prefix was already recorded as read for typeName.
func (tx *Tx) HasReadPrefix(typeName string, prefix []byte) bool {
	if tx.readPrefixes == nil {
		return false
	}
	return tx.readPrefixes[readPrefixKey{typeName: typeName, prefix: string(prefix)}]
}

// LookupRead returns the before-image of a row from the transaction's read set.
// ok is true if the key was recorded in the read set directly or as part of a scanned prefix.
// If ok is true, msg is the row message, or nil if the row was absent.
func (tx *Tx) LookupRead(typeName string, key Key) (proto.Message, bool) {
	if tx.readSet != nil {
		if msg, ok := tx.readSet[readSetKey{typeName: typeName, key: key}]; ok {
			return msg, true
		}
	}
	if tx.readPrefixes != nil {
		for pk := range tx.readPrefixes {
			if pk.typeName == typeName && strings.HasPrefix(key.String(), pk.prefix) {
				return nil, true
			}
		}
	}
	return nil, false
}

func (tx *Tx) recordChange(seq uint64, op sdsv1.OpRecord_Op, msg proto.Message) error {
	def, err := tx.writer.resolveOrRegister(msg)
	if err != nil {
		return err
	}
	keyBytes, valBytes, err := SplitKeyAndNonKey(msg, def.GetKeyFields())
	if err != nil {
		return err
	}
	ch := Change{
		Seq:      seq,
		TypeID:   def.GetId(),
		TypeName: def.GetName(),
		Op:       op,
		TxID:     tx.txID,
		Key:      NewKeyFromBytes(keyBytes),
		RawKey:   keyBytes,
		RawVal:   valBytes,
		Row:      msg,
	}
	tx.changes = append(tx.changes, ch)
	if tx.firstWriteOp == "" {
		opName := sdsv1.OpRecord_Op_name[int32(op)]
		if opName == "" {
			opName = op.String()
		}
		tx.firstWriteOp = fmt.Sprintf("%s %s", opName, def.GetName())
	}
	return nil
}

// Insert writes an OpRecord_CREATE row change belonging to this transaction.
func (tx *Tx) Insert(ctx context.Context, msg proto.Message) (uint64, error) {
	if tx.finished {
		return 0, fmt.Errorf("transaction %d already closed", tx.txID)
	}
	seq, err := tx.writer.writeOp(ctx, sdsv1.OpRecord_CREATE, tx.txID, msg)
	if err != nil {
		return 0, err
	}
	if err := tx.recordChange(seq, sdsv1.OpRecord_CREATE, msg); err != nil {
		panic(fmt.Sprintf("sds: failed to record transaction change for create: %v", err))
	}
	return seq, nil
}

// Create writes an OpRecord_CREATE row change belonging to this transaction.
func (tx *Tx) Create(ctx context.Context, msg proto.Message) (uint64, error) {
	if tx.finished {
		return 0, fmt.Errorf("transaction %d already closed", tx.txID)
	}
	seq, err := tx.writer.writeOp(ctx, sdsv1.OpRecord_CREATE, tx.txID, msg)
	if err != nil {
		return 0, err
	}
	if err := tx.recordChange(seq, sdsv1.OpRecord_CREATE, msg); err != nil {
		panic(fmt.Sprintf("sds: failed to record transaction change for create: %v", err))
	}
	return seq, nil
}

// Update writes an OpRecord_UPDATE row change belonging to this transaction.
func (tx *Tx) Update(ctx context.Context, msg proto.Message) (uint64, error) {
	if tx.finished {
		return 0, fmt.Errorf("transaction %d already closed", tx.txID)
	}
	seq, err := tx.writer.writeOp(ctx, sdsv1.OpRecord_UPDATE, tx.txID, msg)
	if err != nil {
		return 0, err
	}
	if err := tx.recordChange(seq, sdsv1.OpRecord_UPDATE, msg); err != nil {
		panic(fmt.Sprintf("sds: failed to record transaction change for update: %v", err))
	}
	return seq, nil
}

// Delete writes an OpRecord_DELETE row change belonging to this transaction.
func (tx *Tx) Delete(ctx context.Context, msg proto.Message) (uint64, error) {
	if tx.finished {
		return 0, fmt.Errorf("transaction %d already closed", tx.txID)
	}
	seq, err := tx.writer.writeOp(ctx, sdsv1.OpRecord_DELETE, tx.txID, msg)
	if err != nil {
		return 0, err
	}
	if err := tx.recordChange(seq, sdsv1.OpRecord_DELETE, msg); err != nil {
		panic(fmt.Sprintf("sds: failed to record transaction change for delete: %v", err))
	}
	return seq, nil
}

// Commit commits this transaction by writing a TxCommit framework record.
func (tx *Tx) Commit(ctx context.Context) (uint64, error) {
	if tx.finished {
		return 0, fmt.Errorf("transaction %d already closed", tx.txID)
	}
	tx.finished = true
	commit := &sdsv1.TxCommit{
		TxId:       tx.txID,
		CommitTime: timestamppb.Now(),
	}
	commitSeq, err := tx.writer.recordWriter.AppendTxCommit(ctx, commit)
	if err != nil {
		return 0, err
	}
	for i := range tx.changes {
		tx.changes[i].Seq = commitSeq
	}
	return commitSeq, nil
}

// Changes returns all row changes made during this transaction with updated commit sequence numbers.
func (tx *Tx) Changes() []Change {
	return tx.changes
}
