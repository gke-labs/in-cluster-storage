// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package sds

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"k8s.io/klog/v2"
)

const (
	// StreamFormatVersion is the current stream format version.
	StreamFormatVersion uint32 = 1
)

var (
	// ErrMissingStreamHeader is returned when the first record of a stream is not a StreamHeader.
	ErrMissingStreamHeader = errors.New("missing stream header record as first stream record")

	// ErrUnsupportedStreamVersion is returned when a stream format version is greater than StreamFormatVersion.
	ErrUnsupportedStreamVersion = errors.New("unsupported stream format version")
)

// ChangeReaderOption configures a ChangeReader.
type ChangeReaderOption func(*ChangeReader)

// WithAcceptUnversioned configures whether ChangeReader tolerates the absence of a StreamHeader on the first record.
func WithAcceptUnversioned(accept bool) ChangeReaderOption {
	return func(r *ChangeReader) {
		r.acceptUnversioned = accept
	}
}

// ChangeReader decodes stream payloads in order, groups multi-record transactions,
// and yields committed changes in stream order.
type ChangeReader struct {
	mu                   sync.Mutex
	decoder              *record.Decoder
	pendingTx            map[uint64][]Change
	safeSnapshotPosition uint64
	acceptUnversioned    bool
	headerSeen           bool
	firstRecordSeen      bool
	streamHeader         *sdsv1.StreamHeader
}

// NewChangeReader creates a new ChangeReader.
func NewChangeReader(opts ...record.DecoderOption) *ChangeReader {
	return &ChangeReader{
		decoder:   record.NewDecoder(opts...),
		pendingTx: make(map[uint64][]Change),
	}
}

// NewChangeReaderWithOptions creates a ChangeReader with unversioned acceptance setting.
func NewChangeReaderWithOptions(acceptUnversioned bool, opts ...record.DecoderOption) *ChangeReader {
	r := NewChangeReader(opts...)
	r.acceptUnversioned = acceptUnversioned
	return r
}

// SetAcceptUnversioned configures whether unversioned streams are tolerated.
func (r *ChangeReader) SetAcceptUnversioned(accept bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.acceptUnversioned = accept
}

// StreamHeader returns the decoded StreamHeader, or nil if none.
func (r *ChangeReader) StreamHeader() *sdsv1.StreamHeader {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.streamHeader
}

// Decoder returns the underlying record.Decoder.
func (r *ChangeReader) Decoder() *record.Decoder {
	return r.decoder
}

// Registry returns the in-band type registry.
func (r *ChangeReader) Registry() *record.Registry {
	return r.decoder.Registry()
}

// SafeSnapshotPosition returns the latest stream sequence position at which
// no transaction was pending. Snapshots may only be taken at safe positions.
func (r *ChangeReader) SafeSnapshotPosition() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.safeSnapshotPosition
}

// HasPending reports whether any multi-record transactions are currently pending.
func (r *ChangeReader) HasPending() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pendingTx) > 0
}

// PendingTransactions returns a map of all currently pending transactions and their buffered changes.
func (r *ChangeReader) PendingTransactions() map[uint64][]Change {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make(map[uint64][]Change, len(r.pendingTx))
	for txID, changes := range r.pendingTx {
		cp := make([]Change, len(changes))
		copy(cp, changes)
		out[txID] = cp
	}
	return out
}

// DiscardPending discards all uncommitted pending transactions and returns the discarded changes.
func (r *ChangeReader) DiscardPending() []Change {
	r.mu.Lock()
	defer r.mu.Unlock()

	var discarded []Change
	for _, changes := range r.pendingTx {
		discarded = append(discarded, changes...)
	}
	sort.Slice(discarded, func(i, j int) bool {
		return discarded[i].Seq < discarded[j].Seq
	})

	r.pendingTx = make(map[uint64][]Change)
	return discarded
}

// Feed consumes a single stream payload at sequence number seq, maintains stream state,
// and returns any changes that were committed by this record in stream order.
func (r *ChangeReader) Feed(seq uint64, payload []byte) ([]Change, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rec, err := r.decoder.Decode(payload)
	if err != nil {
		return nil, err
	}

	if !r.firstRecordSeen {
		r.firstRecordSeen = true
		if seq == 1 {
			if rec.TypeID == record.TypeIDStreamHeader {
				r.headerSeen = true
			} else {
				if !r.acceptUnversioned {
					return nil, fmt.Errorf("%w: first record type is %d (use --accept-unversioned-segments to allow)", ErrMissingStreamHeader, rec.TypeID)
				}
				klog.Warningf("accepting stream without stream header record (first record type %d, --accept-unversioned-segments)", rec.TypeID)
			}
		}
	}

	switch rec.TypeID {
	case record.TypeIDStreamHeader:
		hdr, ok := rec.Message.(*sdsv1.StreamHeader)
		if !ok || hdr == nil {
			return nil, fmt.Errorf("expected *sdsv1.StreamHeader for TypeIDStreamHeader, got %T", rec.Message)
		}
		if hdr.GetFormatVersion() > StreamFormatVersion {
			return nil, fmt.Errorf("%w: version %d > current %d", ErrUnsupportedStreamVersion, hdr.GetFormatVersion(), StreamFormatVersion)
		}
		r.streamHeader = hdr
		if len(r.pendingTx) == 0 {
			r.safeSnapshotPosition = seq
		}
		return nil, nil

	case record.TypeIDTypeDefinition, record.TypeIDSnapshotPointer, record.TypeIDPadding:
		if len(r.pendingTx) == 0 {
			r.safeSnapshotPosition = seq
		}
		return nil, nil

	case record.TypeIDTxCommit:
		commit, ok := rec.Message.(*sdsv1.TxCommit)
		if !ok {
			return nil, fmt.Errorf("expected *sdsv1.TxCommit for TypeIDTxCommit, got %T", rec.Message)
		}

		committedChanges := r.pendingTx[commit.GetTxId()]
		delete(r.pendingTx, commit.GetTxId())

		if len(r.pendingTx) == 0 {
			r.safeSnapshotPosition = seq
		}
		return committedChanges, nil

	case record.TypeIDOpRecord:
		opRec, ok := rec.Message.(*sdsv1.OpRecord)
		if !ok || opRec == nil {
			return nil, fmt.Errorf("missing OpRecord for TypeIDOpRecord")
		}

		def, md, ok := r.decoder.Registry().LookupByID(opRec.GetTypeId())
		if !ok {
			return nil, fmt.Errorf("%w: %d", record.ErrTypeNotRegistered, opRec.GetTypeId())
		}

		if err := ValidateKeyBytes(md, def.GetKeyFields(), opRec.GetKey()); err != nil {
			return nil, err
		}

		msgType, err := r.decoder.Registry().ResolveMessageType(opRec.GetTypeId())
		if err != nil {
			return nil, err
		}

		key := NewKeyFromBytes(opRec.GetKey())

		rowMsg := msgType.New().Interface()
		if err := MergeKeyAndNonKey(rowMsg, opRec.GetKey(), opRec.GetValue()); err != nil {
			return nil, fmt.Errorf("failed to merge row msg: %w", err)
		}

		change := Change{
			Seq:      seq,
			TypeID:   opRec.GetTypeId(),
			TypeName: def.GetName(),
			Op:       opRec.GetOp(),
			TxID:     opRec.GetTxId(),
			Key:      key,
			RawKey:   opRec.GetKey(),
			RawVal:   opRec.GetValue(),
			Row:      rowMsg,
		}

		if opRec.GetTxId() == 0 {
			// Autocommit: yielded immediately
			if len(r.pendingTx) == 0 {
				r.safeSnapshotPosition = seq
			}
			return []Change{change}, nil
		}

		// Part of multi-record transaction: hold until TxCommit
		r.pendingTx[opRec.GetTxId()] = append(r.pendingTx[opRec.GetTxId()], change)
		return nil, nil

	default:
		// Bare application record (>= 16) if written directly
		if len(r.pendingTx) == 0 {
			r.safeSnapshotPosition = seq
		}
		return nil, nil
	}
}
