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

package table

import (
	"bytes"
	"context"
	"fmt"
	"iter"

	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"google.golang.org/protobuf/proto"
)

// SnapshotReader provides standalone reading of an SSTable snapshot file without requiring a running Pebble LSM engine.
type SnapshotReader struct {
	reader   *sstable.Reader
	streamID string
	position uint64
	registry *record.Registry
}

// OpenSnapshotFile opens an SSTable snapshot from a file path using a standalone table reader.
func OpenSnapshotFile(path string) (*SnapshotReader, error) {
	rf, err := vfs.Default.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open snapshot file %q: %w", path, err)
	}

	readable, err := sstable.NewSimpleReadable(rf)
	if err != nil {
		_ = rf.Close()
		return nil, fmt.Errorf("failed to initialize readable for %q: %w", path, err)
	}

	r, err := sstable.NewReader(context.Background(), readable, sstable.ReaderOptions{})
	if err != nil {
		_ = readable.Close()
		return nil, fmt.Errorf("failed to create sstable reader for %q: %w", path, err)
	}

	return newSnapshotReader(r)
}

// OpenSnapshotBytes opens an SSTable snapshot directly from an in-memory byte buffer.
func OpenSnapshotBytes(buf []byte) (*SnapshotReader, error) {
	r, err := sstable.NewMemReader(buf, sstable.ReaderOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to open in-memory sstable reader: %w", err)
	}
	return newSnapshotReader(r)
}

func newSnapshotReader(r *sstable.Reader) (*SnapshotReader, error) {
	sr := &SnapshotReader{
		reader:   r,
		registry: record.NewRegistry(),
	}

	iter, err := r.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		_ = r.Close()
		return nil, fmt.Errorf("failed to create metadata iterator: %w", err)
	}
	defer iter.Close()

	// Read KeyPosition
	if kv := iter.SeekGE(KeyPosition, 0); kv != nil && bytes.Equal(kv.K.UserKey, KeyPosition) {
		val, _, err := kv.Value(nil)
		if err == nil {
			pos, err := DecodePosition(val)
			if err == nil {
				sr.position = pos
			}
		}
	}

	// Read KeyStreamID
	if kv := iter.SeekGE(KeyStreamID, 0); kv != nil && bytes.Equal(kv.K.UserKey, KeyStreamID) {
		val, _, err := kv.Value(nil)
		if err == nil {
			sr.streamID = string(val)
		}
	}

	// Read KeyRegistry
	if kv := iter.SeekGE(KeyRegistry, 0); kv != nil && bytes.Equal(kv.K.UserKey, KeyRegistry) {
		val, _, err := kv.Value(nil)
		if err == nil {
			var st sdsv1.Registry
			if err := proto.Unmarshal(val, &st); err == nil {
				_ = sr.registry.Import(&st)
			}
		}
	}

	return sr, nil
}

// Position returns the applied position recorded in the snapshot.
func (sr *SnapshotReader) Position() uint64 {
	return sr.position
}

// StreamID returns the stream ID recorded in the snapshot.
func (sr *SnapshotReader) StreamID() string {
	return sr.streamID
}

// Registry returns the decoded type registry from the snapshot.
func (sr *SnapshotReader) Registry() *record.Registry {
	return sr.registry
}

// Get looks up a row by table type name and primary key bytes.
func (sr *SnapshotReader) Get(typeName string, keyBytes []byte) (proto.Message, bool, error) {
	def, _, ok := sr.registry.LookupByName(typeName)
	if !ok {
		return nil, false, fmt.Errorf("%w: type %q", ErrTypeNotRegistered, typeName)
	}

	msgType, err := sr.registry.ResolveMessageType(def.GetId())
	if err != nil {
		return nil, false, fmt.Errorf("failed to resolve message type for %q: %w", typeName, err)
	}

	entryKey := EncodeEntryKey(def.GetId(), keyBytes)
	iter, err := sr.reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	kv := iter.SeekGE(entryKey, 0)
	if kv == nil || !bytes.Equal(kv.K.UserKey, entryKey) {
		return nil, false, nil
	}

	val, _, err := kv.Value(nil)
	if err != nil {
		return nil, false, fmt.Errorf("failed to read value: %w", err)
	}

	target := msgType.New().Interface()
	if err := sds.MergeKeyAndNonKey(target, keyBytes, val); err != nil {
		return nil, false, fmt.Errorf("failed to merge proto key and value: %w", err)
	}

	return target, true, nil
}

// Scan yields rows matching keyPrefix in canonical key-byte order.
func (sr *SnapshotReader) Scan(typeName string, keyPrefix []byte) iter.Seq2[proto.Message, error] {
	return func(yield func(proto.Message, error) bool) {
		def, _, ok := sr.registry.LookupByName(typeName)
		if !ok {
			yield(nil, fmt.Errorf("%w: type %q", ErrTypeNotRegistered, typeName))
			return
		}

		msgType, err := sr.registry.ResolveMessageType(def.GetId())
		if err != nil {
			yield(nil, fmt.Errorf("failed to resolve message type for %q: %w", typeName, err))
			return
		}

		tablePrefix := EncodeTypePrefix(def.GetId())
		scanPrefix := make([]byte, len(tablePrefix)+len(keyPrefix))
		copy(scanPrefix, tablePrefix)
		copy(scanPrefix[len(tablePrefix):], keyPrefix)

		upperBound := prefixLimit(scanPrefix)
		iter, err := sr.reader.NewIter(sstable.NoTransforms, scanPrefix, upperBound, sstable.AssertNoBlobHandles)
		if err != nil {
			yield(nil, fmt.Errorf("failed to create iterator: %w", err))
			return
		}
		defer iter.Close()

		for kv := iter.SeekGE(scanPrefix, 0); kv != nil; kv = iter.Next() {
			k := kv.K.UserKey
			if !bytes.HasPrefix(k, scanPrefix) {
				break
			}

			rawKey := k[len(tablePrefix):]
			rawVal, _, err := kv.Value(nil)
			if err != nil {
				yield(nil, fmt.Errorf("failed to read value: %w", err))
				return
			}

			target := msgType.New().Interface()
			if err := sds.MergeKeyAndNonKey(target, rawKey, rawVal); err != nil {
				yield(nil, fmt.Errorf("failed to merge proto key and value: %w", err))
				return
			}

			if !yield(target, nil) {
				return
			}
		}

		if err := iter.Error(); err != nil {
			yield(nil, err)
		}
	}
}

// ScanSlice returns all rows for typeName matching keyPrefix.
func (sr *SnapshotReader) ScanSlice(typeName string, keyPrefix []byte) ([]proto.Message, error) {
	var results []proto.Message
	for msg, err := range sr.Scan(typeName, keyPrefix) {
		if err != nil {
			return nil, err
		}
		results = append(results, msg)
	}
	return results, nil
}

// Close closes the underlying sstable reader.
func (sr *SnapshotReader) Close() error {
	if sr.reader != nil {
		err := sr.reader.Close()
		sr.reader = nil
		return err
	}
	return nil
}
