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

	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"
)

type snapshotReaderOptions struct {
	acceptUnversioned bool
}

// SnapshotReaderOption configures a SnapshotReader.
type SnapshotReaderOption func(*snapshotReaderOptions)

// WithReaderAcceptUnversioned configures whether unversioned snapshot SSTables are accepted.
func WithReaderAcceptUnversioned(accept bool) SnapshotReaderOption {
	return func(o *snapshotReaderOptions) {
		o.acceptUnversioned = accept
	}
}

// SnapshotReader provides standalone reading of an SSTable snapshot file without requiring a running Pebble LSM engine.
type SnapshotReader struct {
	reader            *sstable.Reader
	streamID          string
	position          uint64
	registry          *record.Registry
	acceptUnversioned bool
}

func defaultReaderOptions() sstable.ReaderOptions {
	fp := bloom.FilterPolicy(10)
	return sstable.ReaderOptions{
		Filters: map[string]sstable.FilterPolicy{
			fp.Name(): fp,
		},
	}
}

// OpenSnapshotFile opens an SSTable snapshot from a file path using a standalone table reader.
func OpenSnapshotFile(path string, opts ...SnapshotReaderOption) (*SnapshotReader, error) {
	var o snapshotReaderOptions
	for _, opt := range opts {
		opt(&o)
	}

	rf, err := vfs.Default.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open snapshot file %q: %w", path, err)
	}

	readable, err := sstable.NewSimpleReadable(rf)
	if err != nil {
		errutil.CloseAndLog(rf, "file "+path)
		return nil, fmt.Errorf("failed to initialize readable for %q: %w", path, err)
	}

	r, err := sstable.NewReader(context.Background(), readable, defaultReaderOptions())
	if err != nil {
		errutil.CloseAndLog(readable, "readable for "+path)
		return nil, fmt.Errorf("failed to create sstable reader for %q: %w", path, err)
	}

	return newSnapshotReader(r, o.acceptUnversioned)
}

// OpenSnapshotBytes opens an SSTable snapshot directly from an in-memory byte buffer.
func OpenSnapshotBytes(buf []byte, opts ...SnapshotReaderOption) (*SnapshotReader, error) {
	var o snapshotReaderOptions
	for _, opt := range opts {
		opt(&o)
	}

	r, err := sstable.NewMemReader(buf, defaultReaderOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to open in-memory sstable reader: %w", err)
	}
	return newSnapshotReader(r, o.acceptUnversioned)
}

func newSnapshotReader(r *sstable.Reader, acceptUnversioned bool) (*SnapshotReader, error) {
	sr := &SnapshotReader{
		reader:            r,
		registry:          record.NewRegistry(),
		acceptUnversioned: acceptUnversioned,
	}

	iter, err := r.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		errutil.CloseAndLog(r, "sstable reader")
		return nil, fmt.Errorf("failed to create metadata iterator: %w", err)
	}
	defer func() {
		errutil.CloseAndLog(iter, "metadata iterator")
	}()

	// Read KeyFormatVersion
	if kv := iter.SeekGE(KeyFormatVersion, 0); kv != nil && bytes.Equal(kv.K.UserKey, KeyFormatVersion) {
		val, _, err := kv.Value(nil)
		if err != nil {
			errutil.CloseAndLog(r, "sstable reader")
			return nil, fmt.Errorf("failed to read format_version value from snapshot: %w", err)
		}
		ver, err := DecodeFormatVersion(val)
		if err != nil {
			errutil.CloseAndLog(r, "sstable reader")
			return nil, fmt.Errorf("failed to decode format_version from snapshot: %w", err)
		}
		if ver > SnapshotFormatVersion {
			errutil.CloseAndLog(r, "sstable reader")
			return nil, fmt.Errorf("%w: version %d > current %d", ErrUnsupportedSnapshotVersion, ver, SnapshotFormatVersion)
		}
	} else {
		if !acceptUnversioned {
			errutil.CloseAndLog(r, "sstable reader")
			return nil, ErrUnversionedSnapshot
		}
		klog.Warning("accepting unversioned table snapshot (--accept-unversioned-segments)")
	}

	// Read KeyPosition
	if kv := iter.SeekGE(KeyPosition, 0); kv != nil && bytes.Equal(kv.K.UserKey, KeyPosition) {
		val, _, err := kv.Value(nil)
		if err != nil {
			errutil.CloseAndLog(r, "sstable reader")
			return nil, fmt.Errorf("failed to read position value from snapshot: %w", err)
		}
		pos, err := DecodePosition(val)
		if err != nil {
			errutil.CloseAndLog(r, "sstable reader")
			return nil, fmt.Errorf("failed to decode position from snapshot: %w", err)
		}
		sr.position = pos
	}

	// Read KeyStreamID
	if kv := iter.SeekGE(KeyStreamID, 0); kv != nil && bytes.Equal(kv.K.UserKey, KeyStreamID) {
		val, _, err := kv.Value(nil)
		if err != nil {
			errutil.CloseAndLog(r, "sstable reader")
			return nil, fmt.Errorf("failed to read stream ID value from snapshot: %w", err)
		}
		sr.streamID = string(val)
	}

	// Read KeyRegistry
	if kv := iter.SeekGE(KeyRegistry, 0); kv != nil && bytes.Equal(kv.K.UserKey, KeyRegistry) {
		val, _, err := kv.Value(nil)
		if err != nil {
			errutil.CloseAndLog(r, "sstable reader")
			return nil, fmt.Errorf("failed to read registry value from snapshot: %w", err)
		}
		var st sdsv1.Registry
		if err := proto.Unmarshal(val, &st); err != nil {
			errutil.CloseAndLog(r, "sstable reader")
			return nil, fmt.Errorf("failed to unmarshal registry from snapshot: %w", err)
		}
		if err := sr.registry.Import(&st); err != nil {
			errutil.CloseAndLog(r, "sstable reader")
			return nil, fmt.Errorf("failed to import registry: %w", err)
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
	defer func() {
		errutil.CloseAndLog(iter, "snapshot get iterator")
	}()

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
		defer func() {
			errutil.CloseAndLog(iter, "snapshot scan iterator")
		}()

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
