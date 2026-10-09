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
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"google.golang.org/protobuf/proto"
)

const (
	// Format is the identifier for this index engine registered with sds.RegisterIndexFactory.
	Format = "table"

	// SnapshotTableFormat is the pinned, RocksDB-compatible block-based table format
	// version used for published snapshot files so that independent readers can open them.
	SnapshotTableFormat = sstable.TableFormatPebblev1
)

var (
	// ErrTypeNotRegistered is returned when a change is encountered for an unregistered type ID.
	ErrTypeNotRegistered = errors.New("type not registered")

	_ sds.LocalIndex      = (*DB)(nil)
	_ sds.Snapshotter     = (*DB)(nil)
	_ sds.ErrorClassifier = (*DB)(nil)
	_ sds.IndexFactory    = (*Factory)(nil)
)

type quietLogger struct{}

func (quietLogger) Infof(format string, args ...any)  {}
func (quietLogger) Errorf(format string, args ...any) {}
func (quietLogger) Fatalf(format string, args ...any) { panic(fmt.Sprintf(format, args...)) }

// DB implements sds.LocalIndex and sds.Snapshotter backed by Pebble LSM.
type DB struct {
	mu           sync.RWMutex
	db           *pebble.DB
	path         string
	streamID     string
	position     uint64
	registry     *record.Registry
	changeReader *sds.ChangeReader
	writeOpts    *pebble.WriteOptions
	opts         options
}

type options struct {
	streamID     string
	disableWAL   bool
	cacheSize    int64
	cache        *pebble.Cache
	memTableSize uint64
	sync         bool
	decoderOpts  []record.DecoderOption
	pebbleOpts   *pebble.Options
}

// Option configures table DB behavior.
type Option func(*options)

// WithStreamID configures the stream ID associated with the table DB.
func WithStreamID(streamID string) Option {
	return func(o *options) {
		o.streamID = streamID
	}
}

// WithDisableWAL configures whether Pebble's internal write-ahead log is disabled.
// When disabled, writes bypass Pebble's WAL; durability and crash-recovery rely
// on the structured-data stream replaying from the last applied position.
func WithDisableWAL(disable bool) Option {
	return func(o *options) {
		o.disableWAL = disable
	}
}

// WithCacheSize sets the Pebble block cache size in bytes.
func WithCacheSize(bytes int64) Option {
	return func(o *options) {
		o.cacheSize = bytes
	}
}

// WithCache sets a shared Pebble block cache.
func WithCache(cache *pebble.Cache) Option {
	return func(o *options) {
		o.cache = cache
	}
}

// WithMemTableSize configures the target steady-state memtable size in bytes.
func WithMemTableSize(bytes uint64) Option {
	return func(o *options) {
		o.memTableSize = bytes
	}
}

// WithSync configures whether writes sync to disk synchronously on every batch commit.
func WithSync(sync bool) Option {
	return func(o *options) {
		o.sync = sync
	}
}

// WithDecoderOptions passes decoder options for the change reader.
func WithDecoderOptions(opts ...record.DecoderOption) Option {
	return func(o *options) {
		o.decoderOpts = append(o.decoderOpts, opts...)
	}
}

// WithPebbleOptions supplies raw underlying Pebble options.
func WithPebbleOptions(opts *pebble.Options) Option {
	return func(o *options) {
		o.pebbleOpts = opts
	}
}

// Open opens or creates a table DB in the specified directory.
func Open(ctx context.Context, dir string, opts ...Option) (*DB, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %q: %w", dir, err)
	}

	var pOpts *pebble.Options
	if o.pebbleOpts != nil {
		pOpts = o.pebbleOpts.Clone()
	} else {
		pOpts = &pebble.Options{}
	}

	if pOpts.Logger == nil {
		pOpts.Logger = quietLogger{}
	}
	if o.cache != nil {
		pOpts.Cache = o.cache
	} else if o.cacheSize > 0 {
		pOpts.CacheSize = o.cacheSize
	}
	if o.memTableSize > 0 {
		pOpts.MemTableSize = o.memTableSize
	}
	pOpts.DisableWAL = o.disableWAL

	pdb, err := pebble.Open(dir, pOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db at %q: %w", dir, err)
	}

	reg := record.NewRegistry()
	decOpts := append([]record.DecoderOption{record.WithDecoderRegistry(reg)}, o.decoderOpts...)
	d := &DB{
		db:           pdb,
		path:         dir,
		streamID:     o.streamID,
		registry:     reg,
		changeReader: sds.NewChangeReader(decOpts...),
		opts:         o,
		writeOpts:    pebble.NoSync,
	}
	if o.sync {
		d.writeOpts = pebble.Sync
	}

	if err := d.loadMetadataLocked(); err != nil {
		_ = pdb.Close()
		return nil, err
	}

	return d, nil
}

func (d *DB) loadMetadataLocked() error {
	// 1. Stream ID
	valSID, closer, err := d.db.Get(KeyStreamID)
	if err == nil {
		d.streamID = string(valSID)
		_ = closer.Close()
	} else if errors.Is(err, pebble.ErrNotFound) {
		if d.streamID != "" {
			if err := d.db.Set(KeyStreamID, []byte(d.streamID), d.writeOpts); err != nil {
				return fmt.Errorf("failed to persist stream ID: %w", err)
			}
		}
	} else {
		return fmt.Errorf("failed to read stream ID from db: %w", err)
	}

	// 2. Position
	valPos, closer, err := d.db.Get(KeyPosition)
	if err == nil {
		pos, err := DecodePosition(valPos)
		_ = closer.Close()
		if err != nil {
			return fmt.Errorf("failed to decode position from db: %w", err)
		}
		d.position = pos
	} else if errors.Is(err, pebble.ErrNotFound) {
		d.position = 0
		if err := d.db.Set(KeyPosition, EncodePosition(0), d.writeOpts); err != nil {
			return fmt.Errorf("failed to persist initial position: %w", err)
		}
	} else {
		return fmt.Errorf("failed to read position from db: %w", err)
	}

	// 3. Registry
	valReg, closer, err := d.db.Get(KeyRegistry)
	if err == nil {
		var regExport sdsv1.Registry
		err := proto.Unmarshal(valReg, &regExport)
		_ = closer.Close()
		if err != nil {
			return fmt.Errorf("failed to unmarshal registry from db: %w", err)
		}
		if err := d.registry.Import(&regExport); err != nil {
			return fmt.Errorf("failed to import registry from db: %w", err)
		}
	} else if errors.Is(err, pebble.ErrNotFound) {
		regBytes, err := proto.Marshal(d.registry.Export())
		if err != nil {
			return fmt.Errorf("failed to marshal empty registry: %w", err)
		}
		if err := d.db.Set(KeyRegistry, regBytes, d.writeOpts); err != nil {
			return fmt.Errorf("failed to persist initial registry: %w", err)
		}
	} else {
		return fmt.Errorf("failed to read registry from db: %w", err)
	}

	return nil
}

// Position returns the stream sequence position of the last applied change.
func (d *DB) Position() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.position
}

// StreamID returns the stream ID associated with this table DB.
func (d *DB) StreamID() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.streamID
}

// Path returns the on-disk directory of this table DB.
func (d *DB) Path() string {
	return d.path
}

// Registry returns the in-memory type registry.
func (d *DB) Registry() *record.Registry {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.registry
}

// Pebble returns the underlying Pebble DB instance.
func (d *DB) Pebble() *pebble.DB {
	return d.db
}

// ChangeReader returns the change reader.
func (d *DB) ChangeReader() *sds.ChangeReader {
	return d.changeReader
}

// SyncRegistry registers type definitions in force with the table DB and persists them.
func (d *DB) SyncRegistry(ctx context.Context, reg *record.Registry) error {
	if reg == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	exported := reg.Export()
	if err := d.registry.Import(exported); err != nil {
		return fmt.Errorf("failed to import registry: %w", err)
	}

	regBytes, err := proto.Marshal(d.registry.Export())
	if err != nil {
		return fmt.Errorf("failed to marshal registry: %w", err)
	}
	if err := d.db.Set(KeyRegistry, regBytes, d.writeOpts); err != nil {
		return fmt.Errorf("failed to persist registry: %w", err)
	}
	return nil
}

// RegisterType registers a protobuf message type with primary key fields and persists the updated registry.
func (d *DB) RegisterType(ctx context.Context, msg proto.Message, keyFields ...int32) (*sdsv1.TypeDefinition, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	def, err := d.registry.RegisterMessage(msg, keyFields...)
	if err != nil {
		return nil, err
	}

	regBytes, err := proto.Marshal(d.registry.Export())
	if err != nil {
		return nil, fmt.Errorf("failed to marshal registry: %w", err)
	}
	if err := d.db.Set(KeyRegistry, regBytes, d.writeOpts); err != nil {
		return nil, fmt.Errorf("failed to persist registry: %w", err)
	}
	return def, nil
}

// Apply atomically applies a single row change.
func (d *DB) Apply(ctx context.Context, change sds.Change) error {
	return d.ApplyBatch(ctx, []sds.Change{change})
}

// ApplyBatch atomically applies a slice of committed row changes in a single Pebble batch,
// advancing Position in the same commit.
func (d *DB) ApplyBatch(ctx context.Context, changes []sds.Change) error {
	if len(changes) == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	batch := d.db.NewBatch()
	defer batch.Close()

	maxSeq := d.position

	for _, ch := range changes {
		typeID := ch.TypeID
		if typeID == 0 && ch.TypeName != "" {
			if def, _, ok := d.registry.LookupByName(ch.TypeName); ok {
				typeID = def.GetId()
			}
		}
		if typeID == 0 {
			return fmt.Errorf("%w: unknown type %q (ID %d)", ErrTypeNotRegistered, ch.TypeName, ch.TypeID)
		}

		rawKey := ch.RawKey
		if len(rawKey) == 0 && !ch.Key.IsZero() {
			rawKey = ch.Key.Bytes()
		}
		if len(rawKey) == 0 {
			return fmt.Errorf("missing primary key for change on type ID %d", typeID)
		}

		entryKey := EncodeEntryKey(typeID, rawKey)

		switch ch.Op {
		case sds.OpCreate, sds.OpUpdate:
			if err := batch.Set(entryKey, ch.RawVal, nil); err != nil {
				return fmt.Errorf("failed to set entry in batch: %w", err)
			}
		case sds.OpDelete:
			if err := batch.Delete(entryKey, nil); err != nil {
				return fmt.Errorf("failed to delete entry in batch: %w", err)
			}
		default:
			return fmt.Errorf("unsupported change op %v", ch.Op)
		}

		if ch.Seq > maxSeq {
			maxSeq = ch.Seq
		}
	}

	if maxSeq > d.position {
		posBytes := EncodePosition(maxSeq)
		if err := batch.Set(KeyPosition, posBytes, nil); err != nil {
			return fmt.Errorf("failed to set position in batch: %w", err)
		}
	}

	if err := batch.Commit(d.writeOpts); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	if maxSeq > d.position {
		d.position = maxSeq
	}
	return nil
}

// Feed decodes and applies a stream record at seq, updating registry and state.
func (d *DB) Feed(ctx context.Context, seq uint64, payload []byte) ([]sds.Change, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	rec, err := d.changeReader.Decoder().Decode(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to decode frame at seq %d: %w", seq, err)
	}

	if rec.TypeID == record.TypeIDTypeDefinition {
		def, ok := rec.Message.(*sdsv1.TypeDefinition)
		if !ok {
			return nil, fmt.Errorf("expected TypeDefinition message at seq %d, got %T", seq, rec.Message)
		}
		if err := d.registry.Register(def); err != nil {
			return nil, fmt.Errorf("failed to register TypeDefinition at seq %d: %w", seq, err)
		}
		regBytes, err := proto.Marshal(d.registry.Export())
		if err != nil {
			return nil, fmt.Errorf("failed to marshal registry at seq %d: %w", seq, err)
		}
		if err := d.db.Set(KeyRegistry, regBytes, d.writeOpts); err != nil {
			return nil, fmt.Errorf("failed to persist registry at seq %d: %w", seq, err)
		}
	}

	changes, err := d.changeReader.Feed(seq, payload)
	if err != nil {
		return nil, err
	}
	if len(changes) == 0 {
		return nil, nil
	}

	batch := d.db.NewBatch()
	defer batch.Close()

	maxSeq := d.position
	for _, ch := range changes {
		typeID := ch.TypeID
		if typeID == 0 && ch.TypeName != "" {
			if def, _, ok := d.registry.LookupByName(ch.TypeName); ok {
				typeID = def.GetId()
			}
		}
		if typeID == 0 {
			return nil, fmt.Errorf("%w: unknown type %q (ID %d)", ErrTypeNotRegistered, ch.TypeName, ch.TypeID)
		}

		rawKey := ch.RawKey
		if len(rawKey) == 0 && !ch.Key.IsZero() {
			rawKey = ch.Key.Bytes()
		}
		entryKey := EncodeEntryKey(typeID, rawKey)

		switch ch.Op {
		case sds.OpCreate, sds.OpUpdate:
			if err := batch.Set(entryKey, ch.RawVal, nil); err != nil {
				return nil, fmt.Errorf("failed to set entry in feed batch: %w", err)
			}
		case sds.OpDelete:
			if err := batch.Delete(entryKey, nil); err != nil {
				return nil, fmt.Errorf("failed to delete entry in feed batch: %w", err)
			}
		default:
			return nil, fmt.Errorf("unsupported change op %v", ch.Op)
		}
		if ch.Seq > maxSeq {
			maxSeq = ch.Seq
		}
	}

	if maxSeq > d.position {
		posBytes := EncodePosition(maxSeq)
		if err := batch.Set(KeyPosition, posBytes, nil); err != nil {
			return nil, fmt.Errorf("failed to set position in feed batch: %w", err)
		}
	}

	if err := batch.Commit(d.writeOpts); err != nil {
		return nil, fmt.Errorf("failed to commit feed batch: %w", err)
	}

	if maxSeq > d.position {
		d.position = maxSeq
	}
	return changes, nil
}

// Get retrieves a merged proto row by table type name and primary key.
func (d *DB) Get(ctx context.Context, typeName string, key sds.Key) (proto.Message, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	def, _, ok := d.registry.LookupByName(typeName)
	if !ok {
		return nil, false, fmt.Errorf("%w: type %q", ErrTypeNotRegistered, typeName)
	}

	msgType, err := d.registry.ResolveMessageType(def.GetId())
	if err != nil {
		return nil, false, fmt.Errorf("failed to resolve message type for %q: %w", typeName, err)
	}

	entryKey := EncodeEntryKey(def.GetId(), key.Bytes())
	valBytes, closer, err := d.db.Get(entryKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer closer.Close()

	target := msgType.New().Interface()
	if err := sds.MergeKeyAndNonKey(target, key.Bytes(), valBytes); err != nil {
		return nil, false, fmt.Errorf("failed to merge proto key and value: %w", err)
	}

	return target, true, nil
}

// Scan yields merged proto rows matching keyPrefix in canonical key-byte order.
func (d *DB) Scan(ctx context.Context, typeName string, keyPrefix []byte) iter.Seq2[proto.Message, error] {
	return func(yield func(proto.Message, error) bool) {
		d.mu.RLock()
		def, _, ok := d.registry.LookupByName(typeName)
		if !ok {
			d.mu.RUnlock()
			yield(nil, fmt.Errorf("%w: type %q", ErrTypeNotRegistered, typeName))
			return
		}

		msgType, err := d.registry.ResolveMessageType(def.GetId())
		if err != nil {
			d.mu.RUnlock()
			yield(nil, fmt.Errorf("failed to resolve message type for %q: %w", typeName, err))
			return
		}

		tablePrefix := EncodeTypePrefix(def.GetId())
		var scanPrefix []byte
		if len(keyPrefix) == 0 {
			scanPrefix = tablePrefix
		} else {
			scanPrefix = make([]byte, len(tablePrefix)+len(keyPrefix))
			copy(scanPrefix, tablePrefix)
			copy(scanPrefix[len(tablePrefix):], keyPrefix)
		}

		upperBound := prefixLimit(scanPrefix)
		iterOpts := &pebble.IterOptions{
			LowerBound: scanPrefix,
			UpperBound: upperBound,
		}

		it, err := d.db.NewIter(iterOpts)
		if err != nil {
			d.mu.RUnlock()
			yield(nil, fmt.Errorf("failed to create iterator: %w", err))
			return
		}
		defer func() {
			_ = it.Close()
			d.mu.RUnlock()
		}()

		for it.SeekGE(scanPrefix); it.Valid(); it.Next() {
			k := it.Key()
			if !bytes.HasPrefix(k, scanPrefix) {
				break
			}

			rawKey := k[len(tablePrefix):]
			rawVal := it.Value()

			target := msgType.New().Interface()
			if err := sds.MergeKeyAndNonKey(target, rawKey, rawVal); err != nil {
				yield(nil, fmt.Errorf("failed to merge proto key and value: %w", err))
				return
			}

			if !yield(target, nil) {
				return
			}
		}

		if err := it.Error(); err != nil {
			yield(nil, err)
		}
	}
}

// ScanLimit returns up to limit rows matching keyPrefix.
func (d *DB) ScanLimit(ctx context.Context, typeName string, keyPrefix []byte, limit int) ([]proto.Message, error) {
	var results []proto.Message
	for msg, err := range d.Scan(ctx, typeName, keyPrefix) {
		if err != nil {
			return nil, err
		}
		results = append(results, msg)
		if limit > 0 && len(results) >= limit {
			break
		}
	}
	return results, nil
}

// ScanSlice returns all rows matching keyPrefix.
func (d *DB) ScanSlice(ctx context.Context, typeName string, keyPrefix []byte) ([]proto.Message, error) {
	return d.ScanLimit(ctx, typeName, keyPrefix, 0)
}

// Rows returns all rows for a table.
func (d *DB) Rows(ctx context.Context, typeName string) ([]proto.Message, error) {
	return d.ScanSlice(ctx, typeName, nil)
}

// Count returns the number of rows in a table.
func (d *DB) Count(ctx context.Context, typeName string) (int, error) {
	rows, err := d.Rows(ctx, typeName)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

// Tables returns the list of registered table names.
func (d *DB) Tables() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var names []string
	for _, t := range d.registry.Export().GetTypes() {
		names = append(names, t.GetName())
	}
	return names
}

// IsUnrecoverable reports whether an error represents data corruption or permanent media loss.
func (d *DB) IsUnrecoverable(err error) bool {
	if err == nil {
		return false
	}
	return pebble.IsCorruptionError(err) || errors.Is(err, pebble.ErrCorruption)
}

// Close closes the underlying Pebble DB.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.db != nil {
		err := d.db.Close()
		d.db = nil
		return err
	}
	return nil
}

// PublishSnapshot creates an atomic snapshot of the DB and uploads it to object storage.
func (d *DB) PublishSnapshot(ctx context.Context, backend objectstore.Backend) (string, uint64, error) {
	return PublishSnapshot(ctx, d, backend, os.TempDir())
}

// Factory implements sds.IndexFactory for table.
type Factory struct {
	opts []Option
}

// NewFactory creates a Factory with default options.
func NewFactory(opts ...Option) *Factory {
	return &Factory{opts: opts}
}

// Format returns "table".
func (f *Factory) Format() string {
	return Format
}

// OpenLocal opens an existing table index in dir if present.
func (f *Factory) OpenLocal(ctx context.Context, streamID string, dir string) (sds.LocalIndex, bool, error) {
	if dir == "" {
		return nil, false, nil
	}
	dbDir := filepath.Join(dir, "table.db")
	if _, err := os.Stat(dbDir); err != nil {
		return nil, false, nil
	}
	desc, err := pebble.Peek(dbDir, vfs.Default)
	if err != nil || desc == nil {
		return nil, false, nil
	}
	allOpts := append([]Option{WithStreamID(streamID)}, f.opts...)
	db, err := Open(ctx, dbDir, allOpts...)
	if err != nil {
		return nil, false, err
	}
	return db, true, nil
}

// FindLatestSnapshot finds the latest table snapshot in object storage at or before maxPos.
func (f *Factory) FindLatestSnapshot(ctx context.Context, backend objectstore.Backend, streamID string, maxPos uint64) (string, uint64, error) {
	return FindLatestSnapshot(ctx, backend, streamID, maxPos)
}

// RestoreSnapshot restores a table snapshot from object storage into dir.
func (f *Factory) RestoreSnapshot(ctx context.Context, backend objectstore.Backend, streamID string, key string, dir string) (sds.LocalIndex, uint64, error) {
	allOpts := append([]Option{WithStreamID(streamID)}, f.opts...)
	return RestoreSnapshotKey(ctx, backend, streamID, key, dir, allOpts...)
}

// NewEmpty creates a new empty local table index in dir.
func (f *Factory) NewEmpty(ctx context.Context, streamID string, dir string) (sds.LocalIndex, error) {
	dbDir := filepath.Join(dir, "table.db")
	_ = os.RemoveAll(dbDir)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %q: %w", dbDir, err)
	}
	allOpts := append([]Option{WithStreamID(streamID)}, f.opts...)
	return Open(ctx, dbDir, allOpts...)
}

func init() {
	sds.RegisterIndexFactory(&Factory{})
}
