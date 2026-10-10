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

// Package view provides a generic in-memory caching and write-behind applier layer
// wrapping any sds.LocalIndex.
//
// Contract:
//   - Messages returned by Get and Scan are shared and must not be modified by callers.
//     Callers that need to change a message must clone it first (e.g., via CachedInode.mutate in objectfs).
//   - Messages passed to the view in changes (sds.Change.Row) must not be modified after they are recorded.
//     The overlay, the read cache, the applier, and LocalIndex implementations may keep and share them.
//   - LocalIndex implementations may return either freshly decoded messages (e.g., SQLite) or stored pointers (e.g., memtable).
//   - The before and after messages passed to StatsUpdateFunc are shared and read-only.
package view

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"google.golang.org/protobuf/proto"
)

// OverlayEntry represents a committed but unapplied row change held in memory.
type OverlayEntry struct {
	Op   sds.Op
	Row  proto.Message
	Seq  uint64
	Size int64
}

// StatsUpdateFunc computes updated stats from a transition between before and after.
// The before and after messages are shared and read-only; stats is modified in place.
type StatsUpdateFunc func(stats, before, after proto.Message) error

// RebuildFunc is called when the index has suffered a persistent or unrecoverable failure.
// It creates or restores a new LocalIndex, replays the stream from the restored position
// up to the current position, and returns the new index and its sequence position.
type RebuildFunc func(ctx context.Context) (sds.LocalIndex, uint64, error)

// Option configures a View.
type Option func(*View)

// WithMutationCheck enables debug assertion checking that proto rows are never mutated in place.
func WithMutationCheck() Option {
	return func(v *View) {
		v.mutationCheck = true
		if v.checker == nil {
			v.checker = newMutationChecker()
		}
	}
}

// WithoutMutationCheck explicitly disables debug mutation checking (e.g., for benchmarks).
func WithoutMutationCheck() Option {
	return func(v *View) {
		v.mutationCheck = false
		v.checker = nil
	}
}

// WithStats configures typed statistics tracking for the View.
func WithStats(initial proto.Message, update StatsUpdateFunc) Option {
	return func(v *View) {
		v.statsRow = proto.Clone(initial)
		v.statsUpdate = update
	}
}

// WithBatchSize sets the maximum batch size for the background applier.
func WithBatchSize(batchSize int) Option {
	return func(v *View) {
		v.batchSize = batchSize
	}
}

// WithRebuildFunc configures the index reconstruction function for automatic recovery.
func WithRebuildFunc(fn RebuildFunc) Option {
	return func(v *View) {
		v.rebuildFunc = fn
	}
}

// WithDegradedTimeout sets how long the view may remain degraded before triggering an automatic index rebuild.
func WithDegradedTimeout(d time.Duration) Option {
	return func(v *View) {
		v.degradedTimeout = d
	}
}

// WithErrorClassifier configures a custom error classifier function for unrecoverable errors.
func WithErrorClassifier(classifier func(error) bool) Option {
	return func(v *View) {
		v.errorClassifier = classifier
	}
}

// WithCacheLimits sets the maximum entry count and maximum bytes for the read cache.
func WithCacheLimits(capacity int, maxBytes int64) Option {
	return func(v *View) {
		v.cacheCapacity = capacity
		v.cacheMaxBytes = maxBytes
	}
}

// WithCacheDisabled enables or disables the read cache.
func WithCacheDisabled(disabled bool) Option {
	return func(v *View) {
		v.cacheDisabled = disabled
	}
}

// WithOverlayMaxBytes sets the maximum byte threshold for unapplied mutations before backpressure is applied.
func WithOverlayMaxBytes(maxBytes int64) Option {
	return func(v *View) {
		v.maxOverlayBytes = maxBytes
	}
}

// WithFaultHook sets a test fault injection hook for the background applier.
func WithFaultHook(hook func() error) Option {
	return func(v *View) {
		v.faultHook = hook
	}
}

// WithBatchAppliedHook configures a callback invoked after an applier batch is applied.
// It is called with the view unlocked, after the batch is visible to readers and before
// flush waiters are released; it must be cheap and must not call back into the view
// (e.g. only bumping a counter and scheduling background work).
func WithBatchAppliedHook(fn func(appliedPos uint64)) Option {
	return func(v *View) {
		v.batchAppliedHook = fn
	}
}

// WithRegistry sets the in-band type registry for the View.
func WithRegistry(reg *record.Registry) Option {
	return func(v *View) {
		v.reg = reg
	}
}

// View wraps an underlying sds.LocalIndex with an unapplied mutations overlay,
// an LRU read cache, and an asynchronous write-behind applier.
type View struct {
	mu      sync.RWMutex
	indexMu sync.Mutex
	index   sds.LocalIndex
	reg     *record.Registry

	cache           *LRUCache[CacheKey, *CachedRow]
	cacheDisabled   bool
	cacheCapacity   int
	cacheMaxBytes   int64
	maxOverlayBytes int64
	unappliedBytes  int64

	overlay map[CacheKey]OverlayEntry
	queue   []sds.Change

	applier          *applier
	appliedPos       uint64
	batchSize        int
	faultHook        func() error
	batchAppliedHook func(appliedPos uint64)

	statsRow    proto.Message
	statsUpdate StatsUpdateFunc
	loadErr     error

	rebuildFunc     RebuildFunc
	degradedTimeout time.Duration
	errorClassifier func(error) bool

	backpressureCond *sync.Cond
	flushCond        *sync.Cond
	closed           bool

	mutationCheck bool
	checker       *mutationChecker
}

// New creates and starts a new View wrapping index.
func New(index sds.LocalIndex, opts ...Option) *View {
	v := &View{
		index:           index,
		overlay:         make(map[CacheKey]OverlayEntry),
		batchSize:       defaultApplierBatchSize,
		degradedTimeout: 5 * time.Minute,
		cacheCapacity:   10000,
		cacheMaxBytes:   64 * 1024 * 1024,
		maxOverlayBytes: 64 * 1024 * 1024,
		mutationCheck:   testing.Testing(),
	}
	for _, opt := range opts {
		opt(v)
	}

	if v.mutationCheck && v.checker == nil {
		v.checker = newMutationChecker()
	}

	v.backpressureCond = sync.NewCond(&v.mu)
	v.flushCond = sync.NewCond(&v.mu)

	var onEvict func(key CacheKey, value *CachedRow)
	if v.mutationCheck {
		onEvict = func(key CacheKey, value *CachedRow) {
			if value != nil && value.Msg != nil {
				v.checkMessage(value.Msg)
			}
		}
	}

	v.cache = NewLRUCacheWithLimits[CacheKey, *CachedRow](
		v.cacheCapacity,
		v.cacheMaxBytes,
		CacheSizeFn,
		onEvict,
	)

	if index != nil {
		v.appliedPos = index.Position()
		if err := v.loadStatsLocked(context.Background()); err != nil {
			v.loadErr = err
		}
	}

	v.applier = newApplier(v, v.batchSize, v.faultHook)
	v.applier.start()

	return v
}

// Rebuild triggers an immediate rebuild of the underlying LocalIndex using the configured RebuildFunc.
func (v *View) Rebuild(ctx context.Context) error {
	if v.rebuildFunc == nil {
		return errors.New("no rebuild function configured")
	}
	newIndex, newPos, err := v.rebuildFunc(ctx)
	if err != nil {
		return err
	}
	v.indexMu.Lock()
	defer v.indexMu.Unlock()
	v.mu.Lock()
	oldIndex := v.index
	v.index = newIndex
	v.loadErr = nil
	if newPos > v.appliedPos {
		v.appliedPos = newPos
	}
	if newIndex != nil {
		if err := v.loadStatsLocked(ctx); err != nil {
			v.loadErr = err
		}
	}
	v.trimQueueAndOverlayLocked(newPos)
	if v.cache != nil && !v.cacheDisabled {
		v.cache.Clear()
	}
	if v.applier != nil {
		v.applier.isDegraded.Store(false)
		v.applier.consecutiveErr.Store(0)
		v.applier.degradedSince = time.Time{}
		v.applier.isRebuilding.Store(false)
	}
	if v.flushCond != nil {
		v.flushCond.Broadcast()
	}
	if v.backpressureCond != nil {
		v.backpressureCond.Broadcast()
	}
	v.mu.Unlock()

	if oldIndex != nil {
		_ = oldIndex.Close()
	}
	return nil
}

func (v *View) trimQueueAndOverlayLocked(newPos uint64) {
	idx := 0
	for idx < len(v.queue) && v.queue[idx].Seq <= newPos {
		idx++
	}
	if idx > 0 {
		for i := 0; i < idx; i++ {
			v.queue[i] = sds.Change{}
		}
		v.queue = v.queue[idx:]
		if len(v.queue) == 0 {
			v.queue = nil
		}
	}

	for ck, entry := range v.overlay {
		if entry.Seq <= newPos {
			if entry.Row != nil {
				v.checkMessage(entry.Row)
			}
			delete(v.overlay, ck)
			v.unappliedBytes -= entry.Size
			if v.unappliedBytes < 0 {
				v.unappliedBytes = 0
			}
		}
	}
}

// IsRebuilding reports whether the view is currently rebuilding its underlying local index.
func (v *View) IsRebuilding() bool {
	if v.applier != nil {
		return v.applier.isRebuilding.Load()
	}
	return false
}

// Index returns the underlying sds.LocalIndex.
func (v *View) Index() sds.LocalIndex {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.index
}

// SetIndex replaces the underlying index (e.g. on restore).
func (v *View) SetIndex(index sds.LocalIndex) error {
	v.indexMu.Lock()
	defer v.indexMu.Unlock()
	v.mu.Lock()
	defer v.mu.Unlock()
	v.index = index
	v.loadErr = nil
	if index != nil {
		v.appliedPos = index.Position()
		if err := v.loadStatsLocked(context.Background()); err != nil {
			v.loadErr = err
			return err
		}
	}
	return nil
}

// LoadStats loads or recomputes index statistics, returning any error encountered.
func (v *View) LoadStats(ctx context.Context) error {
	v.indexMu.Lock()
	defer v.indexMu.Unlock()
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.loadErr != nil {
		return v.loadErr
	}
	return v.loadStatsLocked(ctx)
}

func (v *View) loadStatsLocked(ctx context.Context) error {
	if v.index == nil || v.statsRow == nil || v.statsUpdate == nil {
		return nil
	}
	statsTypeName := string(v.statsRow.ProtoReflect().Descriptor().FullName())
	if v.reg == nil {
		v.reg = record.NewRegistry()
	}
	if _, err := v.reg.RegisterMessage(v.statsRow, 1); err != nil {
		return fmt.Errorf("failed to register stats message %s: %w", statsTypeName, err)
	}
	if err := v.index.SyncRegistry(ctx, v.reg); err != nil {
		return fmt.Errorf("failed to sync registry with index: %w", err)
	}

	def, _, ok := v.reg.LookupByName(statsTypeName)
	if !ok {
		return fmt.Errorf("stats type %s not in registry", statsTypeName)
	}
	kBytes, _, err := sds.SplitKeyAndNonKey(v.statsRow, def.GetKeyFields())
	if err != nil {
		return fmt.Errorf("failed to extract stats key: %w", err)
	}
	statsKey := sds.NewKeyFromBytes(kBytes)

	msg, ok, err := v.index.Get(ctx, statsTypeName, statsKey)
	if err != nil {
		return fmt.Errorf("failed to get stats from index: %w", err)
	}
	if ok && msg != nil {
		v.statsRow = proto.Clone(msg)
		return nil
	}

	// Stats row missing: recompute via full scan over all registered types
	newStats := proto.Clone(v.statsRow)
	hasRows := false

	for _, tdef := range v.reg.Export().GetTypes() {
		typeName := tdef.GetName()
		if typeName == statsTypeName {
			continue
		}
		for row, scanErr := range v.index.Scan(ctx, typeName, nil) {
			if scanErr != nil {
				return fmt.Errorf("failed to scan %s for stats recomputation: %w", typeName, scanErr)
			}
			hasRows = true
			v.recordMessage(row, typeName, nil)
			if err := v.statsUpdate(newStats, nil, row); err != nil {
				return fmt.Errorf("stats update failed on scan of %s: %w", typeName, err)
			}
			v.checkMessage(row)
		}
	}

	if hasRows || v.index.Position() > 0 {
		rawKey, rawVal, err := sds.SplitKeyAndNonKey(newStats, def.GetKeyFields())
		if err != nil {
			return fmt.Errorf("failed to split stats row: %w", err)
		}
		statsChange := sds.Change{
			Seq:      v.index.Position(),
			TypeName: statsTypeName,
			TypeID:   def.GetId(),
			Op:       sds.OpUpdate,
			Key:      statsKey,
			RawKey:   rawKey,
			RawVal:   rawVal,
			Row:      newStats,
		}
		if err := v.index.ApplyBatch(ctx, []sds.Change{statsChange}); err != nil {
			return fmt.Errorf("failed to write initial stats row: %w", err)
		}
	}

	v.statsRow = newStats
	return nil
}

// Stats returns a clone of the current in-memory stats row.
func (v *View) Stats() proto.Message {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.statsRow == nil {
		return nil
	}
	return proto.Clone(v.statsRow)
}

// BeforeLookupFunc returns the before-image of a row identified by (typeName, key).
// ok is false if the row did not exist before the changes.
type BeforeLookupFunc func(typeName string, key sds.Key) (proto.Message, bool, error)

// UpdateStatsFromChanges computes stats updates for a sequence of row changes using
// beforeLookup to obtain the pre-change state and update to adjust stats in-place.
func UpdateStatsFromChanges(
	changes []sds.Change,
	stats proto.Message,
	update StatsUpdateFunc,
	reg *record.Registry,
	beforeLookup BeforeLookupFunc,
) error {
	if stats == nil || update == nil || len(changes) == 0 {
		return nil
	}

	statsTypeName := string(stats.ProtoReflect().Descriptor().FullName())

	type coalescedKey struct {
		typeName string
		key      sds.Key
	}
	latestInBatch := make(map[coalescedKey]proto.Message)

	for _, ch := range changes {
		typeName := ch.TypeName
		if typeName == "" && reg != nil {
			if def, _, ok := reg.LookupByID(ch.TypeID); ok {
				typeName = def.GetName()
			}
		}
		if typeName == statsTypeName {
			continue
		}

		key := ch.Key
		if key.IsZero() && len(ch.RawKey) > 0 {
			key = sds.NewKeyFromBytes(ch.RawKey)
		}

		ck := coalescedKey{typeName: typeName, key: key}
		var before proto.Message
		if prev, exists := latestInBatch[ck]; exists {
			before = prev
		} else if beforeLookup != nil {
			existing, ok, err := beforeLookup(typeName, key)
			if err != nil {
				return fmt.Errorf("failed to get previous row for %s: %w", typeName, err)
			}
			if ok {
				before = existing
			}
		}

		var after proto.Message
		switch ch.Op {
		case sds.OpCreate, sds.OpUpdate:
			after = ch.Row
			if after == nil && reg != nil && (len(ch.RawKey) > 0 || len(ch.RawVal) > 0) {
				def, _, ok := reg.LookupByName(typeName)
				if !ok {
					return fmt.Errorf("type %s not in registry", typeName)
				}
				msgType, err := reg.ResolveMessageType(def.GetId())
				if err != nil {
					return fmt.Errorf("failed to resolve message type for %s: %w", typeName, err)
				}
				target := msgType.New().Interface()
				if err := sds.MergeKeyAndNonKey(target, ch.RawKey, ch.RawVal); err != nil {
					return fmt.Errorf("failed to decode row for %s: %w", typeName, err)
				}
				after = target
			}
		case sds.OpDelete:
			after = nil
		}

		latestInBatch[ck] = after

		if err := update(stats, before, after); err != nil {
			return fmt.Errorf("stats update failed for %s: %w", typeName, err)
		}
	}
	return nil
}

func (v *View) computeBatchStats(ctx context.Context, changes []sds.Change, baseStats proto.Message, batchSeq uint64) ([]sds.Change, proto.Message, error) {
	if v.statsRow == nil || v.statsUpdate == nil || v.index == nil {
		return changes, baseStats, nil
	}

	newStats := proto.Clone(baseStats)
	statsTypeName := string(newStats.ProtoReflect().Descriptor().FullName())

	beforeLookup := func(typeName string, key sds.Key) (proto.Message, bool, error) {
		return v.index.Get(ctx, typeName, key)
	}

	if err := UpdateStatsFromChanges(changes, newStats, v.statsUpdate, v.reg, beforeLookup); err != nil {
		return nil, nil, err
	}

	def, _, ok := v.reg.LookupByName(statsTypeName)
	if !ok {
		return nil, nil, fmt.Errorf("stats type %s not in registry", statsTypeName)
	}
	keyBytes, valBytes, err := sds.SplitKeyAndNonKey(newStats, def.GetKeyFields())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to split stats key and non-key: %w", err)
	}
	statsChange := sds.Change{
		Seq:      batchSeq,
		TypeName: statsTypeName,
		TypeID:   def.GetId(),
		Op:       sds.OpUpdate,
		Key:      sds.NewKeyFromBytes(keyBytes),
		RawKey:   keyBytes,
		RawVal:   valBytes,
		Row:      newStats,
	}

	toApply := make([]sds.Change, 0, len(changes)+1)
	toApply = append(toApply, changes...)
	toApply = append(toApply, statsChange)

	return toApply, newStats, nil
}

// Position returns the last applied stream sequence position.
func (v *View) Position() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.index != nil {
		pos := v.index.Position()
		if pos > v.appliedPos {
			return pos
		}
	}
	return v.appliedPos
}

// AppliedPosition returns the sequence position applied by the background applier.
func (v *View) AppliedPosition() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.appliedPos
}

// SetAppliedPosition sets the applied position (e.g. after snapshot restore or full replay).
func (v *View) SetAppliedPosition(pos uint64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.appliedPos = pos
}

// Lag returns the number of sequence numbers between currentSeq and the last applied sequence.
func (v *View) Lag(currentSeq uint64) uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if currentSeq > v.appliedPos {
		return currentSeq - v.appliedPos
	}
	return 0
}

// UnappliedBytes returns the approximate byte size of unapplied changes in the overlay.
func (v *View) UnappliedBytes() int64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.unappliedBytes
}

// SyncRegistry synchronizes type definitions with the view and underlying index.
func (v *View) SyncRegistry(ctx context.Context, reg *record.Registry) error {
	v.mu.Lock()
	if reg != nil {
		if v.reg == nil {
			v.reg = record.NewRegistry()
		}
		_ = v.reg.Import(reg.Export())
	}
	idx := v.index
	v.mu.Unlock()

	if idx != nil && reg != nil {
		return idx.SyncRegistry(ctx, reg)
	}
	return nil
}

// ApplyChanges enqueues committed changes to the unapplied overlay and background applier.
func (v *View) ApplyChanges(changes []sds.Change) {
	if len(changes) == 0 {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	normalized := make([]sds.Change, len(changes))
	for i, ch := range changes {
		typeName := ch.TypeName
		if typeName == "" && v.reg != nil {
			if def, _, ok := v.reg.LookupByID(ch.TypeID); ok {
				typeName = def.GetName()
			}
		}
		ch.TypeName = typeName
		if ch.Key.IsZero() && len(ch.RawKey) > 0 {
			ch.Key = sds.NewKeyFromBytes(ch.RawKey)
		}
		normalized[i] = ch

		ck := CacheKey{Table: typeName, Key: ch.Key}

		if ch.Row != nil {
			v.recordMessage(ch.Row, typeName, ch.Key)
		}

		sz := int64(len(ch.RawKey) + len(ch.RawVal) + 64)
		if old, ok := v.overlay[ck]; ok {
			if old.Row != nil {
				v.checkMessage(old.Row)
			}
			v.unappliedBytes -= old.Size
		}
		v.unappliedBytes += sz
		v.overlay[ck] = OverlayEntry{
			Op:   ch.Op,
			Row:  ch.Row,
			Seq:  ch.Seq,
			Size: sz,
		}

		if v.cache != nil && !v.cacheDisabled {
			v.cache.Remove(ck)
		}
	}

	v.queue = append(v.queue, normalized...)
	if v.applier != nil {
		v.applier.enqueueLocked(normalized)
	}
}

// ApplyChangesSync applies changes synchronously to the underlying index and read cache.
func (v *View) ApplyChangesSync(ctx context.Context, changes []sds.Change) error {
	if len(changes) == 0 {
		return nil
	}

	v.indexMu.Lock()
	defer v.indexMu.Unlock()

	var maxSeq uint64
	v.mu.RLock()
	maxSeq = v.appliedPos
	for _, ch := range changes {
		if ch.Seq > maxSeq {
			maxSeq = ch.Seq
		}
	}
	currentStats := v.statsRow
	v.mu.RUnlock()

	toApply, newStats, err := v.computeBatchStats(ctx, changes, currentStats, maxSeq)
	if err != nil {
		return err
	}

	if v.index != nil {
		if err := v.index.ApplyBatch(ctx, toApply); err != nil {
			return err
		}
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.statsUpdate != nil {
		v.statsRow = newStats
	}
	for _, ch := range changes {
		typeName := ch.TypeName
		if typeName == "" && v.reg != nil {
			if def, _, ok := v.reg.LookupByID(ch.TypeID); ok {
				typeName = def.GetName()
			}
		}
		key := ch.Key
		if key.IsZero() && len(ch.RawKey) > 0 {
			key = sds.NewKeyFromBytes(ch.RawKey)
		}
		ck := CacheKey{Table: typeName, Key: key}

		if ch.Row != nil {
			v.recordMessage(ch.Row, typeName, key)
		}

		if v.cache != nil && !v.cacheDisabled {
			switch ch.Op {
			case sds.OpCreate, sds.OpUpdate:
				if ch.Row != nil {
					v.cache.Put(ck, &CachedRow{Exists: true, Msg: ch.Row})
				} else {
					v.cache.Remove(ck)
				}
			case sds.OpDelete:
				v.cache.Put(ck, &CachedRow{Exists: false})
			}
		}
	}

	if maxSeq > v.appliedPos {
		v.appliedPos = maxSeq
	}
	return nil
}

// Get retrieves a merged proto row by table type name and primary key.
// It checks overlay first, then read cache, and falls back to index.
// The returned message is shared and must not be modified in place.
func (v *View) Get(ctx context.Context, typeName string, key sds.Key) (proto.Message, bool, error) {
	ck := CacheKey{Table: typeName, Key: key}

	// 1. Check unapplied overlay
	v.mu.RLock()
	if entry, ok := v.overlay[ck]; ok {
		v.mu.RUnlock()
		if entry.Op == sds.OpDelete || entry.Row == nil {
			return nil, false, nil
		}
		v.checkMessage(entry.Row)
		return entry.Row, true, nil
	}
	v.mu.RUnlock()

	// 2. Check read cache
	if v.cache != nil && !v.cacheDisabled {
		if row, ok := v.cache.Get(ck); ok {
			if !row.Exists || row.Msg == nil {
				return nil, false, nil
			}
			v.checkMessage(row.Msg)
			return row.Msg, true, nil
		}
	}

	v.mu.RLock()
	idx := v.index
	v.mu.RUnlock()

	if idx == nil {
		return nil, false, nil
	}

	// 3. Query underlying index
	msg, ok, err := idx.Get(ctx, typeName, key)
	if err != nil {
		return nil, false, err
	}

	if ok && msg != nil {
		v.recordMessage(msg, typeName, key)
	}

	if v.cache != nil && !v.cacheDisabled {
		if !ok || msg == nil {
			v.cache.Put(ck, &CachedRow{Exists: false})
		} else {
			v.cache.Put(ck, &CachedRow{Exists: true, Msg: msg})
		}
	}

	return msg, ok, nil
}

// Scan yields merged proto rows matching keyPrefix in canonical key-byte order.
// It overlays unapplied in-memory mutations onto the underlying index scan.
// Yielded messages are shared and must not be modified in place.
func (v *View) Scan(ctx context.Context, typeName string, keyPrefix []byte) iter.Seq2[proto.Message, error] {
	return func(yield func(proto.Message, error) bool) {
		type rowEntry struct {
			key sds.Key
			msg proto.Message
		}
		merged := make(map[string]rowEntry)

		type overlayRow struct {
			key   sds.Key
			entry OverlayEntry
		}
		overlaySnapshot := make(map[string]overlayRow)

		var keyFields []int32
		v.mu.RLock()
		if v.reg != nil {
			if def, _, ok := v.reg.LookupByName(typeName); ok {
				keyFields = def.GetKeyFields()
			}
		}
		idx := v.index
		for ck, entry := range v.overlay {
			if ck.Table != typeName {
				continue
			}
			if len(keyPrefix) > 0 && !bytes.HasPrefix(ck.Key.Bytes(), keyPrefix) {
				continue
			}
			overlaySnapshot[ck.Key.String()] = overlayRow{key: ck.Key, entry: entry}
		}
		v.mu.RUnlock()

		if idx != nil {
			for idxMsg, err := range idx.Scan(ctx, typeName, keyPrefix) {
				if err != nil {
					yield(nil, err)
					return
				}
				var k sds.Key
				if len(keyFields) > 0 {
					k, _ = sds.ExtractKey(idxMsg, keyFields)
				}
				if k.IsZero() {
					kBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(idxMsg)
					if err == nil {
						k = sds.NewKeyFromBytes(kBytes)
					}
				}
				if !k.IsZero() {
					kStr := k.String()
					if _, inOverlay := overlaySnapshot[kStr]; inOverlay {
						// Overlay mutation was captured at scan start and wins over index state.
						continue
					}
					v.recordMessage(idxMsg, typeName, k)
					merged[kStr] = rowEntry{key: k, msg: idxMsg}
				}
			}
		}

		for _, or := range overlaySnapshot {
			if or.entry.Op != sds.OpDelete && or.entry.Row != nil {
				kStr := or.key.String()
				merged[kStr] = rowEntry{key: or.key, msg: or.entry.Row}
			}
		}

		rows := make([]rowEntry, 0, len(merged))
		for _, re := range merged {
			rows = append(rows, re)
		}
		sort.Slice(rows, func(i, j int) bool {
			return bytes.Compare(rows[i].key.Bytes(), rows[j].key.Bytes()) < 0
		})

		for _, re := range rows {
			v.checkMessage(re.msg)
			if !yield(re.msg, nil) {
				return
			}
		}
	}
}

// ScanSlice returns a slice of all merged proto rows matching keyPrefix in canonical key-byte order.
func (v *View) ScanSlice(ctx context.Context, typeName string, keyPrefix []byte) ([]proto.Message, error) {
	var result []proto.Message
	for msg, err := range v.Scan(ctx, typeName, keyPrefix) {
		if err != nil {
			return nil, err
		}
		result = append(result, msg)
	}
	return result, nil
}

// FlushTo blocks until the background applier has processed changes up to or beyond targetSeq.
func (v *View) FlushTo(ctx context.Context, targetSeq uint64) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.applier == nil {
		return nil
	}

	if ctxDone := ctx.Done(); ctxDone != nil {
		stopCancel := make(chan struct{})
		defer close(stopCancel)
		go func() {
			select {
			case <-ctxDone:
				v.mu.Lock()
				if v.flushCond != nil {
					v.flushCond.Broadcast()
				}
				v.mu.Unlock()
			case <-stopCancel:
			}
		}()
	}

	for v.appliedPos < targetSeq && !v.closed {
		if err := ctx.Err(); err != nil {
			return err
		}
		v.applier.wakeLocked()
		v.flushCond.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// Flush blocks until all currently queued changes in the view have been applied to the index.
func (v *View) Flush(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.applier == nil {
		return nil
	}

	if ctxDone := ctx.Done(); ctxDone != nil {
		stopCancel := make(chan struct{})
		defer close(stopCancel)
		go func() {
			select {
			case <-ctxDone:
				v.mu.Lock()
				if v.flushCond != nil {
					v.flushCond.Broadcast()
				}
				v.mu.Unlock()
			case <-stopCancel:
			}
		}()
	}

	for len(v.queue) > 0 && !v.closed {
		if err := ctx.Err(); err != nil {
			return err
		}
		v.applier.wakeLocked()
		v.flushCond.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// WaitBackpressure blocks if unapplied overlay bytes exceed maxOverlayBytes.
func (v *View) WaitBackpressure(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if ctxDone := ctx.Done(); ctxDone != nil {
		stopCancel := make(chan struct{})
		defer close(stopCancel)
		go func() {
			select {
			case <-ctxDone:
				v.mu.Lock()
				if v.backpressureCond != nil {
					v.backpressureCond.Broadcast()
				}
				v.mu.Unlock()
			case <-stopCancel:
			}
		}()
	}

	for v.maxOverlayBytes > 0 && v.unappliedBytes > v.maxOverlayBytes && !v.closed {
		if err := ctx.Err(); err != nil {
			return err
		}
		if v.applier != nil {
			v.applier.wakeLocked()
		}
		v.backpressureCond.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// CacheStats returns cache hit/miss and capacity metrics.
func (v *View) CacheStats() LRUCacheStats {
	if v.cache == nil {
		return LRUCacheStats{}
	}
	return v.cache.Stats()
}

// CacheResetStats resets cache metrics counters.
func (v *View) CacheResetStats() {
	if v.cache != nil {
		v.cache.ResetStats()
	}
}

// SetCacheLimits updates the entry count and byte capacity of the read cache.
func (v *View) SetCacheLimits(capacity int, maxBytes int64) {
	if v.cache != nil {
		v.cache.SetLimits(capacity, maxBytes)
	}
}

// SetCacheDisabled enables or disables read caching.
func (v *View) SetCacheDisabled(disabled bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.cacheDisabled = disabled
}

// ApplierFailures returns the total number of batch apply errors encountered.
func (v *View) ApplierFailures() uint64 {
	if v.applier != nil {
		return v.applier.failures.Load()
	}
	return 0
}

// IsDegraded reports whether the background applier is currently in a degraded state.
func (v *View) IsDegraded() bool {
	if v.applier != nil {
		return v.applier.isDegraded.Load()
	}
	return false
}

// SetFaultHook sets a fault injection hook for testing.
func (v *View) SetFaultHook(hook func() error) {
	if v.applier != nil {
		v.applier.faultHook = hook
	}
}

// ClearCache clears all items from the read cache.
func (v *View) ClearCache() {
	if v.cache != nil {
		v.cache.Clear()
	}
}

// Close stops the applier worker and closes the underlying index.
func (v *View) Close() error {
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return nil
	}
	v.closed = true
	if v.flushCond != nil {
		v.flushCond.Broadcast()
	}
	if v.backpressureCond != nil {
		v.backpressureCond.Broadcast()
	}
	applier := v.applier
	idx := v.index
	v.mu.Unlock()

	if applier != nil {
		applier.stop()
	}

	if v.cache != nil {
		v.cache.Clear()
	}

	if v.checker != nil {
		v.checker.CheckAll()
	}

	if idx != nil {
		return idx.Close()
	}
	return nil
}

func (v *View) recordMessage(msg proto.Message, typeName string, key any) {
	if v.checker != nil && msg != nil {
		v.checker.Record(msg, typeName, key)
	}
}

func (v *View) checkMessage(msg proto.Message) {
	if v.checker != nil && msg != nil {
		v.checker.Check(msg)
	}
}

// CheckMutations verifies that no tracked messages have been mutated in place.
// Panics if any in-place mutation is detected.
func (v *View) CheckMutations() {
	if v.checker != nil {
		v.checker.CheckAll()
	}
}
