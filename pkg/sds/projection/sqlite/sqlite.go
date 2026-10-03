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

// Package sqlite provides an OLTP snapshot projection for Structured Data Streams (SDS)
// backed by pure-Go SQLite (modernc.org/sqlite).
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	_ "modernc.org/sqlite" // Pure-Go SQLite driver registration
)

var (
	// ErrTypeNotRegistered is returned when a change is encountered for an unregistered type ID.
	ErrTypeNotRegistered = errors.New("type not registered")
	// ErrPendingTransaction is returned when attempting a snapshot while transactions are pending.
	ErrPendingTransaction = errors.New("cannot snapshot stream with pending transactions")
)

// TableName maps a fully-qualified protobuf message name (e.g. "shop.Order")
// to a valid SQLite table name (e.g. "shop_Order") by stripping any leading dot
// and replacing all dots with underscores.
func TableName(fullName string) string {
	trimmed := strings.TrimPrefix(fullName, ".")
	return strings.ReplaceAll(trimmed, ".", "_")
}

// Index defines a declarative secondary index on a projection table.
type Index struct {
	// Table is the protobuf message name (e.g. "objectfs.DirEntry") or SQLite table name (e.g. "objectfs_DirEntry").
	Table string
	// Columns is the ordered list of column names to index.
	Columns []string
	// Unique specifies whether to enforce uniqueness.
	Unique bool
	// Name optionally specifies an explicit index name. If empty, a canonical name is generated.
	Name string
}

// Option configures a SQLite projection DB.
type Option func(*options)

type options struct {
	streamID    string
	journalMode string
	synchronous string
	decoderOpts []record.DecoderOption
	indexes     []Index
}

// WithStreamID sets the stream ID for the projection.
func WithStreamID(streamID string) Option {
	return func(o *options) {
		o.streamID = streamID
	}
}

// WithJournalMode sets the SQLite PRAGMA journal_mode (e.g. "OFF", "WAL", "DELETE").
func WithJournalMode(mode string) Option {
	return func(o *options) {
		o.journalMode = mode
	}
}

// WithSynchronous sets the SQLite PRAGMA synchronous (e.g. "OFF", "NORMAL", "FULL").
func WithSynchronous(syncMode string) Option {
	return func(o *options) {
		o.synchronous = syncMode
	}
}

// WithDecoderOptions passes record decoder options to the underlying ChangeReader.
func WithDecoderOptions(opts ...record.DecoderOption) Option {
	return func(o *options) {
		o.decoderOpts = append(o.decoderOpts, opts...)
	}
}

// WithIndex declares a secondary index on a projection table.
func WithIndex(table string, columns ...string) Option {
	return func(o *options) {
		o.indexes = append(o.indexes, Index{
			Table:   table,
			Columns: columns,
		})
	}
}

// WithSecondaryIndexes declares multiple secondary indexes on projection tables.
func WithSecondaryIndexes(indexes ...Index) Option {
	return func(o *options) {
		o.indexes = append(o.indexes, indexes...)
	}
}

// DB represents a SQLite projection database for a structured data stream.
type DB struct {
	mu           sync.RWMutex
	sqlDB        *sql.DB
	path         string
	streamID     string
	position     uint64
	changeReader *sds.ChangeReader
	knownTables  map[string]bool // tableName -> table created
	indexes      []Index
}

// Open opens an existing SQLite projection database or creates a new one if it does not exist.
func Open(ctx context.Context, path string, opts ...Option) (*DB, error) {
	var opt options
	for _, o := range opts {
		o(&opt)
	}

	sqlDB, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database at %q: %w", path, err)
	}

	if opt.journalMode != "" {
		if _, err := sqlDB.ExecContext(ctx, fmt.Sprintf("PRAGMA journal_mode = %s;", opt.journalMode)); err != nil {
			_ = sqlDB.Close()
			return nil, fmt.Errorf("failed to set journal_mode: %w", err)
		}
	}
	if opt.synchronous != "" {
		if _, err := sqlDB.ExecContext(ctx, fmt.Sprintf("PRAGMA synchronous = %s;", opt.synchronous)); err != nil {
			_ = sqlDB.Close()
			return nil, fmt.Errorf("failed to set synchronous: %w", err)
		}
	}

	db := &DB{
		sqlDB:        sqlDB,
		path:         path,
		streamID:     opt.streamID,
		changeReader: sds.NewChangeReader(opt.decoderOpts...),
		knownTables:  make(map[string]bool),
		indexes:      opt.indexes,
	}

	if err := db.initBookkeeping(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}

	if err := db.loadState(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}

	return db, nil
}

// SQLDB returns the underlying *sql.DB.
func (d *DB) SQLDB() *sql.DB {
	return d.sqlDB
}

// Path returns the file path of the SQLite database.
func (d *DB) Path() string {
	return d.path
}

// StreamID returns the stream ID.
func (d *DB) StreamID() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.streamID
}

// Position returns the stream sequence position of the last applied record.
func (d *DB) Position() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.position
}

// Registry returns the in-band type registry.
func (d *DB) Registry() *record.Registry {
	return d.changeReader.Registry()
}

// ChangeReader returns the underlying sds.ChangeReader.
func (d *DB) ChangeReader() *sds.ChangeReader {
	return d.changeReader
}

// Close closes the SQLite database connection.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sqlDB != nil {
		return d.sqlDB.Close()
	}
	return nil
}

func (d *DB) initBookkeeping(ctx context.Context) error {
	createTypesTable := `
CREATE TABLE IF NOT EXISTS _stream_types (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    fingerprint BLOB NOT NULL,
    descriptors BLOB,
    key_fields TEXT NOT NULL
);`
	if _, err := d.sqlDB.ExecContext(ctx, createTypesTable); err != nil {
		return fmt.Errorf("failed to create _stream_types table: %w", err)
	}

	createPosTable := `
CREATE TABLE IF NOT EXISTS _stream_position (
    stream_id TEXT PRIMARY KEY,
    position INTEGER NOT NULL,
    snapshot_time INTEGER NOT NULL
);`
	if _, err := d.sqlDB.ExecContext(ctx, createPosTable); err != nil {
		return fmt.Errorf("failed to create _stream_position table: %w", err)
	}

	return nil
}

func (d *DB) loadState(ctx context.Context) error {
	// 1. Load registry types
	rows, err := d.sqlDB.QueryContext(ctx, "SELECT id, name, fingerprint, descriptors, key_fields FROM _stream_types ORDER BY id ASC")
	if err != nil {
		return fmt.Errorf("failed to query _stream_types: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id uint32
		var name string
		var fingerprint []byte
		var descBytes []byte
		var keyFieldsJSON string

		if err := rows.Scan(&id, &name, &fingerprint, &descBytes, &keyFieldsJSON); err != nil {
			return fmt.Errorf("failed to scan _stream_types row: %w", err)
		}

		var keyFields []int32
		if err := json.Unmarshal([]byte(keyFieldsJSON), &keyFields); err != nil {
			return fmt.Errorf("failed to parse key_fields for type ID %d: %w", id, err)
		}

		var fds *descriptorpb.FileDescriptorSet
		if len(descBytes) > 0 {
			fds = &descriptorpb.FileDescriptorSet{}
			if err := proto.Unmarshal(descBytes, fds); err != nil {
				return fmt.Errorf("failed to unmarshal descriptors for type ID %d: %w", id, err)
			}
		}

		def := &sdsv1.TypeDefinition{
			Id:          id,
			Name:        name,
			Fingerprint: fingerprint,
			Descriptors: fds,
			KeyFields:   keyFields,
		}

		if err := d.changeReader.Registry().Register(def); err != nil {
			return fmt.Errorf("failed to register restored type %d (%s): %w", id, name, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error reading _stream_types: %w", err)
	}

	// 2. Ensure schema for all restored types
	for _, def := range d.changeReader.Registry().Export().GetTypes() {
		if err := d.ensureTableSchema(ctx, nil, def); err != nil {
			return fmt.Errorf("failed to ensure table schema for %s: %w", def.GetName(), err)
		}
	}

	// 3. Load stream position
	posRows, err := d.sqlDB.QueryContext(ctx, "SELECT stream_id, position FROM _stream_position LIMIT 1")
	if err != nil {
		return fmt.Errorf("failed to query _stream_position: %w", err)
	}
	defer posRows.Close()

	if posRows.Next() {
		var sID string
		var pos uint64
		if err := posRows.Scan(&sID, &pos); err != nil {
			return fmt.Errorf("failed to scan _stream_position: %w", err)
		}
		if d.streamID == "" {
			d.streamID = sID
		}
		d.position = pos
	}

	return nil
}

// Feed consumes a stream payload at sequence number seq, maintaining in-band schemas,
// grouping transactions, applying committed row changes, and updating _stream_position.
func (d *DB) Feed(ctx context.Context, seq uint64, payload []byte) ([]sds.Change, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	changes, err := d.changeReader.Feed(seq, payload)
	if err != nil {
		return nil, err
	}

	// Sync any newly registered or evolved types into SQLite tables and _stream_types
	regExport := d.changeReader.Registry().Export()
	for _, def := range regExport.GetTypes() {
		if err := d.ensureTableSchema(ctx, nil, def); err != nil {
			return nil, err
		}
	}

	if len(changes) == 0 {
		// No committed changes to apply for this record.
		return nil, nil
	}

	// Apply all committed changes and advance position in a single transaction
	tx, err := d.sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	for _, ch := range changes {
		if err := d.applyChangeInTx(ctx, tx, ch); err != nil {
			return nil, err
		}
	}

	// Advance _stream_position
	if err := d.updatePositionInTx(ctx, tx, seq); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	d.position = seq
	return changes, nil
}

// Apply applies a single committed row change directly to the SQLite projection.
func (d *DB) Apply(ctx context.Context, change sds.Change) error {
	return d.ApplyBatch(ctx, []sds.Change{change})
}

// ApplyBatch applies a slice of committed row changes in a single SQLite transaction.
func (d *DB) ApplyBatch(ctx context.Context, changes []sds.Change) error {
	if len(changes) == 0 {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	tx, err := d.sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var maxSeq uint64 = d.position
	for _, ch := range changes {
		if err := d.applyChangeInTx(ctx, tx, ch); err != nil {
			return err
		}
		if ch.Seq > maxSeq {
			maxSeq = ch.Seq
		}
	}

	if maxSeq > d.position {
		if err := d.updatePositionInTx(ctx, tx, maxSeq); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	if maxSeq > d.position {
		d.position = maxSeq
	}
	return nil
}

func (d *DB) applyChangeInTx(ctx context.Context, tx *sql.Tx, ch sds.Change) error {
	def, md, ok := d.changeReader.Registry().LookupByID(ch.TypeID)
	if !ok {
		return fmt.Errorf("%w: type ID %d (%s)", ErrTypeNotRegistered, ch.TypeID, ch.TypeName)
	}

	if err := d.ensureTableSchema(ctx, tx, def); err != nil {
		return err
	}

	tableName := TableName(def.GetName())

	switch ch.Op {
	case sds.OpCreate, sds.OpUpdate:
		var rowMsg proto.Message
		if ch.Row != nil {
			rowMsg = ch.Row
		} else {
			msgType, err := d.changeReader.Registry().ResolveMessageType(ch.TypeID)
			if err != nil {
				return fmt.Errorf("failed to resolve message type for %d: %w", ch.TypeID, err)
			}
			rowMsg = msgType.New().Interface()
			if err := sds.MergeKeyAndNonKey(rowMsg, ch.RawKey, ch.RawVal); err != nil {
				return fmt.Errorf("failed to merge row proto: %w", err)
			}
		}

		rowBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(rowMsg)
		if err != nil {
			return fmt.Errorf("failed to marshal full row proto: %w", err)
		}

		cols := sds.Columns(md)
		colNames := make([]string, 0, 2+len(cols))
		placeholders := make([]string, 0, 2+len(cols))
		args := make([]any, 0, 2+len(cols))

		colNames = append(colNames, "keydata", "rowdata")
		placeholders = append(placeholders, "?", "?")
		args = append(args, ch.RawKey, rowBytes)

		mReflect := rowMsg.ProtoReflect()
		for _, col := range cols {
			colNames = append(colNames, fmt.Sprintf("%q", col.Name))
			placeholders = append(placeholders, "?")
			args = append(args, sds.ExtractColumnValue(col, mReflect))
		}

		query := fmt.Sprintf("INSERT OR REPLACE INTO %q (%s) VALUES (%s);",
			tableName,
			strings.Join(colNames, ", "),
			strings.Join(placeholders, ", "),
		)
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("failed to upsert row in %q: %w", tableName, err)
		}

	case sds.OpDelete:
		query := fmt.Sprintf("DELETE FROM %q WHERE keydata = ?;", tableName)
		if _, err := tx.ExecContext(ctx, query, ch.RawKey); err != nil {
			return fmt.Errorf("failed to delete row in %q: %w", tableName, err)
		}

	default:
		return fmt.Errorf("unsupported change op: %v", ch.Op)
	}

	return nil
}

type dbExecQueryer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (d *DB) ensureTableSchema(ctx context.Context, tx *sql.Tx, def *sdsv1.TypeDefinition) error {
	tableName := TableName(def.GetName())

	var execer dbExecQueryer
	if tx != nil {
		execer = tx
	} else {
		execer = d.sqlDB
	}

	// 1. Sync _stream_types table
	descBytes, err := proto.Marshal(def.GetDescriptors())
	if err != nil {
		return fmt.Errorf("failed to marshal descriptors for %d: %w", def.GetId(), err)
	}
	kfBytes, err := json.Marshal(def.GetKeyFields())
	if err != nil {
		return fmt.Errorf("failed to marshal key_fields for %d: %w", def.GetId(), err)
	}

	upsertTypeSQL := `INSERT OR REPLACE INTO _stream_types (id, name, fingerprint, descriptors, key_fields) VALUES (?, ?, ?, ?, ?);`
	if _, err := execer.ExecContext(ctx, upsertTypeSQL, def.GetId(), def.GetName(), def.GetFingerprint(), descBytes, string(kfBytes)); err != nil {
		return fmt.Errorf("failed to upsert _stream_types for %d: %w", def.GetId(), err)
	}

	// 2. Resolve MessageDescriptor and derived columns
	msgType, err := d.changeReader.Registry().ResolveMessageType(def.GetId())
	if err != nil {
		return fmt.Errorf("failed to resolve message type for %d: %w", def.GetId(), err)
	}
	md := msgType.Descriptor()
	cols := sds.Columns(md)

	// 3. Inspect existing table columns using PRAGMA table_info
	existingCols := make(map[string]bool)
	rows, err := execer.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%q);", tableName))
	if err != nil {
		return fmt.Errorf("failed to inspect table info for %q: %w", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name string
		var colType string
		var notNull int
		var dfltVal sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltVal, &pk); err != nil {
			return fmt.Errorf("failed to scan table info row for %q: %w", tableName, err)
		}
		existingCols[name] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error reading table info for %q: %w", tableName, err)
	}

	if len(existingCols) == 0 {
		// Table does not exist: create with keydata BLOB PRIMARY KEY, rowdata BLOB, and all typed columns
		colDefs := []string{
			"keydata BLOB PRIMARY KEY",
			"rowdata BLOB",
		}
		for _, col := range cols {
			colDefs = append(colDefs, fmt.Sprintf("%q %s", col.Name, col.SQLiteType()))
		}

		createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %q (\n    %s\n);", tableName, strings.Join(colDefs, ",\n    "))
		if _, err := execer.ExecContext(ctx, createSQL); err != nil {
			return fmt.Errorf("failed to create table %q: %w", tableName, err)
		}
		for _, col := range cols {
			existingCols[col.Name] = true
		}
		d.knownTables[tableName] = true
	} else {
		// Table exists: evolve schema if new columns are defined
		for _, col := range cols {
			if !existingCols[col.Name] {
				alterSQL := fmt.Sprintf("ALTER TABLE %q ADD COLUMN %q %s;", tableName, col.Name, col.SQLiteType())
				if _, err := execer.ExecContext(ctx, alterSQL); err != nil {
					return fmt.Errorf("failed to add column %q to table %q: %w", col.Name, tableName, err)
				}
				existingCols[col.Name] = true
			}
		}
		d.knownTables[tableName] = true
	}

	// 4. Ensure secondary indexes for this table
	for _, idx := range d.indexes {
		if !matchesTable(idx.Table, def.GetName(), tableName) {
			continue
		}

		// Check all indexed columns exist
		allColsExist := true
		for _, colName := range idx.Columns {
			if !existingCols[colName] {
				allColsExist = false
				break
			}
		}
		if !allColsExist {
			continue
		}

		idxName := idx.Name
		if idxName == "" {
			idxName = fmt.Sprintf("idx_%s_%s", tableName, strings.Join(idx.Columns, "_"))
		}

		var quotedCols []string
		for _, colName := range idx.Columns {
			quotedCols = append(quotedCols, fmt.Sprintf("%q", colName))
		}

		uniqueStr := ""
		if idx.Unique {
			uniqueStr = "UNIQUE "
		}

		createIndexSQL := fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %q ON %q (%s);",
			uniqueStr,
			idxName,
			tableName,
			strings.Join(quotedCols, ", "),
		)
		if _, err := execer.ExecContext(ctx, createIndexSQL); err != nil {
			return fmt.Errorf("failed to create index %q on %q: %w", idxName, tableName, err)
		}
	}

	return nil
}

func matchesTable(indexTable, fullName, tableName string) bool {
	if indexTable == fullName || indexTable == tableName {
		return true
	}
	if TableName(indexTable) == tableName {
		return true
	}
	if strings.HasSuffix(fullName, "."+indexTable) {
		return true
	}
	return false
}

func (d *DB) updatePositionInTx(ctx context.Context, tx *sql.Tx, seq uint64) error {
	nowMicros := time.Now().UnixMicro()
	sqlStr := `INSERT OR REPLACE INTO _stream_position (stream_id, position, snapshot_time) VALUES (?, ?, ?);`
	if _, err := tx.ExecContext(ctx, sqlStr, d.streamID, seq, nowMicros); err != nil {
		return fmt.Errorf("failed to update _stream_position to %d: %w", seq, err)
	}
	return nil
}

// Get retrieves a merged proto row from a table by primary key.
func (d *DB) Get(ctx context.Context, typeName string, key sds.Key) (proto.Message, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	def, _, ok := d.changeReader.Registry().LookupByName(typeName)
	if !ok {
		return nil, false, fmt.Errorf("%w: type %q", ErrTypeNotRegistered, typeName)
	}

	tableName := TableName(typeName)
	var rowdata []byte
	query := fmt.Sprintf("SELECT rowdata FROM %q WHERE keydata = ?;", tableName)
	err := d.sqlDB.QueryRowContext(ctx, query, key.Bytes()).Scan(&rowdata)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("failed to query row from %q: %w", tableName, err)
	}

	msgType, err := d.changeReader.Registry().ResolveMessageType(def.GetId())
	if err != nil {
		return nil, false, fmt.Errorf("failed to resolve message type for %q: %w", typeName, err)
	}

	target := msgType.New().Interface()
	if err := proto.Unmarshal(rowdata, target); err != nil {
		return nil, false, fmt.Errorf("failed to unmarshal row proto: %w", err)
	}

	return target, true, nil
}

// Count returns the number of rows in the specified table.
func (d *DB) Count(ctx context.Context, typeName string) (int, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	tableName := TableName(typeName)
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %q;", tableName)
	if err := d.sqlDB.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count rows in %q: %w", tableName, err)
	}
	return count, nil
}

// Tables returns the sorted list of registered table names present in the projection.
func (d *DB) Tables() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	regExport := d.changeReader.Registry().Export()
	var tables []string
	for _, def := range regExport.GetTypes() {
		if def.GetId() >= 16 {
			tables = append(tables, def.GetName())
		}
	}
	sort.Strings(tables)
	return tables
}

// Rows returns all merged proto rows from the specified table.
func (d *DB) Rows(ctx context.Context, typeName string) ([]proto.Message, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	def, _, ok := d.changeReader.Registry().LookupByName(typeName)
	if !ok {
		return nil, fmt.Errorf("%w: type %q", ErrTypeNotRegistered, typeName)
	}

	msgType, err := d.changeReader.Registry().ResolveMessageType(def.GetId())
	if err != nil {
		return nil, fmt.Errorf("failed to resolve message type for %q: %w", typeName, err)
	}

	tableName := TableName(typeName)
	query := fmt.Sprintf("SELECT rowdata FROM %q;", tableName)
	rows, err := d.sqlDB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query rows from %q: %w", tableName, err)
	}
	defer rows.Close()

	var result []proto.Message
	for rows.Next() {
		var rowdata []byte
		if err := rows.Scan(&rowdata); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		target := msgType.New().Interface()
		if err := proto.Unmarshal(rowdata, target); err != nil {
			return nil, fmt.Errorf("failed to unmarshal row proto: %w", err)
		}
		result = append(result, target)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}
