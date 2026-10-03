# Structured Data Streams: SQL, OLTP and OLAP as Snapshots over a Stream of Proto Records

> **Status:** Architecture and design rationale document. For the normative wire format specification, see [`docs/sds-spec.md`](sds-spec.md). It builds on the existing Streams service ([`docs/streams.md`](streams.md)) and borrows the log-plus-snapshot pattern that ObjectFS already uses ([`docs/objectfs.md`](objectfs.md)).

---

## Overview

A Streams stream is an ordered, durable sequence of opaque payloads. This design proposes a **structured** payload format on top of it, so that a stream becomes a **logical change log for a relational dataset**: typed records describing row inserts, updates and deletes, optionally grouped into transactions, with the schema carried in-band.

Once the log is the source of truth, every other representation is a **projection** of it, materialized at a known stream position:

| Projection | Format | Serves |
| :--- | :--- | :--- |
| **OLTP snapshot** | SQLite database file | Point lookups, small transactions, serving reads with indexes |
| **OLAP snapshot** | Parquet files (one per table) | Scans, aggregations, DuckDB / Spark |
| **Filesystem image** *(case study)* | EROFS image | The ObjectFS metadata tree, as today |

The log is authoritative and append-only. Snapshots are derived, rebuildable, and interchangeable: a consumer restores state from *any* snapshot at position `P` and replays the log from `P` forward. This is the architecture of Delta Lake (JSON action log plus parquet checkpoints) and Iceberg (metadata log plus manifests), with a durable low-latency log underneath and a first-class OLTP projection that those systems lack.

The design has four layers, each independent of the ones above it:

```
+-----------------------------------------------------------------------------------+
|  Layer 3: Projections (snapshots at position P, each carrying the type registry)  |
|     SQLite (OLTP)      Parquet (OLAP)      EROFS (filesystem)      ...            |
+-----------------------------------------------------------------------------------+
|  Layer 2: Logical change log                                                      |
|     application messages as tables, row changes, transactions, schema evolution   |
+-----------------------------------------------------------------------------------+
|  Layer 1: Record typing                                                           |
|     payload = varint(type_id) body ; type 1 defines new types in-band             |
+-----------------------------------------------------------------------------------+
|  Layer 0: Streams transport (existing, unchanged)                                 |
|     WALC/WALL framing, CRC32C, stream_seq, Local/Witness/Permanent durability      |
+-----------------------------------------------------------------------------------+
```

Layer 0 never inspects a payload. Everything above it is a client-side convention, which is also what allows payloads to be encrypted end to end (see [Encryption](#encryption)).

---

## Layer 1: Record Typing

### The problem

A stream that carries more than one kind of record needs a per-record type. The conventional answers are a `google.protobuf.Any` (a 40–60 byte type URL per record) or a `oneof` in a fixed envelope message (types fixed at compile time). Neither suits a change log where the record types are *tables*, which appear at runtime and must be readable from an archive years later without access to the source tree.

### The format

Bodies are always protobuf. Every payload begins with a varint type id, followed by the record body:

```
payload := varint(type_id) body
```

| `type_id` | Meaning | Body |
| :--- | :--- | :--- |
| `0` | Reserved, never valid. Zeroed or torn data is detected immediately. | — |
| `1` | **TypeDefinition**: introduces or compatibly updates a type id for this stream. | `TypeDefinition` (schema fixed by the format version) |
| `2` | **TxCommit**: closes a multi-record transaction. | `TxCommit` |
| `3` | **SnapshotPointer**: announces that a snapshot covering up to a position exists. | `SnapshotPointer` |
| `4` | **Padding**: no-op. | Arbitrary bytes |
| `5` | **OpRecord**: relational row change (CREATE, UPDATE, DELETE). | `OpRecord` |
| `6`–`15` | Reserved for the framework. | — |
| `16`+ | Application types, defined by a preceding `TypeDefinition`. | Bare registered message (keyless event) |

Type ids `16`–`127` cost one byte per record; ids up to `16383` cost two.

```proto
message TypeDefinition {
  uint32 id = 1;                                      // >= 16; never reused within a stream
  string name = 2;                                    // fully-qualified message name, e.g. "shop.Order"
  bytes  fingerprint = 3;                             // sha256 over the canonical serialized descriptors
  google.protobuf.FileDescriptorSet descriptors = 4;  // defines `name`; may be omitted if this fingerprint is already known
  repeated int32 key_fields = 5;                      // field numbers of `name` that form the primary key, in order
}
```

### Key Principles

1. **Define before use.** A type's `TypeDefinition` precedes its first use in the same stream. This is inherent to any in-band scheme.
2. **Ids are never reused, and redefinitions must be compatible.** A `TypeDefinition` for an id that already exists is allowed only if the new descriptors are a compatible evolution of the previous ones: existing field numbers keep their wire type and label, fields may be added, and fields may be removed only by reserving their numbers. Renaming a field is allowed (proto identifies fields by number). Changing a field's type, its `repeated`-ness, or its number, or changing `key_fields`, is incompatible and is rejected by writers and readers alike.
3. **Ids are scoped to a stream and allocated by its single writer.** Streams already have exactly one writer per `stream_id`, so no coordination is needed. Types are shared *across* streams by `fingerprint` or `name`, never by id.
4. **Definitions are idempotent by fingerprint.** A repeated definition with a matching fingerprint is a no-op; a compatible one with a new fingerprint is an evolution; an incompatible one is a hard error. This lets a restarting writer simply re-emit its definitions instead of persisting which ids it has already announced.
5. **The registry travels with snapshots and cursors, not with periodic keyframes in the log.** See [Layer 3](#layer-3-projections-as-snapshots). A reader that starts from a snapshot at `P` has every definition in force at `P`; anything newer is in the log after `P`. A tailing consumer persists the handful of definitions it has seen alongside its position. The log therefore carries each definition once per version.

The accepted cost of principle 5 is that a lone segment pulled out of object storage is not decodable without the snapshot before it. Tooling that inspects segments takes a snapshot or registry as input.

### Why proto

Bodies are protobuf, with no alternative encodings, for reasons that matter specifically to an archive:

- **Structural decode without a schema.** Every proto field carries a tag and wire type, so a record can be walked field by field with no descriptor at all (`protoc --decode_raw`). If a registry is ever lost, the structure of a ten-year-old segment is still recoverable. Avro's positional encoding has no field boundaries and is unrecoverable without its exact writer schema.
- **Misparse fails loudly.** Decoding an Avro record with the wrong writer schema yields plausible garbage; a proto decoded against the wrong type yields unknown fields or a wire-type error.
- **One IDL, one toolchain.** The Streams and ObjectFS APIs, and `MutationRecord`, are already proto with first-party Go support. A log record can flow through `AppendRecord` and out of `Tail` as the same bytes, and applications register the message types they already have.
- **Sparse records are smaller.** Unset fields cost nothing; a change log is sparse (deletes carry only key fields, most updates touch two columns).
- **Unknown fields are preserved**, so a relay or compactor built against an older schema forwards newer fields intact, and compatible schema evolution needs no coordination between writer and readers.

Avro wins on dense tiny rows and on its logical-type vocabulary (`decimal`, `date`, `uuid`); the first is recovered by segment compression and the second is handled by annotations (Layer 2). Consumers that need Avro, e.g. Kafka sinks, convert at the edge from the descriptors in the registry.

---

## Layer 2: The Logical Change Log

### Tables are the application's messages

A table is a protobuf message the application already has. The `TypeDefinition` registers that message by name, carries its `FileDescriptorSet` so that readers without the generated code can decode it with `dynamicpb`, and names the fields that form the primary key. Nothing is generated per table.

```proto
// shop/order.proto — the application's own message, unchanged.
message Order {
  int64  id = 1;
  string customer = 2;
  double total = 3;
  google.protobuf.Timestamp placed_at = 4;
  bytes  receipt_sha256 = 5;
}
```

```
TypeDefinition{ id: 16, name: "shop.Order", descriptors: <shop/order.proto + imports>, key_fields: [1] }
```

Every row change is an `OpRecord` framework record (type ID 5):

```proto
message OpRecord {
  enum Op {
    CREATE = 0;
    UPDATE = 1;
    DELETE = 2;
  }
  Op     op = 1;
  uint32 type_id = 2;
  uint64 tx_id = 3;   // 0: this record is its own transaction; otherwise pending until TxCommit{tx_id}
  bytes  key = 4;     // canonical binary proto encoding of the key fields
  bytes  value = 5;   // binary proto encoding of non-key fields (CREATE/UPDATE)
}
```

A row change on the wire carries the table type ID, the operation, the transaction ID (`0` for autocommit), the canonically encoded proto key bytes, and the non-key proto value bytes. There is no column unpacking or per-table wrapper; key data and value data remain native protobuf bytes.

**Columns are derived from the descriptor.** Scalar fields map to columns; `key_fields` become the primary key.

**Schema evolution is in place.** Adding a field to `Order` is a new `TypeDefinition` for id 16 with the new descriptors. Records written before it decode under the new descriptor with the field unset; records written after it decode under an older descriptor with an unknown field. Projections add the column when they see the definition. Incompatible changes are a new message and a new table.

### Value types

Column types are derived from proto field types, restricted to what the projections represent losslessly:

| proto field | SQLite | Parquet |
| :--- | :--- | :--- |
| `bool` | `INTEGER` 0/1 | `BOOLEAN` |
| `int32`/`int64`/`sint*`/`uint32`, enums | `INTEGER` | `INT32`/`INT64` |
| `uint64`/`fixed64` | `INTEGER` (two's complement) or `BLOB` | `INT64` with `UINT_64` annotation |
| `float`/`double` | `REAL` | `FLOAT`/`DOUBLE` |
| `string` | `TEXT` | `BYTE_ARRAY` (UTF8) |
| `bytes` | `BLOB` | `BYTE_ARRAY` |
| `google.protobuf.Timestamp` | `INTEGER` (micros) | `INT64` (TIMESTAMP_MICROS) |
| proto3 `optional`, message fields | `NULL` when unset | definition level |

Richer semantics (`decimal`, `date`, `uuid`) are field annotations carried in the descriptors (custom options), stored over one of the above. Large values (file contents, images) are not stored inline: they are written to the content-addressed blob store and referenced by SHA-256, the same rule ObjectFS is adopting for large file content.

### Transactions

Most changes are single rows. An `OpRecord` with `tx_id = 0` is its own autocommit transaction and is applied immediately; no `TxCommit` is written. A multi-row transaction sets the same non-zero `tx_id` on each of its records and closes with:

```proto
message TxCommit {
  uint64 tx_id = 1;
  google.protobuf.Timestamp commit_time = 2;
}
```

A reader holds records with a non-zero `tx_id` as pending until the matching `TxCommit` arrives; on recovery, pending groups with no commit are discarded. Snapshots are only taken at points where nothing is pending (safe positions). A filesystem rename, which touches two directory rows and one inode row, is one transaction; a single `mkdir` is not.

`tx_id` values are allocated by the stream's writer and need only be unique among transactions that are open at the same time. Cross-stream transactions are out of scope: a stream is the unit of atomicity and ordering, as it is today.

---

## Layer 3: Projections as Snapshots

### Definition

A **snapshot** is a materialization of a stream's state:

- taken at a position `P` (the `stream_seq` of the last applied record) at which no transaction is pending;
- containing the **type registry in force at `P`** (the current `TypeDefinition` of every id seen so far), not merely the table schemas;
- stored in object storage under the stream, named by position so lexical order is stream order: `streams/<stream-uuid>/snapshots/<format>/<position 20 digits>.<ext>`;
- immutable once written.

Given a snapshot at `P`, a reader that wants the state at `Q >= P` restores the snapshot and replays records `(P, Q]`. This is the same contract the ObjectFS controller has with its EROFS image plus mutation log, and it gives **time travel** for free: pick the latest snapshot at or before `Q`.

Snapshots are also the **retention anchor**. Segments older than the oldest snapshot anyone still needs can be deleted; the log need not be retained from the beginning of time, and definitions from before the retention window survive in the snapshot registry.

### SQLite (OLTP)

- One database file per stream (or per shard if a stream is sharded).
- Tables are created from the registry via `sds.Columns(md)`. Each scalar field and `google.protobuf.Timestamp` is a typed SQL column (`INTEGER`, `REAL`, `TEXT`, `BLOB`); `keydata BLOB` is the canonical primary key; `rowdata BLOB` stores the full encoded row (key and non-key fields merged) preserving nested messages and repeated fields losslessly.
- Column naming rule: derived from the protobuf field name. If a field name collides with internal projection columns (`keydata`, `rowdata`, `valuedata`) or duplicate column names, underscores are appended (e.g. `keydata_`). SQL identifiers are quoted in double quotes to prevent keyword conflicts.
- Secondary indexes are a declarative projection option (e.g. `WithIndex("DirEntry", "ino")`), created when the table is created or evolved.
- Schema evolution is handled in place: when an evolved `TypeDefinition` is registered mid-stream, new columns are added via `ALTER TABLE ... ADD COLUMN`, leaving earlier rows `NULL` for new columns.
- Two bookkeeping tables: `_stream_types` (`id`, `name`, `fingerprint`, `descriptors BLOB`, `key_fields`) and `_stream_position` (`stream_id`, `position`, `snapshot_time`).
- Apply rules: `CREATE` → `INSERT OR REPLACE`; `UPDATE` → `UPDATE ... WHERE key` (or replace); `DELETE` → `DELETE WHERE key`. An autocommit `OpRecord` is its own SQLite transaction; a `TxCommit` closes one covering its pending rows.
- Writing: build in a temp file, `PRAGMA journal_mode=OFF` during load, fsync, then upload. Serving: download or stream to local disk, open read-only, and keep applying the live tail into it from `Tail`.
- SQLite is the initial OLTP shape because a single file is trivially snapshotted, has real indexes and a page cache, and is readable everywhere. Its WAL and page format are *not* used as the log format: the log is logical, the file is a projection.

### Parquet (OLAP)

- One parquet file per table per snapshot, under the position-named directory. The type registry and position go in the file footer's key/value metadata, so a parquet file is self-describing to DuckDB or Spark without any side channel.
- Append-only tables can be snapshotted **incrementally**: each snapshot writes only rows committed since the previous one, and readers union the files. Tables with updates and deletes need either a full rewrite per snapshot or a merge-on-read design with delete files, as Iceberg does; the first is the proposal's starting point and the second is an open question.
- Row-group sizing, sorting, and partitioning are projection choices. Two OLAP snapshots of the same log can be organized differently for different query shapes.
- Writing the OLAP projection as an Iceberg table instead of bare files is a natural upgrade of the projector alone: each stream snapshot becomes a table commit carrying the position in its summary, delete files answer the mutable-table question, and any Iceberg-aware engine can read the result with no knowledge of Streams.

### EROFS (filesystem, case study)

ObjectFS today stores its metadata mutations as a proto `MutationRecord` in a stream and periodically compiles an EROFS image. Recast in this design, the filesystem is two registered messages, an inode and a directory entry; a `mkdir` is one transaction touching both; and the EROFS image is a third kind of projection alongside SQLite and Parquet, with `user.digest` xattrs carrying the blob SHA exactly as now. This unification is deliberately tabled: it would change the on-disk WAL format for a working system and belongs behind stream-format versioning rather than in this proposal. It is listed here because it demonstrates that the layering is general, and because it would let a directory tree be queried with SQL from the SQLite projection.

### SnapshotPointer

Snapshots are discoverable by listing the object-store prefix, so no manifest is required (the same manifest-free choice Streams made for segments). Optionally, the writer or the snapshotter appends a framework record so that the log itself says where its snapshots are:

```proto
message SnapshotPointer {
  uint64 position = 1;     // stream_seq the snapshot covers up to
  string format = 2;       // "sqlite", "parquet", "erofs"
  string location = 3;     // object key or URL
  bytes  registry_fingerprint = 4;
}
```

A reader tailing a stream then learns about new snapshots without polling the bucket. It is a pointer, never a copy of the snapshot's content.

---

## Reading

| Reader | Starts from | Needs |
| :--- | :--- | :--- |
| Full restore / new replica | Latest snapshot ≤ target position, then replay | Snapshot only |
| Serving node | Snapshot on local disk + live `Tail` | Snapshot + cursor |
| CDC / tailing consumer | Its saved cursor | Its saved cursor **and** the definitions it has seen |
| Ad-hoc analytics | A Parquet snapshot directly | Nothing else |
| Debugging (`streams cat`) | Any segment | A snapshot or registry file to resolve type ids |

A consumer that loses its registry restarts from the latest snapshot at or before its cursor. This is the only place a snapshot is strictly required for correctness, and it is exactly what snapshots are for.

---

## Encryption

The client-side encryption proposed for Streams (per-record AEAD, [issue #87](https://github.com/gke-labs/in-cluster-storage/issues/87)) composes cleanly because the type varint lives **inside** the payload: an observer sees neither record types nor their distribution, only sizes and timing. The `Padding` type exists so that sizes can be bucketed under encryption, and the key envelope for a stream and its `TypeDefinition`s are both "stream header" records that a snapshot carries forward.

---

## Alternatives Considered

| Alternative | Why not (here) |
| :--- | :--- |
| `google.protobuf.Any` per record | 40–60 bytes per record for the type URL; still no schema in the archive. |
| `oneof` in a fixed envelope | Same cost as the varint, but the set of types is fixed at compile time; tables appear at runtime. |
| Generated per-table row messages | Duplicates messages applications already have; registering the application's own message costs one extra tag-and-length per image and needs no code generation. |
| Per-record `Encoding` (proto / Avro / JSON / raw) | Every reader and every projector would have to support every encoding forever; one encoding keeps the archive uniformly decodable. Conversions happen at the edge. |
| Avro | No structural decode without the writer schema; silent misparse on schema mismatch; second IDL and third-party Go tooling. |
| Arrow IPC record batches as the log | Excellent for bulk ingestion and maps straight to Parquet, but hundreds of bytes of overhead per single-row change. |
| New type id per schema change | Simpler invariants on paper, but every projection then has to merge several ids into one table. Requiring compatible in-place evolution keeps one id per table and leans on proto's own compatibility rules. |
| SQLite session changesets | Almost exactly a row-change log, and worth reading, but engine-specific binary with no versioning; unsuitable as an archive format. |
| SQLite WAL / physical page log | Ties the log to one engine's page layout; cannot be projected into Parquet. |
| Riegeli | Solves the *file* problem well (descriptors in the header, chunk checksums, resync, transposition) and validates the in-band-schema approach, but is one type per file, has no streaming or append semantics, and has no Go implementation. Its role would be a sealed-segment format, which zstd on segments already covers. |
| Periodic type keyframes in the log | Redundant once snapshots carry the registry and consumers persist it with their cursor. Dropped. |

---

## Open Questions

- **Column mapping for nested and repeated fields.** Column derivation for relational projections excludes nested messages (other than `google.protobuf.Timestamp`) and repeated fields, keeping them only in the raw row proto. Whether future projections flatten with a naming convention, store as JSON text, or treat them as non-queryable remains open. The raw-proto-plus-index SQLite layout (see the TODO above) sidesteps this for OLTP; Parquet has native nested types.
- **Precise compatibility rules.** Fully specified in the normative specification ([`docs/sds-spec.md#4-precise-schema-compatibility-rules`](sds-spec.md)).
- **Snapshot production.** Who takes snapshots: the writer (has the state in memory), a buffer-side compactor (has all streams, no application code), or a dedicated projector reading `Tail`? The projector is the most general and keeps the buffer schema-free; the writer is simplest for a single-tenant stream.
- **Cadence and cost.** Snapshot on a size threshold of log since last snapshot, on a timer, or on demand. Interaction with `Permanent` durability: a snapshot must not cover positions beyond the `s3Seq` watermark.
- **Sharding.** A stream is the unit of ordering; a large table may need many streams. How keys map to streams, and whether a Parquet projection can span streams, is undecided.
- **Mutable tables in Parquet.** Full rewrite per snapshot versus delete files and merge-on-read, or delegating this to Iceberg.
- **Compaction of the log itself.** Whether to ever rewrite segments (e.g. dropping superseded rows) or rely purely on snapshots plus retention. The latter is simpler and the proposal's default.
- **Annotation vocabulary.** Custom options for `decimal`, `date`, `uuid`, and how they round-trip into SQLite (`DECIMAL` has no native type there).
- **Format versioning.** A stream header record identifying the payload format version, shared with the encryption envelope, so that Layer 1 can evolve.

---

## Roadmap

- [x] Layer 1 in `pkg/sds`: `TypeDefinition` / `OpRecord` / `TxCommit` / `SnapshotPointer` / `Padding` protos, typed `Append`/`Read` framing that maintains the registry and enforces compatibility.
- [x] Layer 2: `OpRecord` row operations (CREATE, UPDATE, DELETE), canonical primary key splitting/merging, transaction grouping on read, safe snapshot positions, and in-memory memtable projection.
- [ ] SQLite projector: build, publish, restore, live tail apply.
- [ ] Parquet projector: append-only tables first.
- [ ] Conformance suite: random logs replayed into both projections must agree, from any snapshot position and across compatible schema changes.
- [x] Case study: express the ObjectFS `MutationRecord` as relational tables (`Inode`, `DirEntry`, `Content`) and produce EROFS snapshots as position-named projections with embedded schemas and no pointer files.
