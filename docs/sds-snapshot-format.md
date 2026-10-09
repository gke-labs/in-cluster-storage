# Structured Data Streams (SDS) Table Snapshot Format Specification

> **Version:** v1 (prototype)  
> **Status:** Prototype Specification  
> **Related Documents:** [`docs/sds-spec.md`](sds-spec.md), [`docs/structured-data-streams.md`](structured-data-streams.md)

---

## 1. Overview and Motivation

Structured Data Streams (SDS) streams relational log mutations (`CREATE`, `UPDATE`, `DELETE`) over an append-only, ordered byte transport. To support point-in-time recovery, fast volume startup, and random-access reads, SDS generates periodic snapshots at consistent transaction boundaries (safe stream positions $P$).

Historically, published snapshots used whole SQLite database files (`.sqlite`), and the local mutable index ran SQLite with write-ahead logging (WAL). While functional, this design created tight coupling to SQLite:
1. **Unnecessary overhead:** All SDS tables are simple binary key/value relations `(keydata BLOB PRIMARY KEY, valuedata BLOB)` storing canonical SDS keys and Protobuf wire bytes. Parsing SQL schemas and running transpiled C engines (~240 MB Go driver) is excessive for a snapshot reader.
2. **Restore friction:** Restoring an SQLite snapshot requires downloading the full file and opening an engine instance, while local index recovery involves replaying WAL pages.
3. **Write amplification:** Benchmarks revealed 4–40 KB of disk writes per small filesystem metadata operation in SQLite's WAL/B-tree structures.

The **Table Snapshot Format** solves this by establishing an in-house, sorted-table-of-protos format for both published snapshots and local indexing. Published snapshots are standalone, immutable, checksummed block-based tables (SSTables) with embedded schema definitions. Restore is an instant ingest operation (hard-linking or copying the table directly into the LSM engine's L0), and external tools can read snapshots directly using standalone block-based table readers without launching an LSM engine.

---

## 2. Keyspace and Key Conventions

All entries—both framework metadata and application table records—live in a single, globally sorted byte keyspace:

$$\text{Entry Key} := \text{varint}(\text{type\_id}) \mathbin{\Vert} \text{canonical SDS key bytes}$$

$$\text{Entry Value} := \text{non-key Protobuf bytes}$$

- **`type_id`:** Protobuf unsigned varint (LEB128). Identifies the table schema registered in the stream registry.
- **Canonical SDS key:** Binary Protobuf wire encoding of primary key fields in strictly ascending field-number order, as extracted by `sds.PrimaryKey.Split()` / `sds.ExtractKey()`.
- **Non-key Protobuf bytes:** Binary Protobuf wire encoding of the remaining non-key fields. A complete row message is reconstituted by merging the key and non-key bytes using `sds.MergeKeyAndNonKey()`.

### Prefix Scanning and Lexicographical Sorting

Because tables are stored in lexicographical key order (`bytes.Compare`):
1. All rows for a given `type_id` share the prefix `varint(type_id)` and are stored contiguously.
2. Scanning an entire table is a seek to `varint(type_id)` followed by sequential block iteration until the prefix no longer matches.
3. Scans with a primary key prefix `P` seek to `varint(type_id) ‖ P` with an upper bound computed by incrementing the least-significant byte of the prefix.
4. Entries within each table retain strict canonical SDS key ordering.

---

## 3. Reserved Metadata Keys (Type ID 0)

Type ID `0` is invalid for application data records and is reserved for framework metadata. Because `varint(0)` encodes to a single byte `0x00`, and application type IDs are $\ge 16$ (encoded as varints with first byte $\ge 0x10$), **all metadata keys sort strictly before all application table entries**.

| Metadata Key | Format / Value | Description |
| :--- | :--- | :--- |
| `\x00position` | 8-byte big-endian `uint64` | The stream sequence position $P$ reflected by this snapshot. |
| `\x00registry` | Protobuf wire bytes (`sds.v1.Registry`) | The full type registry in force, containing all `TypeDefinition` messages. |
| `\x00stream_id` | UTF-8 string bytes | The unique stream identifier. |

By embedding the applied position and the complete type registry directly in the snapshot table, every snapshot file is completely **self-describing**, just like a stream segment. Readers require no external catalogs or out-of-band schema definitions.

---

## 4. Pinned Physical Format

Published snapshots are formatted as block-based Sorted String Tables (SSTables) following the RocksDB block-based table format specification.

### Version Pinning

Published snapshots pin **`sstable.TableFormatPebblev1`** (RocksDB block-based format with Pebble v1 block properties):
- **Magic Number:** 8 bytes at the end of the footer.
- **Footer Size:** Fixed 48 bytes at file end.
- **Block Size:** 4096 bytes target block size (configurable).
- **Restart Interval:** 16 keys between delta-compression restart points.
- **Checksum Algorithm:** CRC32C checksum verified per block.

```
+-------------------------------------------------------------+
| Data Block 0: Metadata Keys (\x00position, \x00registry...)  |
+-------------------------------------------------------------+
| Data Block 1: Application Rows (varint(type_id) || key)     |
+-------------------------------------------------------------+
| ...                                                         |
+-------------------------------------------------------------+
| Filter Block (optional bloom filters)                       |
+-------------------------------------------------------------+
| Meta Index Block (properties, table metadata)               |
+-------------------------------------------------------------+
| Index Block (binary search index over data blocks)          |
+-------------------------------------------------------------+
| Footer (48 bytes: index handle, meta handle, magic number)  |
+-------------------------------------------------------------+
```

Point lookups execute via binary search in the index block followed by loading a single data block. Range scans seek into the index block and stream sequentially through data blocks.

---

## 5. Reading Snapshots Without an Engine

Snapshot files can be parsed by standalone readers without running a Pebble LSM or SQLite instance. The Pebble repository provides standalone `sstable.NewReader` and `sstable.NewMemReader` utilities, and RocksDB provides standard C++, Rust, and Java block-based table readers.

### Example: Standalone Reader in Go

```go
package main

import (
    "bytes"
    "context"
    "fmt"
    "github.com/cockroachdb/pebble/v2/sstable"
    "github.com/cockroachdb/pebble/v2/vfs"
    "github.com/gke-labs/in-cluster-storage/pkg/sds/projection/table"
)

func InspectSnapshot(path string) error {
    sr, err := table.OpenSnapshotFile(path)
    if err != nil {
        return err
    }
    defer sr.Close()

    fmt.Printf("Stream ID: %s\n", sr.StreamID())
    fmt.Printf("Applied Position: %d\n", sr.Position())
    fmt.Printf("Registered Tables: %v\n", sr.Registry().TypeNames())

    // Iterate through rows in a table
    for msg, err := range sr.Scan("objectfs.v1alpha1.Inode", nil) {
        if err != nil {
            return err
        }
        fmt.Printf("Row: %v\n", msg)
    }
    return nil
}
```

Direct readers in other languages open the block-based table using the standard 48-byte RocksDB footer, inspect the index block, and seek to user keys starting at `\x00` for metadata.

---

## 6. Object Storage Layout and Ingestion Lifecycle

### Canonical Key Path

Published snapshots are stored at:
```
streams/<stream_id>/snapshots/table/<position, 20 digits>.sst
```
Example: `streams/vol-42/snapshots/table/00000000000000010000.sst`

### Restore via Ingest

When a volume restores a table snapshot:
1. The `.sst` file is downloaded into the local volume staging directory.
2. A new empty local table index is initialized.
3. The engine ingests the table file via `db.Ingest([]string{path})`.
4. Ingestion creates a link into L0 in a single atomic metadata transaction without deserializing or re-indexing individual records.
5. The local index immediately reflects position $P$ and all rows, ready for real-time stream replay of subsequent positions $P+1 \dots$
