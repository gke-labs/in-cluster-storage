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
	"encoding/binary"
	"errors"
	"fmt"
)

// Reserved Type ID 0 keys for table metadata.
// In the table format keyspace, all keys are formatted as:
//
//	varint(type_id) ‖ canonical SDS key bytes
//
// For metadata keys, type_id is 0, which encodes to a single byte 0x00.
// Application types have type_id >= 16 (encoded as varints with first byte >= 0x10).
// Because 0x00 < 0x10, metadata keys sort strictly before all application table entries.
var (
	// KeyPosition stores the 8-byte big-endian applied stream sequence position.
	KeyPosition = []byte{0x00, 'p', 'o', 's', 'i', 't', 'i', 'o', 'n'}

	// KeyRegistry stores the protobuf-serialized sdsv1.StreamTypes containing type definitions.
	KeyRegistry = []byte{0x00, 'r', 'e', 'g', 'i', 's', 't', 'r', 'y'}

	// KeyStreamID stores the UTF-8 bytes of the stream ID.
	KeyStreamID = []byte{0x00, 's', 't', 'r', 'e', 'a', 'm', '_', 'i', 'd'}
)

// EncodeTypePrefix returns the varint-encoded type prefix for a table type ID.
func EncodeTypePrefix(typeID uint32) []byte {
	return binary.AppendUvarint(nil, uint64(typeID))
}

// EncodeEntryKey encodes a table entry key from a table type ID and canonical SDS key bytes:
//
//	varint(type_id) ‖ canonical SDS key bytes
func EncodeEntryKey(typeID uint32, rawKey []byte) []byte {
	prefix := EncodeTypePrefix(typeID)
	out := make([]byte, len(prefix)+len(rawKey))
	copy(out, prefix)
	copy(out[len(prefix):], rawKey)
	return out
}

// DecodeEntryKey decodes an entry key into its table type ID and canonical SDS key bytes.
func DecodeEntryKey(k []byte) (uint32, []byte, error) {
	v, n := binary.Uvarint(k)
	if n <= 0 {
		return 0, nil, fmt.Errorf("invalid varint in entry key: code=%d", n)
	}
	return uint32(v), k[n:], nil
}

// EncodePosition encodes a 64-bit stream sequence position into an 8-byte big-endian slice.
func EncodePosition(pos uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, pos)
	return b
}

// DecodePosition decodes an 8-byte big-endian stream sequence position.
func DecodePosition(b []byte) (uint64, error) {
	if len(b) < 8 {
		return 0, errors.New("position bytes too short (expected 8 bytes)")
	}
	return binary.BigEndian.Uint64(b[:8]), nil
}

// prefixLimit computes the exclusive upper-bound key for prefix-seeking in Pebble iterators.
func prefixLimit(prefix []byte) []byte {
	limit := make([]byte, len(prefix))
	copy(limit, prefix)
	for i := len(limit) - 1; i >= 0; i-- {
		if limit[i] < 0xff {
			limit[i]++
			return limit[:i+1]
		}
	}
	return nil
}
