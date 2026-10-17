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

package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Well-known and framework Type IDs.
const (
	TypeIDInvalid         uint32 = 0
	TypeIDTypeDefinition  uint32 = 1
	TypeIDTxCommit        uint32 = 2
	TypeIDSnapshotPointer uint32 = 3
	TypeIDPadding         uint32 = 4
	TypeIDOpRecord        uint32 = 5
	TypeIDStreamHeader    uint32 = 6

	// MinReservedTypeID and MaxReservedTypeID define the reserved framework range [7, 15].
	MinReservedTypeID uint32 = 7
	MaxReservedTypeID uint32 = 15

	// MinAppTypeID is the minimum type ID for application-defined types.
	MinAppTypeID uint32 = 16
)

var (
	// ErrInvalidTypeID is returned when a type ID is 0.
	ErrInvalidTypeID = errors.New("invalid type ID 0 (zeroed or torn data)")

	// ErrReservedTypeID is returned when a type ID is within the reserved range [7, 15].
	ErrReservedTypeID = errors.New("reserved type ID (7..15 are reserved for framework)")

	// ErrEmptyPayload is returned when attempting to split an empty payload buffer.
	ErrEmptyPayload = errors.New("empty payload")

	// ErrMalformedVarint is returned when the varint header in a frame cannot be parsed.
	ErrMalformedVarint = errors.New("malformed varint type ID in frame")

	// ErrTypeNotRegistered is returned when an application type ID has not been defined before use.
	ErrTypeNotRegistered = errors.New("type ID used before definition")
)

// ValidateTypeID checks if a type ID is valid according to Layer 1 framing rules.
func ValidateTypeID(typeID uint32) error {
	if typeID == TypeIDInvalid {
		return ErrInvalidTypeID
	}
	if typeID >= MinReservedTypeID && typeID <= MaxReservedTypeID {
		return fmt.Errorf("%w: %d", ErrReservedTypeID, typeID)
	}
	return nil
}

// EncodeFrame encodes a type ID and body into a framed payload:
//
//	payload := varint(type_id) body
func EncodeFrame(typeID uint32, body []byte) ([]byte, error) {
	if err := ValidateTypeID(typeID); err != nil {
		return nil, err
	}
	buf := make([]byte, binary.MaxVarintLen32+len(body))
	n := binary.PutUvarint(buf, uint64(typeID))
	copy(buf[n:], body)
	return buf[:n+len(body)], nil
}

// SplitFrame parses the varint type ID from a payload and returns the type ID and body slice.
func SplitFrame(payload []byte) (typeID uint32, body []byte, err error) {
	if len(payload) == 0 {
		return 0, nil, ErrEmptyPayload
	}
	id, n := binary.Uvarint(payload)
	if n <= 0 {
		return 0, nil, ErrMalformedVarint
	}
	if id > math.MaxUint32 {
		return 0, nil, fmt.Errorf("type ID %d overflows uint32", id)
	}
	tID := uint32(id)
	if err := ValidateTypeID(tID); err != nil {
		return 0, nil, err
	}
	return tID, payload[n:], nil
}
