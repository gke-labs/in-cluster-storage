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
	"bytes"
	"errors"
	"testing"
)

func TestValidateTypeID(t *testing.T) {
	tests := []struct {
		name    string
		typeID  uint32
		wantErr error
	}{
		{name: "invalid zero", typeID: 0, wantErr: ErrInvalidTypeID},
		{name: "TypeDefinition (1)", typeID: 1, wantErr: nil},
		{name: "TxCommit (2)", typeID: 2, wantErr: nil},
		{name: "SnapshotPointer (3)", typeID: 3, wantErr: nil},
		{name: "Padding (4)", typeID: 4, wantErr: nil},
		{name: "OpRecord (5)", typeID: 5, wantErr: nil},
		{name: "StreamHeader (6)", typeID: 6, wantErr: nil},
		{name: "reserved 7", typeID: 7, wantErr: ErrReservedTypeID},
		{name: "reserved 10", typeID: 10, wantErr: ErrReservedTypeID},
		{name: "reserved 15", typeID: 15, wantErr: ErrReservedTypeID},
		{name: "app type min (16)", typeID: 16, wantErr: nil},
		{name: "app type 127", typeID: 127, wantErr: nil},
		{name: "app type 128 (2-byte varint)", typeID: 128, wantErr: nil},
		{name: "app type 16383", typeID: 16383, wantErr: nil},
		{name: "app type large", typeID: 1000000, wantErr: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTypeID(tt.typeID)
			if tt.wantErr != nil {
				if err == nil || !errors.Is(err, tt.wantErr) {
					t.Fatalf("ValidateTypeID(%d) = %v, want error wrapping %v", tt.typeID, err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("ValidateTypeID(%d) unexpected error: %v", tt.typeID, err)
			}
		})
	}
}

func TestEncodeAndSplitFrame(t *testing.T) {
	testCases := []struct {
		name   string
		typeID uint32
		body   []byte
	}{
		{name: "TypeDefinition with body", typeID: TypeIDTypeDefinition, body: []byte("proto-type-def")},
		{name: "TxCommit empty body", typeID: TypeIDTxCommit, body: []byte{}},
		{name: "SnapshotPointer", typeID: TypeIDSnapshotPointer, body: []byte("snapshot-pointer")},
		{name: "Padding", typeID: TypeIDPadding, body: bytes.Repeat([]byte{0x00}, 64)},
		{name: "OpRecord (5)", typeID: TypeIDOpRecord, body: []byte("op-record-body")},
		{name: "App type 16", typeID: 16, body: []byte("row-change-data")},
		{name: "App type 128", typeID: 128, body: []byte("multi-byte-varint-data")},
		{name: "App type 16384", typeID: 16384, body: []byte("large-varint-data")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			framed, err := EncodeFrame(tc.typeID, tc.body)
			if err != nil {
				t.Fatalf("EncodeFrame(%d) error: %v", tc.typeID, err)
			}

			gotID, gotBody, err := SplitFrame(framed)
			if err != nil {
				t.Fatalf("SplitFrame error: %v", err)
			}
			if gotID != tc.typeID {
				t.Errorf("SplitFrame got typeID %d, want %d", gotID, tc.typeID)
			}
			if !bytes.Equal(gotBody, tc.body) {
				t.Errorf("SplitFrame got body %q, want %q", gotBody, tc.body)
			}
		})
	}
}

func TestFrameRejections(t *testing.T) {
	// Encode invalid ID 0
	if _, err := EncodeFrame(0, []byte("body")); !errors.Is(err, ErrInvalidTypeID) {
		t.Errorf("EncodeFrame(0) got error %v, want ErrInvalidTypeID", err)
	}

	// Encode reserved IDs 7..15
	for id := uint32(7); id <= 15; id++ {
		if _, err := EncodeFrame(id, []byte("body")); !errors.Is(err, ErrReservedTypeID) {
			t.Errorf("EncodeFrame(%d) got error %v, want ErrReservedTypeID", id, err)
		}
	}

	// SplitFrame on empty payload
	if _, _, err := SplitFrame(nil); !errors.Is(err, ErrEmptyPayload) {
		t.Errorf("SplitFrame(nil) got error %v, want ErrEmptyPayload", err)
	}

	// SplitFrame on invalid 0 type_id wire
	zeroFrame := []byte{0x00, 0x01, 0x02}
	if _, _, err := SplitFrame(zeroFrame); !errors.Is(err, ErrInvalidTypeID) {
		t.Errorf("SplitFrame([0]) got error %v, want ErrInvalidTypeID", err)
	}

	// SplitFrame on reserved type_id 7 wire
	reservedFrame := []byte{0x07, 0x01, 0x02}
	if _, _, err := SplitFrame(reservedFrame); !errors.Is(err, ErrReservedTypeID) {
		t.Errorf("SplitFrame([7]) got error %v, want ErrReservedTypeID", err)
	}

	// SplitFrame on malformed varint (overflowing varint)
	malformed := bytes.Repeat([]byte{0x80}, 12)
	if _, _, err := SplitFrame(malformed); !errors.Is(err, ErrMalformedVarint) {
		t.Errorf("SplitFrame(malformed) got error %v, want ErrMalformedVarint", err)
	}
}
