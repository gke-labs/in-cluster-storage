/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"k8s.io/klog/v2"
)

const (
	// SegmentMagic is the 4-byte header identifier "OFWL".
	SegmentMagic = "OFWL"

	// SegmentVersion is the current WAL segment format version.
	SegmentVersion uint16 = 1

	// SegmentHeaderSize is the fixed size of a segment header (16 bytes).
	SegmentHeaderSize = 16
)

var (
	// SegmentMagicBytes is [4]byte{'O', 'F', 'W', 'L'}.
	SegmentMagicBytes = [4]byte{'O', 'F', 'W', 'L'}

	// ErrInvalidSegmentMagic is returned when a segment file does not start with SegmentMagic.
	ErrInvalidSegmentMagic = errors.New("invalid WAL segment magic")

	// ErrUnsupportedSegmentVersion is returned when a segment file format version is greater than SegmentVersion.
	ErrUnsupportedSegmentVersion = errors.New("unsupported WAL segment version")

	// ErrInvalidSegmentHeaderLength is returned when the header length is less than 8 bytes.
	ErrInvalidSegmentHeaderLength = errors.New("invalid WAL segment header length")

	// ErrTruncatedSegmentHeader is returned when reading a segment file ends before the full header.
	ErrTruncatedSegmentHeader = errors.New("truncated WAL segment header")

	// ErrUnversionedSegment is returned when a segment file lacks a segment header and unversioned recovery is not enabled.
	ErrUnversionedSegment = errors.New("unversioned WAL segment (use --accept-unversioned-segments to recover)")
)

// SegmentHeader represents the fixed 16-byte header at offset 0 of every WAL segment file:
// magic (4 bytes "OFWL") + format_version (uint16) + header_length (uint16) + reserved (8 bytes).
type SegmentHeader struct {
	Magic        [4]byte
	Version      uint16
	HeaderLength uint16
	Reserved     [8]byte
}

// Encode serializes the SegmentHeader to a 16-byte slice.
func (h SegmentHeader) Encode() []byte {
	buf := make([]byte, SegmentHeaderSize)
	copy(buf[0:4], h.Magic[:])
	binary.BigEndian.PutUint16(buf[4:6], h.Version)
	binary.BigEndian.PutUint16(buf[6:8], h.HeaderLength)
	copy(buf[8:16], h.Reserved[:])
	return buf
}

// DefaultSegmentHeader returns a new SegmentHeader with current version and defaults.
func DefaultSegmentHeader() SegmentHeader {
	return SegmentHeader{
		Magic:        SegmentMagicBytes,
		Version:      SegmentVersion,
		HeaderLength: SegmentHeaderSize,
	}
}

// ScanOptions configures options for scanning segment files.
type ScanOptions struct {
	AcceptUnversioned bool
}

// ScanOption configures a ScanOptions.
type ScanOption func(*ScanOptions)

// WithAcceptUnversioned configures whether unversioned segments are accepted for one-time upgrade.
func WithAcceptUnversioned(accept bool) ScanOption {
	return func(o *ScanOptions) {
		o.AcceptUnversioned = accept
	}
}

// ReadSegmentHeader reads and validates the segment header from an io.ReadSeeker.
// If the reader is at EOF with 0 bytes read, it returns (SegmentHeader{}, 0, io.EOF).
// If a valid SegmentHeader is read, it positions the reader at offset header_length
// and returns the header, header_length as int64, and nil.
// If magic is unknown:
//   - If acceptUnversioned is true and the first 4 bytes match ClientMagicBytes or LogMagicBytes,
//     it rewinds the reader to offset 0, logs a warning, and returns (SegmentHeader{}, 0, nil).
//   - If the first 4 bytes match ClientMagicBytes or LogMagicBytes but acceptUnversioned is false,
//     it returns ErrUnversionedSegment.
//   - Otherwise, it returns ErrInvalidSegmentMagic.
func ReadSegmentHeader(r io.ReadSeeker, acceptUnversioned bool) (SegmentHeader, int64, error) {
	var buf [SegmentHeaderSize]byte
	n, err := io.ReadFull(r, buf[:])
	if err != nil {
		if errors.Is(err, io.EOF) {
			return SegmentHeader{}, 0, io.EOF
		}
		if n < 4 {
			return SegmentHeader{}, 0, ErrTruncatedSegmentHeader
		}
	}

	var magic [4]byte
	copy(magic[:], buf[0:4])

	if magic == SegmentMagicBytes {
		if n < SegmentHeaderSize {
			return SegmentHeader{}, 0, ErrTruncatedSegmentHeader
		}
		ver := binary.BigEndian.Uint16(buf[4:6])
		if ver > SegmentVersion {
			return SegmentHeader{}, 0, fmt.Errorf("%w: version %d > current %d", ErrUnsupportedSegmentVersion, ver, SegmentVersion)
		}
		hdrLen := binary.BigEndian.Uint16(buf[6:8])
		if hdrLen < 8 {
			return SegmentHeader{}, 0, fmt.Errorf("%w: %d", ErrInvalidSegmentHeaderLength, hdrLen)
		}
		var reserved [8]byte
		copy(reserved[:], buf[8:16])

		// If header length is larger than SegmentHeaderSize, seek to header length
		if int64(hdrLen) != int64(n) {
			if _, seekErr := r.Seek(int64(hdrLen), io.SeekStart); seekErr != nil {
				return SegmentHeader{}, 0, seekErr
			}
		}

		return SegmentHeader{
			Magic:        magic,
			Version:      ver,
			HeaderLength: hdrLen,
			Reserved:     reserved,
		}, int64(hdrLen), nil
	}

	// Unknown magic: check if this is an unversioned segment file
	if magic == ClientMagicBytes || magic == LogMagicBytes {
		if acceptUnversioned {
			klog.Warning("accepting unversioned segment file (--accept-unversioned-segments)")
			if _, seekErr := r.Seek(0, io.SeekStart); seekErr != nil {
				return SegmentHeader{}, 0, seekErr
			}
			return SegmentHeader{}, 0, nil
		}
		return SegmentHeader{}, 0, fmt.Errorf("%w: starts with record magic %q", ErrUnversionedSegment, string(magic[:]))
	}

	return SegmentHeader{}, 0, fmt.Errorf("%w: got %q, expected %q", ErrInvalidSegmentMagic, string(magic[:]), SegmentMagic)
}
