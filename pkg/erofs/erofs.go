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

package erofs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	// SuperMagic is the unique magic number identifying an EROFS filesystem (0xE0F5E1E2).
	SuperMagic = 0xE0F5E1E2
	// SuperOffset is the byte offset of the superblock from the beginning of the image (1024 bytes).
	SuperOffset = 1024
	// SuperSize is the standard size of the EROFS superblock on-disk representation (128 bytes).
	SuperSize = 128
	// SlotSize is the unit slot size used to align and address inodes (32 bytes).
	SlotSize = 32
	// BlockSize4K is the default filesystem page and block alignment size (4096 bytes).
	BlockSize4K = 4096
)

const (
	DatalayoutFlatPlain  = 0
	DatalayoutFlatInline = 2
)

const (
	FTUnknown = 0
	FTRegFile = 1
	FTDir     = 2
	FTChrDev  = 3
	FTBlkDev  = 4
	FTFifo    = 5
	FTSock    = 6
	FTSymlink = 7
)

const (
	S_IFMT  = 0170000
	S_IFREG = 0100000
	S_IFDIR = 0040000
	S_IFLNK = 0120000
)

type ErrorCode int

const (
	ErrUnknown ErrorCode = iota
	ErrInvalidSuperblock
	ErrInvalidInode
	ErrInvalidDirectoryBlock
	ErrPathTraversal
	ErrCycleDetected
	ErrMaxDepthExceeded
)

type ErofsError struct {
	Code    ErrorCode
	Message string
	Err     error
}

func (e *ErofsError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *ErofsError) Unwrap() error {
	return e.Err
}

func IsInvalidSuperblockError(err error) bool {
	var erofsErr *ErofsError
	if errors.As(err, &erofsErr) {
		return erofsErr.Code == ErrInvalidSuperblock
	}
	return false
}

func IsInvalidInodeError(err error) bool {
	var erofsErr *ErofsError
	if errors.As(err, &erofsErr) {
		return erofsErr.Code == ErrInvalidInode
	}
	return false
}

func IsInvalidDirectoryBlockError(err error) bool {
	var erofsErr *ErofsError
	if errors.As(err, &erofsErr) {
		return erofsErr.Code == ErrInvalidDirectoryBlock
	}
	return false
}

func IsPathTraversalError(err error) bool {
	var erofsErr *ErofsError
	if errors.As(err, &erofsErr) {
		return erofsErr.Code == ErrPathTraversal
	}
	return false
}

func IsCycleDetectedError(err error) bool {
	var erofsErr *ErofsError
	if errors.As(err, &erofsErr) {
		return erofsErr.Code == ErrCycleDetected
	}
	return false
}

func IsMaxDepthExceededError(err error) bool {
	var erofsErr *ErofsError
	if errors.As(err, &erofsErr) {
		return erofsErr.Code == ErrMaxDepthExceeded
	}
	return false
}

// Superblock represents the EROFS superblock.
type Superblock struct {
	Magic           uint32
	Checksum        uint32
	FeatureCompat   uint32
	Blkszbits       uint8
	SbExtslots      uint8
	RootNID         uint16
	Inodes          uint64
	BuildTime       uint64
	BuildTimeNsec   uint32
	Blocks          uint32
	MetaBlkaddr     uint32
	XattrBlkaddr    uint32
	UUID            [16]byte
	VolumeName      [16]byte
	FeatureIncompat uint32
	ExtraDevices    uint16
	DevtSlotoff     uint16
	Dirblkbits      uint8
	XattrPrefixCnt  uint8
	XattrPrefixSt   uint32
	PackedNID       uint64
	MetaNID         uint64
	RootNID8b       uint64
	Reserved2       [10]byte
}

// Inode represents an EROFS inode.
type Inode struct {
	NID         uint64
	Format      uint16
	XattrICount uint16
	Mode        uint16
	Size        uint64
	RawBlkaddr  uint32
	Ino         uint32
	UID         uint32
	GID         uint32
	Mtime       uint64
	MtimeNsec   uint32
	Nlink       uint32
	Version     uint16 // 0 for compact, 1 for extended
	DataLayout  uint16
}

// Dirent represents a directory entry.
type Dirent struct {
	NID      uint64
	Name     string
	FileType uint8
}

// ReadSuperblock parses and thoroughly verifies the EROFS superblock from the given ReaderAt.
func ReadSuperblock(r io.ReaderAt) (*Superblock, error) {
	buf := make([]byte, SuperSize)
	n, err := r.ReadAt(buf, SuperOffset)
	if err != nil && err != io.EOF {
		return nil, &ErofsError{Code: ErrInvalidSuperblock, Message: "failed to read superblock", Err: err}
	}
	if n < SuperSize {
		return nil, &ErofsError{Code: ErrInvalidSuperblock, Message: fmt.Sprintf("truncated superblock, read only %d bytes", n)}
	}

	sb := &Superblock{}
	sb.Magic = binary.LittleEndian.Uint32(buf[0:4])
	if sb.Magic != SuperMagic {
		return nil, &ErofsError{Code: ErrInvalidSuperblock, Message: fmt.Sprintf("invalid EROFS magic: got 0x%x, expected 0x%x", sb.Magic, SuperMagic)}
	}

	sb.Checksum = binary.LittleEndian.Uint32(buf[4:8])
	if sb.Checksum != 0 {
		// Zero out the checksum field to compute the expected CRC32
		checksumBuf := make([]byte, SuperSize)
		copy(checksumBuf, buf)
		checksumBuf[4] = 0
		checksumBuf[5] = 0
		checksumBuf[6] = 0
		checksumBuf[7] = 0
		expectedSum := crc32.ChecksumIEEE(checksumBuf)
		if sb.Checksum != expectedSum {
			return nil, &ErofsError{Code: ErrInvalidSuperblock, Message: fmt.Sprintf("superblock checksum mismatch: got 0x%x, expected 0x%x", sb.Checksum, expectedSum)}
		}
	}

	sb.FeatureCompat = binary.LittleEndian.Uint32(buf[8:12])
	sb.Blkszbits = buf[12]
	sb.SbExtslots = buf[13]
	sb.RootNID = binary.LittleEndian.Uint16(buf[14:16])
	sb.Inodes = binary.LittleEndian.Uint64(buf[16:24])
	sb.BuildTime = binary.LittleEndian.Uint64(buf[24:32])
	sb.BuildTimeNsec = binary.LittleEndian.Uint32(buf[32:36])
	sb.Blocks = binary.LittleEndian.Uint32(buf[36:40])
	sb.MetaBlkaddr = binary.LittleEndian.Uint32(buf[40:44])

	// Verify superblock fields
	if sb.Blkszbits != 12 { // 12 bits = 4096 bytes (the only standard/supported block size)
		return nil, &ErofsError{Code: ErrInvalidSuperblock, Message: fmt.Sprintf("unsupported block size bits %d (expected 12)", sb.Blkszbits)}
	}
	if sb.Blocks == 0 {
		return nil, &ErofsError{Code: ErrInvalidSuperblock, Message: "invalid superblock: zero blocks count"}
	}
	if sb.Inodes == 0 {
		return nil, &ErofsError{Code: ErrInvalidSuperblock, Message: "invalid superblock: zero inodes count"}
	}
	if sb.MetaBlkaddr == 0 {
		return nil, &ErofsError{Code: ErrInvalidSuperblock, Message: "invalid superblock: zero meta block address"}
	}

	sb.XattrBlkaddr = binary.LittleEndian.Uint32(buf[44:48])
	copy(sb.UUID[:], buf[48:64])
	copy(sb.VolumeName[:], buf[64:80])
	sb.FeatureIncompat = binary.LittleEndian.Uint32(buf[80:84])
	sb.ExtraDevices = binary.LittleEndian.Uint16(buf[84:86])
	sb.DevtSlotoff = binary.LittleEndian.Uint16(buf[86:88])
	sb.Dirblkbits = buf[88]
	sb.XattrPrefixCnt = buf[89]
	sb.XattrPrefixSt = binary.LittleEndian.Uint32(buf[90:94])
	sb.PackedNID = binary.LittleEndian.Uint64(buf[94:102])
	sb.MetaNID = binary.LittleEndian.Uint64(buf[102:110])
	sb.RootNID8b = binary.LittleEndian.Uint64(buf[110:118])
	copy(sb.Reserved2[:], buf[118:128])

	return sb, nil
}

// BlockSize returns the filesystem block size in bytes.
func (sb *Superblock) BlockSize() uint32 {
	if sb.Blkszbits == 0 {
		return BlockSize4K
	}
	return 1 << sb.Blkszbits
}

// GetRootNID retrieves the root directory NID, preferring the 64-bit field.
func (sb *Superblock) GetRootNID() uint64 {
	if sb.RootNID8b != 0 {
		return sb.RootNID8b
	}
	return uint64(sb.RootNID)
}

// InodeOffset returns the absolute byte offset of the given NID on-disk.
func (sb *Superblock) InodeOffset(nid uint64) int64 {
	return int64(sb.MetaBlkaddr)*int64(sb.BlockSize()) + int64(nid)*SlotSize
}

// ReadInode reads and parses an inode from the EROFS image by NID.
func ReadInode(r io.ReaderAt, sb *Superblock, nid uint64) (*Inode, error) {
	offset := sb.InodeOffset(nid)
	buf := make([]byte, 64)
	n, err := r.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read inode for NID %d: %w", nid, err)
	}
	if n < 32 {
		return nil, fmt.Errorf("truncated inode for NID %d, read %d bytes", nid, n)
	}

	format := binary.LittleEndian.Uint16(buf[0:2])
	version := (format >> 0) & 0x01
	dataLayout := (format >> 1) & 0x07

	inode := &Inode{
		NID:        nid,
		Format:     format,
		Version:    version,
		DataLayout: dataLayout,
	}

	if version == 0 {
		// Compact Inode (32 bytes)
		inode.XattrICount = binary.LittleEndian.Uint16(buf[2:4])
		inode.Mode = binary.LittleEndian.Uint16(buf[4:6])
		inode.Nlink = uint32(binary.LittleEndian.Uint16(buf[6:8]))
		inode.Size = uint64(binary.LittleEndian.Uint32(buf[8:12]))
		// buf[12:16] is reserved
		inode.RawBlkaddr = binary.LittleEndian.Uint32(buf[16:20])
		inode.Ino = binary.LittleEndian.Uint32(buf[20:24])
		inode.UID = uint32(binary.LittleEndian.Uint16(buf[24:26]))
		inode.GID = uint32(binary.LittleEndian.Uint16(buf[26:28]))
	} else {
		// Extended Inode (64 bytes)
		if n < 64 {
			return nil, fmt.Errorf("truncated extended inode for NID %d, read %d bytes", nid, n)
		}
		inode.XattrICount = binary.LittleEndian.Uint16(buf[2:4])
		inode.Mode = binary.LittleEndian.Uint16(buf[4:6])
		inode.Size = binary.LittleEndian.Uint64(buf[8:16])
		inode.RawBlkaddr = binary.LittleEndian.Uint32(buf[16:20])
		inode.Ino = binary.LittleEndian.Uint32(buf[20:24])
		inode.UID = binary.LittleEndian.Uint32(buf[24:28])
		inode.GID = binary.LittleEndian.Uint32(buf[28:32])
		inode.Mtime = binary.LittleEndian.Uint64(buf[32:40])
		inode.MtimeNsec = binary.LittleEndian.Uint32(buf[40:44])
		inode.Nlink = binary.LittleEndian.Uint32(buf[44:48])
	}

	return inode, nil
}

// InlineOffset returns the start offset of inline data relative to the inode start.
func (inode *Inode) InlineOffset() uint32 {
	var inlineOffset uint32
	if inode.Version == 0 {
		inlineOffset = 32
	} else {
		inlineOffset = 64
	}
	if inode.XattrICount > 0 {
		inlineOffset += 8 + 4*uint32(inode.XattrICount)
	}
	return inlineOffset
}

// ReadInlineData reads inline tail data of flat-inline inodes.
func (inode *Inode) ReadInlineData(r io.ReaderAt, sb *Superblock) ([]byte, error) {
	if inode.DataLayout != DatalayoutFlatInline {
		return nil, fmt.Errorf("inode is not flat inline")
	}

	inodeOffset := sb.InodeOffset(inode.NID)
	inlineOffset := inode.InlineOffset()

	blockSize := uint64(sb.BlockSize())
	blockStart := (uint64(inodeOffset) / blockSize) * blockSize
	blockEnd := blockStart + blockSize

	tailSize := inode.Size % blockSize
	if inode.Size < blockSize {
		tailSize = inode.Size
	}

	if tailSize == 0 {
		return nil, nil
	}

	inlineStart := uint64(inodeOffset) + uint64(inlineOffset)
	if inlineStart+tailSize > blockEnd {
		return nil, fmt.Errorf("malicious layout: inline data overflows block boundary")
	}

	buf := make([]byte, tailSize)
	_, err := r.ReadAt(buf, int64(inlineStart))
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read inline data: %w", err)
	}

	return buf, nil
}

// DataReader returns an io.Reader to stream file data, avoiding full in-memory buffering.
func (inode *Inode) DataReader(r io.ReaderAt, sb *Superblock) (io.Reader, error) {
	blockSize := uint64(sb.BlockSize())

	if inode.DataLayout == DatalayoutFlatPlain {
		totalBlocks := (inode.Size + blockSize - 1) / blockSize
		if totalBlocks == 0 {
			return bytes.NewReader(nil), nil
		}

		if uint64(inode.RawBlkaddr)+totalBlocks > uint64(sb.Blocks) {
			return nil, fmt.Errorf("block address out of bounds: raw_blkaddr %d, totalBlocks %d, image total blocks %d", inode.RawBlkaddr, totalBlocks, sb.Blocks)
		}

		return io.NewSectionReader(r, int64(inode.RawBlkaddr)*int64(blockSize), int64(inode.Size)), nil

	} else if inode.DataLayout == DatalayoutFlatInline {
		numBlocks := inode.Size / blockSize
		tailSize := inode.Size % blockSize

		var readers []io.Reader
		if numBlocks > 0 {
			if uint64(inode.RawBlkaddr)+numBlocks > uint64(sb.Blocks) {
				return nil, fmt.Errorf("block address out of bounds: raw_blkaddr %d, blocks %d, image total blocks %d", inode.RawBlkaddr, numBlocks, sb.Blocks)
			}
			readers = append(readers, io.NewSectionReader(r, int64(inode.RawBlkaddr)*int64(blockSize), int64(numBlocks*blockSize)))
		}

		if tailSize > 0 {
			tail, err := inode.ReadInlineData(r, sb)
			if err != nil {
				return nil, err
			}
			readers = append(readers, bytes.NewReader(tail))
		}

		if len(readers) == 0 {
			return bytes.NewReader(nil), nil
		}
		return io.MultiReader(readers...), nil
	}

	return nil, fmt.Errorf("unsupported EROFS data layout: %d", inode.DataLayout)
}

// ReadData retrieves the entire data payload for the given inode by fully reading its DataReader.
func (inode *Inode) ReadData(r io.ReaderAt, sb *Superblock) ([]byte, error) {
	reader, err := inode.DataReader(r, sb)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(reader)
}

// ParseDirectoryBlock parses directory entries from a block buffer.
func ParseDirectoryBlock(buf []byte) ([]Dirent, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	if len(buf) < 12 {
		return nil, &ErofsError{Code: ErrInvalidDirectoryBlock, Message: fmt.Sprintf("directory block too small: %d bytes", len(buf))}
	}

	firstNameoff := binary.LittleEndian.Uint16(buf[8:10])
	if firstNameoff == 0 {
		return nil, nil
	}
	if firstNameoff%12 != 0 {
		return nil, &ErofsError{Code: ErrInvalidDirectoryBlock, Message: fmt.Sprintf("invalid directory block structure: first nameoff %d is not a multiple of 12", firstNameoff)}
	}

	entryCount := int(firstNameoff / 12)
	if entryCount*12 > len(buf) {
		return nil, &ErofsError{Code: ErrInvalidDirectoryBlock, Message: fmt.Sprintf("invalid directory layout: entryCount %d overflows block size %d", entryCount, len(buf))}
	}

	dirents := make([]Dirent, 0, entryCount)
	for i := 0; i < entryCount; i++ {
		offset := i * 12
		nid := binary.LittleEndian.Uint64(buf[offset : offset+8])
		nameoff := binary.LittleEndian.Uint16(buf[offset+8 : offset+10])
		fileType := buf[offset+10]
		reserved := buf[offset+11]

		if reserved != 0 {
			return nil, &ErofsError{Code: ErrInvalidDirectoryBlock, Message: "invalid directory entry: reserved byte is non-zero"}
		}

		var nameLen int
		if i < entryCount-1 {
			nextNameoff := binary.LittleEndian.Uint16(buf[offset+12+8 : offset+12+10])
			if nextNameoff < nameoff {
				return nil, &ErofsError{Code: ErrInvalidDirectoryBlock, Message: "invalid directory layout: non-monotonic nameoff values"}
			}
			nameLen = int(nextNameoff - nameoff)
		} else {
			nameLen = len(buf) - int(nameoff)
		}

		if int(nameoff)+nameLen > len(buf) {
			return nil, &ErofsError{Code: ErrInvalidDirectoryBlock, Message: fmt.Sprintf("directory filename bounds exceeded: nameoff %d, len %d, block size %d", nameoff, nameLen, len(buf))}
		}

		nameBytes := buf[nameoff : int(nameoff)+nameLen]
		if i == entryCount-1 {
			if idx := bytes.IndexByte(nameBytes, 0); idx != -1 {
				nameBytes = nameBytes[:idx]
			}
		}
		name := string(nameBytes)

		if bytes.Contains(nameBytes, []byte{0}) {
			return nil, &ErofsError{Code: ErrPathTraversal, Message: "malicious directory entry: filename contains null byte"}
		}
		if bytes.Contains(nameBytes, []byte{'/'}) {
			return nil, &ErofsError{Code: ErrPathTraversal, Message: "malicious directory entry: filename contains path separator"}
		}
		if !utf8.ValidString(name) {
			return nil, &ErofsError{Code: ErrPathTraversal, Message: "invalid directory entry: filename is not valid UTF-8"}
		}
		if name == "" {
			return nil, &ErofsError{Code: ErrPathTraversal, Message: "invalid directory entry: empty filename"}
		}

		dirents = append(dirents, Dirent{
			NID:      nid,
			Name:     name,
			FileType: fileType,
		})
	}

	return dirents, nil
}

// Fsck recursively traverses and validates the filesystem. It prevents loops, path traversals, and corruptions.
func Fsck(r io.ReaderAt) error {
	sb, err := ReadSuperblock(r)
	if err != nil {
		return err
	}

	visited := make(map[uint64]bool)
	activeStack := make(map[uint64]bool)
	maxDepth := 100
	totalInodesParsed := 0

	var traverse func(nid uint64, depth int) error
	traverse = func(nid uint64, depth int) error {
		if depth > maxDepth {
			return &ErofsError{Code: ErrMaxDepthExceeded, Message: fmt.Sprintf("directory depth limit exceeded (max %d)", maxDepth)}
		}

		if activeStack[nid] {
			return &ErofsError{Code: ErrCycleDetected, Message: fmt.Sprintf("malicious loop detected at NID %d", nid)}
		}
		if visited[nid] {
			return nil
		}
		visited[nid] = true
		activeStack[nid] = true
		defer func() { activeStack[nid] = false }()

		totalInodesParsed++

		if sb.Inodes > 0 && uint64(totalInodesParsed) > sb.Inodes+100 {
			return &ErofsError{Code: ErrInvalidInode, Message: fmt.Sprintf("parsed more inodes (%d) than declared in superblock (%d)", totalInodesParsed, sb.Inodes)}
		}

		inode, err := ReadInode(r, sb, nid)
		if err != nil {
			return &ErofsError{Code: ErrInvalidInode, Message: fmt.Sprintf("failed to read inode for NID %d", nid), Err: err}
		}

		if inode.Version > 1 {
			return &ErofsError{Code: ErrInvalidInode, Message: fmt.Sprintf("invalid inode version %d for NID %d", inode.Version, nid)}
		}
		if inode.DataLayout != DatalayoutFlatPlain && inode.DataLayout != DatalayoutFlatInline {
			return &ErofsError{Code: ErrInvalidInode, Message: fmt.Sprintf("unsupported data layout %d for NID %d", inode.DataLayout, nid)}
		}

		if (inode.Mode & S_IFMT) == S_IFDIR {
			data, err := inode.ReadData(r, sb)
			if err != nil {
				return &ErofsError{Code: ErrInvalidDirectoryBlock, Message: fmt.Sprintf("corrupt directory data for NID %d", nid), Err: err}
			}

			blockSize := int(sb.BlockSize())
			if len(data) > 0 {
				for offset := 0; offset < len(data); offset += blockSize {
					end := offset + blockSize
					if end > len(data) {
						end = len(data)
					}
					blockBuf := data[offset:end]
					dirents, err := ParseDirectoryBlock(blockBuf)
					if err != nil {
						return err
					}

					for _, de := range dirents {
						if de.Name == "." || de.Name == ".." {
							continue
						}

						if err := traverse(de.NID, depth+1); err != nil {
							return err
						}
					}
				}
			}
		} else if (inode.Mode & S_IFMT) == S_IFREG {
			_, err := inode.ReadData(r, sb)
			if err != nil {
				return &ErofsError{Code: ErrInvalidInode, Message: fmt.Sprintf("corrupt regular file data for NID %d", nid), Err: err}
			}
		} else if (inode.Mode & S_IFMT) == S_IFLNK {
			_, err := inode.ReadData(r, sb)
			if err != nil {
				return &ErofsError{Code: ErrInvalidInode, Message: fmt.Sprintf("corrupt symlink data for NID %d", nid), Err: err}
			}
		}

		return nil
	}

	rootNID := sb.GetRootNID()
	if err := traverse(rootNID, 1); err != nil {
		return err
	}

	return nil
}

// Reader provides high-level APIs to query and list files in EROFS image.
type Reader struct {
	r  io.ReaderAt
	sb *Superblock
}

// NewReader instantiates a Reader.
func NewReader(r io.ReaderAt) (*Reader, error) {
	sb, err := ReadSuperblock(r)
	if err != nil {
		return nil, err
	}
	return &Reader{r: r, sb: sb}, nil
}

// ReadFileContent returns an io.Reader to stream the content of a regular/symlink file.
func (reader *Reader) ReadFileContent(nid uint64) (io.Reader, error) {
	inode, err := ReadInode(reader.r, reader.sb, nid)
	if err != nil {
		return nil, err
	}
	return inode.DataReader(reader.r, reader.sb)
}

// ListDirectory lists all directory entries.
func (reader *Reader) ListDirectory(nid uint64) ([]Dirent, error) {
	inode, err := ReadInode(reader.r, reader.sb, nid)
	if err != nil {
		return nil, err
	}
	if (inode.Mode & S_IFMT) != S_IFDIR {
		return nil, errors.New("not a directory")
	}

	data, err := inode.ReadData(reader.r, reader.sb)
	if err != nil {
		return nil, err
	}

	var allDirents []Dirent
	blockSize := int(reader.sb.BlockSize())
	if len(data) > 0 {
		for offset := 0; offset < len(data); offset += blockSize {
			end := offset + blockSize
			if end > len(data) {
				end = len(data)
			}
			blockBuf := data[offset:end]
			dirents, err := ParseDirectoryBlock(blockBuf)
			if err != nil {
				return nil, err
			}
			allDirents = append(allDirents, dirents...)
		}
	}
	return allDirents, nil
}

// BuildDirectoryBlock packs directory entries into an EROFS block buffer.
func BuildDirectoryBlock(dirents []Dirent, size int) ([]byte, error) {
	buf := make([]byte, size)
	if len(dirents) == 0 {
		return buf, nil
	}

	entryCount := len(dirents)
	nameStart := entryCount * 12

	currentOffset := nameStart
	for i, de := range dirents {
		offset := i * 12
		binary.LittleEndian.PutUint64(buf[offset:offset+8], de.NID)
		binary.LittleEndian.PutUint16(buf[offset+8:offset+10], uint16(currentOffset))
		buf[offset+10] = de.FileType
		buf[offset+11] = 0 // reserved

		if strings.Contains(de.Name, "\x00") {
			return nil, fmt.Errorf("invalid filename contains null byte: %q", de.Name)
		}
		if strings.Contains(de.Name, "/") {
			return nil, fmt.Errorf("invalid filename contains path separator: %q", de.Name)
		}
		if de.Name == "" {
			return nil, fmt.Errorf("invalid filename is empty")
		}
		if !utf8.ValidString(de.Name) {
			return nil, fmt.Errorf("invalid filename is not valid UTF-8: %q", de.Name)
		}

		nameBytes := []byte(de.Name)
		if currentOffset+len(nameBytes) > size {
			return nil, fmt.Errorf("directory entries and filenames overflow block size")
		}
		copy(buf[currentOffset:currentOffset+len(nameBytes)], nameBytes)
		currentOffset += len(nameBytes)
	}

	return buf, nil
}

// Node represents a node in the virtual filesystem to be written.
type Node interface {
	Name() string
	IsDir() bool
	Mode() uint16
	UID() uint32
	GID() uint32
	Mtime() uint64
	Size() uint64
	// Open returns an io.ReadCloser to read the file/symlink content.
	// For directories, it is not used.
	Open() (io.ReadCloser, error)
	// Children returns a sorted slice of child nodes of a directory.
	// For non-directories, it is not used.
	Children() ([]Node, error)
}

// memoryNode implements the Node interface for virtual in-memory trees.
type memoryNode struct {
	name     string
	isDir    bool
	mode     uint16
	uid      uint32
	gid      uint32
	mtime    uint64
	content  []byte
	children []Node
}

func (m *memoryNode) Name() string  { return m.name }
func (m *memoryNode) IsDir() bool   { return m.isDir }
func (m *memoryNode) Mode() uint16  { return m.mode }
func (m *memoryNode) UID() uint32   { return m.uid }
func (m *memoryNode) GID() uint32   { return m.gid }
func (m *memoryNode) Mtime() uint64 { return m.mtime }
func (m *memoryNode) Size() uint64  { return uint64(len(m.content)) }
func (m *memoryNode) Children() ([]Node, error) {
	if !m.isDir {
		return nil, errors.New("not a directory")
	}
	return m.children, nil
}

func (m *memoryNode) Open() (io.ReadCloser, error) {
	if m.isDir {
		return nil, errors.New("cannot open directory")
	}
	return io.NopCloser(bytes.NewReader(m.content)), nil
}

// fileSystemNode implements the Node interface for physical host directories.
type fileSystemNode struct {
	name   string
	path   string
	isDir  bool
	mode   uint16
	mtime  uint64
	size   uint64
	target string
}

// newFileSystemNode instantiates and caches file system information, calling Lstat exactly once.
func newFileSystemNode(name string, path string) (Node, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	m := uint32(st.Mode())
	var mode uint16 = uint16(m & 0777)
	isDir := st.IsDir()
	if isDir {
		mode |= S_IFDIR
	} else if (m & uint32(os.ModeSymlink)) != 0 {
		mode |= S_IFLNK
	} else {
		mode |= S_IFREG
	}

	node := &fileSystemNode{
		name:  name,
		path:  path,
		isDir: isDir,
		mode:  mode,
		mtime: uint64(st.ModTime().Unix()),
		size:  uint64(st.Size()),
	}

	if (mode & S_IFMT) == S_IFLNK {
		target, err := os.Readlink(path)
		if err != nil {
			return nil, err
		}
		node.target = target
		node.size = uint64(len(target))
	}

	return node, nil
}

// NewFileSystemNode instantiates a local directory-backed EROFS compiler Node.
func NewFileSystemNode(name string, path string) Node {
	n, err := newFileSystemNode(name, path)
	if err != nil {
		return nil
	}
	return n
}

func (f *fileSystemNode) Name() string  { return f.name }
func (f *fileSystemNode) IsDir() bool   { return f.isDir }
func (f *fileSystemNode) Mode() uint16  { return f.mode }
func (f *fileSystemNode) UID() uint32   { return 0 }
func (f *fileSystemNode) GID() uint32   { return 0 }
func (f *fileSystemNode) Mtime() uint64 { return f.mtime }
func (f *fileSystemNode) Size() uint64  { return f.size }

func (f *fileSystemNode) Open() (io.ReadCloser, error) {
	if (f.mode & S_IFMT) == S_IFLNK {
		return io.NopCloser(strings.NewReader(f.target)), nil
	}
	return os.Open(f.path)
}

func (f *fileSystemNode) Children() ([]Node, error) {
	if !f.isDir {
		return nil, errors.New("not a directory")
	}
	entries, err := os.ReadDir(f.path)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	var children []Node
	for _, name := range names {
		childNode, err := newFileSystemNode(name, filepath.Join(f.path, name))
		if err != nil {
			return nil, err
		}
		children = append(children, childNode)
	}
	return children, nil
}

type treeBuilderNode struct {
	name     string
	isDir    bool
	mode     uint16
	uid      uint32
	gid      uint32
	mtime    uint64
	content  []byte
	children map[string]*treeBuilderNode
}

func convertToMemoryNode(b *treeBuilderNode) *memoryNode {
	m := &memoryNode{
		name:    b.name,
		isDir:   b.isDir,
		mode:    b.mode,
		uid:     b.uid,
		gid:     b.gid,
		mtime:   b.mtime,
		content: b.content,
	}
	if b.isDir {
		var keys []string
		for k := range b.children {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			m.children = append(m.children, convertToMemoryNode(b.children[k]))
		}
	}
	return m
}

// BuildTree constructs an in-memory Node hierarchy from a sequence of virtual nodes.
func BuildTree(paths []string, isDirs []bool, contents [][]byte, modes []uint16) (Node, error) {
	root := &treeBuilderNode{
		name:     "",
		isDir:    true,
		mode:     S_IFDIR | 0755,
		children: make(map[string]*treeBuilderNode),
	}

	for idx, path := range paths {
		if path == "" || path == "." {
			continue
		}
		parts := splitPath(path)
		curr := root
		for i, part := range parts {
			if i == len(parts)-1 {
				node := &treeBuilderNode{
					name:    part,
					isDir:   isDirs[idx],
					content: contents[idx],
					mode:    modes[idx],
				}
				if isDirs[idx] {
					node.mode |= S_IFDIR
					node.children = make(map[string]*treeBuilderNode)
				} else if modes[idx]&S_IFMT == 0 {
					node.mode |= S_IFREG
				}
				curr.children[part] = node
			} else {
				next, ok := curr.children[part]
				if !ok {
					next = &treeBuilderNode{
						name:     part,
						isDir:    true,
						mode:     S_IFDIR | 0755,
						children: make(map[string]*treeBuilderNode),
					}
					curr.children[part] = next
				}
				curr = next
			}
		}
	}
	return convertToMemoryNode(root), nil
}

// splitPath splits a file path into its constituents.
func splitPath(p string) []string {
	var parts []string
	for _, part := range strings.Split(p, "/") {
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

// nodeInfo represents the lightweight layout metadata collected during Pass 1.
type nodeInfo struct {
	node       Node
	nid        uint64
	parentNID  uint64
	name       string
	isDir      bool
	mode       uint16
	uid        uint32
	gid        uint32
	mtime      uint64
	size       uint64
	dataLen    uint64
	dataOffset int64
	dirData    []byte
}

type writerAtWrapper struct {
	w   io.WriterAt
	off int64
}

func (wrapper *writerAtWrapper) Write(p []byte) (int, error) {
	n, err := wrapper.w.WriteAt(p, wrapper.off)
	wrapper.off += int64(n)
	return n, err
}

// marshalCompactNode constructs the 32-byte compact inode layout.
func marshalCompactNode(n *nodeInfo) []byte {
	buf := make([]byte, SlotSize)

	var format uint16 = 0
	binary.LittleEndian.PutUint16(buf[0:2], format)
	binary.LittleEndian.PutUint16(buf[2:4], 0) // xattr_icount
	binary.LittleEndian.PutUint16(buf[4:6], n.mode)

	var nlink uint16 = 1
	if n.isDir {
		nlink = 2
	}
	binary.LittleEndian.PutUint16(buf[6:8], nlink)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(n.dataLen))
	binary.LittleEndian.PutUint32(buf[12:16], 0) // reserved
	binary.LittleEndian.PutUint32(buf[16:20], uint32(n.dataOffset/BlockSize4K))
	binary.LittleEndian.PutUint32(buf[20:24], uint32(n.nid))
	binary.LittleEndian.PutUint16(buf[24:26], uint16(n.uid))
	binary.LittleEndian.PutUint16(buf[26:28], uint16(n.gid))
	binary.LittleEndian.PutUint32(buf[28:32], 0) // reserved2

	return buf
}

// WriteImage compiles a Node hierarchy into a valid, block-aligned EROFS filesystem image.
// Writes directly to w at exact offsets, avoiding any full disk image or file buffering in memory.
func WriteImage(w io.WriterAt, root Node) error {
	var nodes []*nodeInfo

	// Recursively collect metadata of the nodes
	var visit func(n Node, parentNID uint64, name string) (*nodeInfo, error)
	visit = func(n Node, parentNID uint64, name string) (*nodeInfo, error) {
		nid := uint64(len(nodes))
		info := &nodeInfo{
			node:      n,
			nid:       nid,
			parentNID: parentNID,
			name:      name,
			isDir:     n.IsDir(),
			mode:      n.Mode(),
			uid:       n.UID(),
			gid:       n.GID(),
			mtime:     n.Mtime(),
			size:      n.Size(),
		}
		if info.isDir {
			info.mode |= S_IFDIR
		} else if info.mode&S_IFMT == 0 {
			info.mode |= S_IFREG
		}

		nodes = append(nodes, info)

		if info.isDir {
			children, err := n.Children()
			if err != nil {
				return nil, err
			}

			for _, child := range children {
				_, err := visit(child, nid, child.Name())
				if err != nil {
					return nil, err
				}
			}
		}
		return info, nil
	}

	_, err := visit(root, 0, "")
	if err != nil {
		return err
	}

	blockSize := int64(BlockSize4K)

	for _, n := range nodes {
		if n.isDir {
			var dirents []Dirent
			dirents = append(dirents, Dirent{NID: n.nid, Name: ".", FileType: FTDir})
			dirents = append(dirents, Dirent{NID: n.parentNID, Name: "..", FileType: FTDir})

			children, err := n.node.Children()
			if err != nil {
				return err
			}

			for _, child := range children {
				var childInfo *nodeInfo
				for _, info := range nodes {
					if info.parentNID == n.nid && info.name == child.Name() {
						childInfo = info
						break
					}
				}
				if childInfo == nil {
					return fmt.Errorf("child node info not found for name %s under parent NID %d", child.Name(), n.nid)
				}

				var ft uint8 = FTRegFile
				if childInfo.isDir {
					ft = FTDir
				} else if (childInfo.mode & S_IFMT) == S_IFLNK {
					ft = FTSymlink
				}
				dirents = append(dirents, Dirent{NID: childInfo.nid, Name: childInfo.name, FileType: ft})
			}

			dirBlock, err := BuildDirectoryBlock(dirents, int(blockSize))
			if err != nil {
				return err
			}
			n.dirData = dirBlock
			n.size = uint64(len(dirBlock))
			n.dataLen = uint64(len(dirBlock))
		} else {
			n.dataLen = n.size
		}
	}

	inodesBytes := int64(len(nodes) * SlotSize)
	inodesBlocks := (inodesBytes + blockSize - 1) / blockSize

	metaBlkaddr := int64(1)
	currentBlock := metaBlkaddr + inodesBlocks

	for _, n := range nodes {
		if n.isDir && n.dataLen > 0 {
			n.dataOffset = currentBlock * blockSize
			currentBlock += int64((n.dataLen + uint64(blockSize) - 1) / uint64(blockSize))
		}
	}

	for _, n := range nodes {
		if !n.isDir && n.dataLen > 0 {
			n.dataOffset = currentBlock * blockSize
			currentBlock += int64((n.dataLen + uint64(blockSize) - 1) / uint64(blockSize))
		}
	}

	totalBlocks := uint32(currentBlock)

	sb := &Superblock{
		Magic:       SuperMagic,
		Blkszbits:   12,
		RootNID:     uint16(nodes[0].nid),
		Inodes:      uint64(len(nodes)),
		Blocks:      totalBlocks,
		MetaBlkaddr: uint32(metaBlkaddr),
	}

	sbBuf := make([]byte, SuperSize)
	binary.LittleEndian.PutUint32(sbBuf[0:4], sb.Magic)
	// sbBuf[4:8] is the Checksum field, kept zero during CRC computation
	sbBuf[12] = sb.Blkszbits
	binary.LittleEndian.PutUint16(sbBuf[14:16], sb.RootNID)
	binary.LittleEndian.PutUint64(sbBuf[16:24], sb.Inodes)
	binary.LittleEndian.PutUint32(sbBuf[36:40], sb.Blocks)
	binary.LittleEndian.PutUint32(sbBuf[40:44], sb.MetaBlkaddr)

	// Compute superblock checksum
	sb.Checksum = crc32.ChecksumIEEE(sbBuf)
	binary.LittleEndian.PutUint32(sbBuf[4:8], sb.Checksum)

	if _, err := w.WriteAt(sbBuf, SuperOffset); err != nil {
		return fmt.Errorf("failed to write superblock: %w", err)
	}

	for _, n := range nodes {
		inodeOffset := sb.InodeOffset(n.nid)
		inodeBuf := marshalCompactNode(n)
		if _, err := w.WriteAt(inodeBuf, inodeOffset); err != nil {
			return fmt.Errorf("failed to write inode for NID %d: %w", n.nid, err)
		}
	}

	for _, n := range nodes {
		if n.isDir && n.dataLen > 0 {
			if _, err := w.WriteAt(n.dirData, n.dataOffset); err != nil {
				return fmt.Errorf("failed to write directory block for NID %d: %w", n.nid, err)
			}
		}
	}

	for _, n := range nodes {
		if !n.isDir && n.dataLen > 0 {
			rc, err := n.node.Open()
			if err != nil {
				return fmt.Errorf("failed to open file %s for streaming: %w", n.name, err)
			}
			wrapper := &writerAtWrapper{
				w:   w,
				off: n.dataOffset,
			}
			written, err := io.Copy(wrapper, rc)
			rc.Close()
			if err != nil {
				return fmt.Errorf("failed to stream file content for %s: %w", n.name, err)
			}
			if uint64(written) != n.dataLen {
				return fmt.Errorf("file size mismatch for %s: wrote %d, expected %d", n.name, written, n.dataLen)
			}

			tail := n.dataLen % uint64(blockSize)
			if tail > 0 {
				padSize := blockSize - int64(tail)
				pad := make([]byte, padSize)
				if _, err := w.WriteAt(pad, n.dataOffset+int64(n.dataLen)); err != nil {
					return fmt.Errorf("failed to write padding for %s: %w", n.name, err)
				}
			}
		}
	}

	return nil
}
