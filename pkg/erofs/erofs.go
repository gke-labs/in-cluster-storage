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
	"io"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	SuperMagic   = 0xE0F5E1E2
	SuperOffset  = 1024
	SuperSize    = 128
	SlotSize     = 32
	BlockSize4K  = 4096
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

// Entry represents a file or directory for writing an image.
type Entry struct {
	Path    string // relative path, e.g. "hello.txt", "sub/world.txt"
	IsDir   bool
	Content []byte // file content or symlink target path
	Mode    uint16 // permissions / mode
	UID     uint32
	GID     uint32
	Mtime   uint64
}

// writeNode is used internally by the EROFS compiler/writer to represent the file tree.
type writeNode struct {
	name        string
	isDir       bool
	content     []byte
	mode        uint16
	uid         uint32
	gid         uint32
	mtime       uint64
	nid         uint64
	children    map[string]*writeNode
	parent      *writeNode
	inodeOffset int64
	dataOffset  int64
	dataLen     uint64
}

// ReadSuperblock parses the EROFS superblock from the given ReaderAt.
func ReadSuperblock(r io.ReaderAt) (*Superblock, error) {
	buf := make([]byte, SuperSize)
	n, err := r.ReadAt(buf, SuperOffset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read superblock: %w", err)
	}
	if n < SuperSize {
		return nil, fmt.Errorf("truncated superblock, read only %d bytes", n)
	}

	sb := &Superblock{}
	sb.Magic = binary.LittleEndian.Uint32(buf[0:4])
	if sb.Magic != SuperMagic {
		return nil, fmt.Errorf("invalid EROFS magic: got 0x%x, expected 0x%x", sb.Magic, SuperMagic)
	}

	sb.Checksum = binary.LittleEndian.Uint32(buf[4:8])
	sb.FeatureCompat = binary.LittleEndian.Uint32(buf[8:12])
	sb.Blkszbits = buf[12]
	sb.SbExtslots = buf[13]
	sb.RootNID = binary.LittleEndian.Uint16(buf[14:16])
	sb.Inodes = binary.LittleEndian.Uint64(buf[16:24])
	sb.BuildTime = binary.LittleEndian.Uint64(buf[24:32])
	sb.BuildTimeNsec = binary.LittleEndian.Uint32(buf[32:36])
	sb.Blocks = binary.LittleEndian.Uint32(buf[36:40])
	sb.MetaBlkaddr = binary.LittleEndian.Uint32(buf[40:44])
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

// ReadData retrieves the entire data payload for the given inode.
func (inode *Inode) ReadData(r io.ReaderAt, sb *Superblock) ([]byte, error) {
	blockSize := uint64(sb.BlockSize())

	if inode.DataLayout == DatalayoutFlatPlain {
		totalBlocks := (inode.Size + blockSize - 1) / blockSize
		if totalBlocks == 0 {
			return nil, nil
		}

		if uint64(inode.RawBlkaddr)+totalBlocks > uint64(sb.Blocks) {
			return nil, fmt.Errorf("block address out of bounds: raw_blkaddr %d, totalBlocks %d, image total blocks %d", inode.RawBlkaddr, totalBlocks, sb.Blocks)
		}

		buf := make([]byte, inode.Size)
		startOffset := int64(inode.RawBlkaddr) * int64(blockSize)
		_, err := r.ReadAt(buf, startOffset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read plain data block: %w", err)
		}
		return buf, nil

	} else if inode.DataLayout == DatalayoutFlatInline {
		numBlocks := inode.Size / blockSize
		tailSize := inode.Size % blockSize

		var data []byte
		if numBlocks > 0 {
			if uint64(inode.RawBlkaddr)+numBlocks > uint64(sb.Blocks) {
				return nil, fmt.Errorf("block address out of bounds: raw_blkaddr %d, blocks %d, image total blocks %d", inode.RawBlkaddr, numBlocks, sb.Blocks)
			}
			data = make([]byte, numBlocks*blockSize)
			startOffset := int64(inode.RawBlkaddr) * int64(blockSize)
			_, err := r.ReadAt(data, startOffset)
			if err != nil && err != io.EOF {
				return nil, fmt.Errorf("failed to read inline base blocks: %w", err)
			}
		}

		if tailSize > 0 {
			tail, err := inode.ReadInlineData(r, sb)
			if err != nil {
				return nil, err
			}
			data = append(data, tail...)
		}
		return data, nil
	}

	return nil, fmt.Errorf("unsupported EROFS data layout: %d", inode.DataLayout)
}

// ParseDirectoryBlock parses directory entries from a block buffer.
func ParseDirectoryBlock(buf []byte) ([]Dirent, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	if len(buf) < 12 {
		return nil, fmt.Errorf("directory block too small: %d bytes", len(buf))
	}

	firstNameoff := binary.LittleEndian.Uint16(buf[8:10])
	if firstNameoff == 0 {
		return nil, nil
	}
	if firstNameoff%12 != 0 {
		return nil, fmt.Errorf("invalid directory block structure: first nameoff %d is not a multiple of 12", firstNameoff)
	}

	entryCount := int(firstNameoff / 12)
	if entryCount*12 > len(buf) {
		return nil, fmt.Errorf("invalid directory layout: entryCount %d overflows block size %d", entryCount, len(buf))
	}

	dirents := make([]Dirent, 0, entryCount)
	for i := 0; i < entryCount; i++ {
		offset := i * 12
		nid := binary.LittleEndian.Uint64(buf[offset : offset+8])
		nameoff := binary.LittleEndian.Uint16(buf[offset+8 : offset+10])
		fileType := buf[offset+10]
		reserved := buf[offset+11]

		if reserved != 0 {
			return nil, fmt.Errorf("invalid directory entry: reserved byte is non-zero")
		}

		var nameLen int
		if i < entryCount-1 {
			nextNameoff := binary.LittleEndian.Uint16(buf[offset+12+8 : offset+12+10])
			if nextNameoff < nameoff {
				return nil, fmt.Errorf("invalid directory layout: non-monotonic nameoff values")
			}
			nameLen = int(nextNameoff - nameoff)
		} else {
			nameLen = len(buf) - int(nameoff)
		}

		if int(nameoff)+nameLen > len(buf) {
			return nil, fmt.Errorf("directory filename bounds exceeded: nameoff %d, len %d, block size %d", nameoff, nameLen, len(buf))
		}

		nameBytes := buf[nameoff : int(nameoff)+nameLen]
		if i == entryCount-1 {
			if idx := bytes.IndexByte(nameBytes, 0); idx != -1 {
				nameBytes = nameBytes[:idx]
			}
		}
		name := string(nameBytes)

		if bytes.Contains(nameBytes, []byte{0}) {
			return nil, fmt.Errorf("malicious directory entry: filename contains null byte")
		}
		if bytes.Contains(nameBytes, []byte{'/'}) {
			return nil, fmt.Errorf("malicious directory entry: filename contains path separator")
		}
		if !utf8.ValidString(name) {
			return nil, fmt.Errorf("invalid directory entry: filename is not valid UTF-8")
		}
		if name == "" {
			return nil, fmt.Errorf("invalid directory entry: empty filename")
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
	maxDepth := 100
	totalInodesParsed := 0

	var traverse func(nid uint64, depth int) error
	traverse = func(nid uint64, depth int) error {
		if depth > maxDepth {
			return fmt.Errorf("directory depth limit exceeded (max %d)", maxDepth)
		}

		if visited[nid] {
			return nil
		}
		visited[nid] = true
		totalInodesParsed++

		if sb.Inodes > 0 && uint64(totalInodesParsed) > sb.Inodes+100 {
			return fmt.Errorf("parsed more inodes (%d) than declared in superblock (%d)", totalInodesParsed, sb.Inodes)
		}

		inode, err := ReadInode(r, sb, nid)
		if err != nil {
			return fmt.Errorf("failed to read inode for NID %d: %w", nid, err)
		}

		if inode.Version > 1 {
			return fmt.Errorf("invalid inode version %d for NID %d", inode.Version, nid)
		}
		if inode.DataLayout != DatalayoutFlatPlain && inode.DataLayout != DatalayoutFlatInline {
			return fmt.Errorf("unsupported data layout %d for NID %d", inode.DataLayout, nid)
		}

		if (inode.Mode & S_IFMT) == S_IFDIR {
			data, err := inode.ReadData(r, sb)
			if err != nil {
				return fmt.Errorf("corrupt directory data for NID %d: %w", nid, err)
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
						return fmt.Errorf("corrupt directory block at offset %d for NID %d: %w", offset, nid, err)
					}

					for _, de := range dirents {
						if de.Name == "." || de.Name == ".." {
							continue
						}

						if err := traverse(de.NID, depth+1); err != nil {
							return fmt.Errorf("error in path %q: %w", de.Name, err)
						}
					}
				}
			}
		} else if (inode.Mode & S_IFMT) == S_IFREG {
			_, err := inode.ReadData(r, sb)
			if err != nil {
				return fmt.Errorf("corrupt regular file data for NID %d: %w", nid, err)
			}
		} else if (inode.Mode & S_IFMT) == S_IFLNK {
			_, err := inode.ReadData(r, sb)
			if err != nil {
				return fmt.Errorf("corrupt symlink data for NID %d: %w", nid, err)
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

// ReadFileContent returns the full file content of a regular/symlink file.
func (reader *Reader) ReadFileContent(nid uint64) ([]byte, error) {
	inode, err := ReadInode(reader.r, reader.sb, nid)
	if err != nil {
		return nil, err
	}
	return inode.ReadData(reader.r, reader.sb)
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

// (n *writeNode) MarshalCompact constructs the 32-byte compact inode layout.
func (n *writeNode) MarshalCompact() []byte {
	buf := make([]byte, SlotSize)

	// Format = (version << 0) | (dataLayout << 1)
	// Compact version = 0, Flat plain layout = 0 -> Format = 0
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

// WriteImage compiles a set of virtual files/directories into a valid EROFS filesystem image.
func WriteImage(w io.Writer, entries []Entry) error {
	root := &writeNode{
		name:     "",
		isDir:    true,
		mode:     S_IFDIR | 0755,
		children: make(map[string]*writeNode),
	}
	root.parent = root

	for _, entry := range entries {
		if entry.Path == "" || entry.Path == "." {
			continue
		}
		parts := splitPath(entry.Path)
		curr := root
		for i, part := range parts {
			if i == len(parts)-1 {
				node := &writeNode{
					name:    part,
					isDir:   entry.IsDir,
					content: entry.Content,
					mode:    entry.Mode,
					uid:     entry.UID,
					gid:     entry.GID,
					mtime:   entry.Mtime,
				}
				if entry.IsDir {
					node.mode |= S_IFDIR
					node.children = make(map[string]*writeNode)
				} else if entry.Mode&S_IFMT == 0 {
					node.mode |= S_IFREG
				}
				node.parent = curr
				curr.children[part] = node
			} else {
				next, ok := curr.children[part]
				if !ok {
					next = &writeNode{
						name:     part,
						isDir:    true,
						mode:     S_IFDIR | 0755,
						children: make(map[string]*writeNode),
						parent:   curr,
					}
					curr.children[part] = next
				}
				curr = next
			}
		}
	}

	var nodes []*writeNode
	var collect func(n *writeNode)
	collect = func(n *writeNode) {
		nodes = append(nodes, n)
		var keys []string
		for k := range n.children {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			collect(n.children[k])
		}
	}
	collect(root)

	// Assign NIDs
	for i, n := range nodes {
		n.nid = uint64(i)
	}

	blockSize := int64(BlockSize4K)
	for _, n := range nodes {
		if n.isDir {
			var dirents []Dirent
			var keys []string
			for k := range n.children {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			dirents = append(dirents, Dirent{NID: n.nid, Name: ".", FileType: FTDir})
			dirents = append(dirents, Dirent{NID: n.parent.nid, Name: "..", FileType: FTDir})

			for _, k := range keys {
				child := n.children[k]
				var ft uint8 = FTRegFile
				if child.isDir {
					ft = FTDir
				} else if (child.mode & S_IFMT) == S_IFLNK {
					ft = FTSymlink
				}
				dirents = append(dirents, Dirent{NID: child.nid, Name: child.name, FileType: ft})
			}

			dirBlock, err := BuildDirectoryBlock(dirents, int(blockSize))
			if err != nil {
				return err
			}
			n.content = dirBlock
			n.dataLen = uint64(len(dirBlock))
		} else {
			n.dataLen = uint64(len(n.content))
		}
	}

	// Blocks allocation
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
		Magic:           SuperMagic,
		Blkszbits:       12, // 4096 bytes
		RootNID:         uint16(root.nid),
		Inodes:          uint64(len(nodes)),
		Blocks:          totalBlocks,
		MetaBlkaddr:     uint32(metaBlkaddr),
		FeatureIncompat: 0,
	}

	block0 := make([]byte, blockSize)
	sbBuf := make([]byte, SuperSize)
	binary.LittleEndian.PutUint32(sbBuf[0:4], sb.Magic)
	binary.LittleEndian.PutUint16(sbBuf[14:16], sb.RootNID)
	binary.LittleEndian.PutUint64(sbBuf[16:24], sb.Inodes)
	binary.LittleEndian.PutUint32(sbBuf[36:40], sb.Blocks)
	binary.LittleEndian.PutUint32(sbBuf[40:44], sb.MetaBlkaddr)
	copy(block0[SuperOffset:SuperOffset+SuperSize], sbBuf)

	if _, err := w.Write(block0); err != nil {
		return err
	}

	inodeBuf := make([]byte, inodesBlocks*blockSize)
	for i, n := range nodes {
		copy(inodeBuf[i*SlotSize:(i+1)*SlotSize], n.MarshalCompact())
	}
	if _, err := w.Write(inodeBuf); err != nil {
		return err
	}

	for _, n := range nodes {
		if n.isDir && n.dataLen > 0 {
			if _, err := w.Write(n.content); err != nil {
				return err
			}
		}
	}

	for _, n := range nodes {
		if !n.isDir && n.dataLen > 0 {
			fileBlock := make([]byte, ((n.dataLen+uint64(blockSize)-1)/uint64(blockSize))*uint64(blockSize))
			copy(fileBlock, n.content)
			if _, err := w.Write(fileBlock); err != nil {
				return err
			}
		}
	}

	return nil
}
