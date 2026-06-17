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
	"io"
	"os"
	"path/filepath"
	"testing"
)

type memoryWriterAt struct {
	buf []byte
}

func (m *memoryWriterAt) WriteAt(p []byte, off int64) (int, error) {
	end := off + int64(len(p))
	if end > int64(len(m.buf)) {
		newBuf := make([]byte, end)
		copy(newBuf, m.buf)
		m.buf = newBuf
	}
	copy(m.buf[off:end], p)
	return len(p), nil
}

// compareTrees recursively compares a Node tree with a compiled EROFS filesystem Reader,
// ensuring count, name, type, and file contents match exactly without hardcoded offsets.
func compareTrees(t *testing.T, expected Node, reader *Reader, nid uint64) {
	if expected.IsDir() {
		expectedChildren, err := expected.Children()
		if err != nil {
			t.Fatalf("failed to get expected children: %v", err)
		}
		actualChildren, err := reader.ListDirectory(nid)
		if err != nil {
			t.Fatalf("failed to list actual directory at NID %d: %v", nid, err)
		}

		// Verify that "." and ".." are present in actualChildren
		foundDot := false
		foundDotDot := false
		for _, de := range actualChildren {
			if de.Name == "." {
				foundDot = true
				if de.FileType != FTDir {
					t.Errorf("expected FTDir for '.', got %d", de.FileType)
				}
				if de.NID != nid {
					t.Errorf("expected '.' to point to self NID %d, got %d", nid, de.NID)
				}
			}
			if de.Name == ".." {
				foundDotDot = true
				if de.FileType != FTDir {
					t.Errorf("expected FTDir for '..', got %d", de.FileType)
				}
			}
		}

		if !foundDot {
			t.Errorf("directory at NID %d does not contain '.' entry", nid)
		}
		if !foundDotDot {
			t.Errorf("directory at NID %d does not contain '..' entry", nid)
		}

		// Filter out "." and ".." from actualChildren
		var actualFiltered []Dirent
		for _, de := range actualChildren {
			if de.Name != "." && de.Name != ".." {
				actualFiltered = append(actualFiltered, de)
			}
		}

		if len(expectedChildren) != len(actualFiltered) {
			t.Fatalf("directory children count mismatch under %s: expected %d, got %d", expected.Name(), len(expectedChildren), len(actualFiltered))
		}

		for i, ec := range expectedChildren {
			ac := actualFiltered[i]
			if ec.Name() != ac.Name {
				t.Fatalf("child name mismatch under %s at index %d: expected %s, got %s", expected.Name(), i, ec.Name(), ac.Name)
			}

			// Validate file type
			var expectedFileType uint8
			if ec.IsDir() {
				expectedFileType = FTDir
			} else if (ec.Mode() & S_IFMT) == S_IFLNK {
				expectedFileType = FTSymlink
			} else {
				expectedFileType = FTRegFile
			}

			if ac.FileType != expectedFileType {
				t.Errorf("file type mismatch for %s: expected %d, got %d", ec.Name(), expectedFileType, ac.FileType)
			}

			// Recurse into children
			compareTrees(t, ec, reader, ac.NID)
		}
	} else {
		// Verify file/symlink content
		rc, err := expected.Open()
		if err != nil {
			t.Fatalf("failed to open expected file content for %s: %v", expected.Name(), err)
		}
		defer rc.Close()
		expectedBytes, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("failed to read expected file content for %s: %v", expected.Name(), err)
		}

		r, err := reader.ReadFileContent(nid)
		if err != nil {
			t.Fatalf("failed to read actual file content at NID %d: %v", nid, err)
		}
		actualBytes, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("failed to read actual streamed content: %v", err)
		}

		if !bytes.Equal(expectedBytes, actualBytes) {
			t.Errorf("content mismatch for %s: expected %q, got %q", expected.Name(), string(expectedBytes), string(actualBytes))
		}
	}
}

func TestErofsSuccess(t *testing.T) {
	// 1. Define paths, directories, contents and modes to BuildTree
	paths := []string{
		"hello.txt",
		"foo/bar.txt",
		"foo/baz",
		"foo/baz/deep.txt",
		"sym_link",
	}
	isDirs := []bool{
		false,
		false,
		true,
		false,
		false,
	}
	contents := [][]byte{
		[]byte("Hello, EROFS!"),
		[]byte("Nested file content."),
		nil,
		[]byte("Deeply nested file."),
		[]byte("hello.txt"),
	}
	modes := []uint16{
		0644,
		0644,
		0755,
		0644,
		S_IFLNK | 0777,
	}

	rootNode, err := BuildTree(paths, isDirs, contents, modes)
	if err != nil {
		t.Fatalf("failed to build virtual tree: %v", err)
	}

	// 2. Compile to an EROFS image using memoryWriterAt
	mw := &memoryWriterAt{}
	err = WriteImage(mw, rootNode)
	if err != nil {
		t.Fatalf("failed to write EROFS image: %v", err)
	}

	imageBytes := mw.buf
	readerAt := bytes.NewReader(imageBytes)

	// 3. Perform Fsck validation
	err = Fsck(readerAt)
	if err != nil {
		t.Errorf("Fsck failed on valid image: %v", err)
	}

	// 4. Use Reader to navigate and recursively verify with expected tree
	reader, err := NewReader(readerAt)
	if err != nil {
		t.Fatalf("failed to create Reader: %v", err)
	}

	compareTrees(t, rootNode, reader, reader.sb.GetRootNID())
}

func createMinimalImage(t *testing.T) []byte {
	root, err := BuildTree(
		[]string{"file.txt"},
		[]bool{false},
		[][]byte{[]byte("test data")},
		[]uint16{0644},
	)
	if err != nil {
		t.Fatalf("failed to build minimal tree: %v", err)
	}

	mw := &memoryWriterAt{}
	err = WriteImage(mw, root)
	if err != nil {
		t.Fatalf("failed to write image: %v", err)
	}

	return mw.buf
}

func TestErofsFsckFailures(t *testing.T) {
	t.Run("Invalid Superblock Magic", func(t *testing.T) {
		img := createMinimalImage(t)
		// Corrupt SuperMagic at SuperOffset (1024)
		img[SuperOffset] = 0xAA
		img[SuperOffset+1] = 0xBB
		img[SuperOffset+2] = 0xCC
		img[SuperOffset+3] = 0xDD

		err := Fsck(bytes.NewReader(img))
		if err == nil {
			t.Error("expected Fsck to fail with invalid superblock magic")
		} else if !IsInvalidSuperblockError(err) {
			t.Errorf("expected ErrInvalidSuperblock error, got %v", err)
		}
	})

	t.Run("Superblock Checksum Verification", func(t *testing.T) {
		img := createMinimalImage(t)
		// Corrupt checksum byte at index SuperOffset + 4
		img[SuperOffset+4] ^= 0xFF

		err := Fsck(bytes.NewReader(img))
		if err == nil {
			t.Error("expected Fsck to fail with invalid superblock checksum")
		} else if !IsInvalidSuperblockError(err) {
			t.Errorf("expected ErrInvalidSuperblock error, got %v", err)
		}
	})

	t.Run("Directory Entry Null Byte Injection", func(t *testing.T) {
		root, err := BuildTree(
			[]string{"magicnullpath", "zz_lastfile"},
			[]bool{false, false},
			[][]byte{[]byte("test bytes"), []byte("more bytes")},
			[]uint16{0644, 0644},
		)
		if err != nil {
			t.Fatalf("failed to build tree: %v", err)
		}
		mw := &memoryWriterAt{}
		if err := WriteImage(mw, root); err != nil {
			t.Fatalf("failed to compile image: %v", err)
		}
		img := mw.buf

		// Find the index of "magicnullpath" filename on-disk
		idx := bytes.Index(img, []byte("magicnullpath"))
		if idx == -1 {
			t.Fatalf("could not find filename string inside compiled image")
		}

		// Corrupt a character to null byte
		corruptImg := make([]byte, len(img))
		copy(corruptImg, img)
		corruptImg[idx+5] = 0 // "magic\x00ullpath"

		err = Fsck(bytes.NewReader(corruptImg))
		if err == nil {
			t.Error("expected Fsck to fail when null byte is injected in a directory entry filename")
		} else if !IsPathTraversalError(err) {
			t.Errorf("expected ErrPathTraversal, got %v", err)
		}
	})

	t.Run("Directory Entry Slash Injection", func(t *testing.T) {
		root, err := BuildTree(
			[]string{"magicslashpath"},
			[]bool{false},
			[][]byte{[]byte("test bytes")},
			[]uint16{0644},
		)
		if err != nil {
			t.Fatalf("failed to build tree: %v", err)
		}
		mw := &memoryWriterAt{}
		if err := WriteImage(mw, root); err != nil {
			t.Fatalf("failed to compile image: %v", err)
		}
		img := mw.buf

		// Find the index of "magicslashpath" filename on-disk
		idx := bytes.Index(img, []byte("magicslashpath"))
		if idx == -1 {
			t.Fatalf("could not find filename string inside compiled image")
		}

		// Corrupt a character to a slash separator
		corruptImg := make([]byte, len(img))
		copy(corruptImg, img)
		corruptImg[idx+5] = '/' // "magic/lashpath"

		err = Fsck(bytes.NewReader(corruptImg))
		if err == nil {
			t.Error("expected Fsck to fail when slash separator is injected in a directory entry filename")
		} else if !IsPathTraversalError(err) {
			t.Errorf("expected ErrPathTraversal, got %v", err)
		}
	})

	t.Run("Cycle Detection", func(t *testing.T) {
		root, err := BuildTree(
			[]string{"dir1"},
			[]bool{true},
			[][]byte{nil},
			[]uint16{0755},
		)
		if err != nil {
			t.Fatalf("failed to build virtual tree: %v", err)
		}

		mw := &memoryWriterAt{}
		err = WriteImage(mw, root)
		if err != nil {
			t.Fatalf("failed to write image: %v", err)
		}

		imageBytes := mw.buf
		readerAt := bytes.NewReader(imageBytes)

		sb, err := ReadSuperblock(readerAt)
		if err != nil {
			t.Fatalf("failed to read superblock: %v", err)
		}

		// Find the directory's inode. NID 0 is root, NID 1 is dir1.
		inode, err := ReadInode(readerAt, sb, 1)
		if err != nil {
			t.Fatalf("failed to read inode 1: %v", err)
		}

		dirents := []Dirent{
			{NID: 1, Name: ".", FileType: FTDir},
			{NID: 0, Name: "..", FileType: FTDir},
			{NID: 1, Name: "loop", FileType: FTDir},
		}

		block, err := BuildDirectoryBlock(dirents, BlockSize4K)
		if err != nil {
			t.Fatalf("failed to build directory block: %v", err)
		}

		dataOffset := int64(inode.RawBlkaddr) * BlockSize4K
		copy(imageBytes[dataOffset:dataOffset+BlockSize4K], block)

		err = Fsck(bytes.NewReader(imageBytes))
		if err == nil {
			t.Error("expected Fsck to return an error when a cycle is detected")
		} else if !IsCycleDetectedError(err) {
			t.Errorf("expected ErrCycleDetected, got %v", err)
		}
	})
}

func TestErofsPhysicalDirectory(t *testing.T) {
	// Build a filesystem in a tmp directory, compile it, and compare
	tmpDir, err := os.MkdirTemp("", "erofs-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create physical structure
	err = os.WriteFile(filepath.Join(tmpDir, "file1.txt"), []byte("Physical file 1 content."), 0644)
	if err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	subDir := filepath.Join(tmpDir, "subdir")
	err = os.Mkdir(subDir, 0755)
	if err != nil {
		t.Fatalf("failed to mkdir: %v", err)
	}

	err = os.WriteFile(filepath.Join(subDir, "file2.txt"), []byte("Physical file 2 content inside subdir."), 0644)
	if err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	err = os.Symlink("file1.txt", filepath.Join(tmpDir, "link1"))
	if err != nil {
		t.Fatalf("failed to symlink: %v", err)
	}

	// Build the EROFS tree using local directory node
	fsNode := NewFileSystemNode("", tmpDir)

	mw := &memoryWriterAt{}
	err = WriteImage(mw, fsNode)
	if err != nil {
		t.Fatalf("failed to write image: %v", err)
	}

	readerAt := bytes.NewReader(mw.buf)
	err = Fsck(readerAt)
	if err != nil {
		t.Errorf("Fsck failed on physically compiled EROFS image: %v", err)
	}

	reader, err := NewReader(readerAt)
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}

	// Recursively validate compiled EROFS tree against the physical fileSystemNode tree input!
	compareTrees(t, fsNode, reader, reader.sb.GetRootNID())
}
