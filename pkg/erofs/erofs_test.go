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

func TestErofsSuccess(t *testing.T) {
	// 1. Define a set of files and directories using Entry
	entries := []Entry{
		{Path: "hello.txt", IsDir: false, Content: []byte("Hello, EROFS!"), Mode: 0644},
		{Path: "foo/bar.txt", IsDir: false, Content: []byte("Nested file content."), Mode: 0644},
		{Path: "foo/baz", IsDir: true, Mode: 0755},
		{Path: "foo/baz/deep.txt", IsDir: false, Content: []byte("Deeply nested file."), Mode: 0644},
		{Path: "sym_link", IsDir: false, Content: []byte("hello.txt"), Mode: S_IFLNK | 0777},
	}

	// Helper to yield entries for BuildTree
	entrySeq := func(yield func(Entry) bool) {
		for _, e := range entries {
			if !yield(e) {
				return
			}
		}
	}

	rootNode, err := BuildTree(entrySeq)
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

	// 4. Use Reader to navigate and verify file contents
	reader, err := NewReader(readerAt)
	if err != nil {
		t.Fatalf("failed to create Reader: %v", err)
	}

	// List root directory
	rootDirents, err := reader.ListDirectory(reader.sb.GetRootNID())
	if err != nil {
		t.Fatalf("failed to list root directory: %v", err)
	}

	foundHello := false
	foundFoo := false
	foundSym := false
	for _, de := range rootDirents {
		if de.Name == "hello.txt" {
			foundHello = true
			if de.FileType != FTRegFile {
				t.Errorf("expected FTRegFile for hello.txt, got %d", de.FileType)
			}
			// Verify content
			r, err := reader.ReadFileContent(de.NID)
			if err != nil {
				t.Fatalf("failed to read hello.txt: %v", err)
			}
			content, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("failed to read streamed hello.txt content: %v", err)
			}
			if string(content) != "Hello, EROFS!" {
				t.Errorf("unexpected content for hello.txt: %q", string(content))
			}
		} else if de.Name == "foo" {
			foundFoo = true
			if de.FileType != FTDir {
				t.Errorf("expected FTDir for foo, got %d", de.FileType)
			}
			// List foo directory
			fooDirents, err := reader.ListDirectory(de.NID)
			if err != nil {
				t.Fatalf("failed to list foo directory: %v", err)
			}
			foundBar := false
			foundBaz := false
			for _, fde := range fooDirents {
				if fde.Name == "bar.txt" {
					foundBar = true
					r, err := reader.ReadFileContent(fde.NID)
					if err != nil {
						t.Fatalf("failed to read bar.txt: %v", err)
					}
					content, err := io.ReadAll(r)
					if err != nil {
						t.Fatalf("failed to read streamed bar.txt content: %v", err)
					}
					if string(content) != "Nested file content." {
						t.Errorf("unexpected content for bar.txt: %q", string(content))
					}
				} else if fde.Name == "baz" {
					foundBaz = true
					bazDirents, err := reader.ListDirectory(fde.NID)
					if err != nil {
						t.Fatalf("failed to list baz directory: %v", err)
					}
					foundDeep := false
					for _, bde := range bazDirents {
						if bde.Name == "deep.txt" {
							foundDeep = true
							r, err := reader.ReadFileContent(bde.NID)
							if err != nil {
								t.Fatalf("failed to read deep.txt: %v", err)
							}
							content, err := io.ReadAll(r)
							if err != nil {
								t.Fatalf("failed to read streamed deep.txt content: %v", err)
							}
							if string(content) != "Deeply nested file." {
								t.Errorf("unexpected content for deep.txt: %q", string(content))
							}
						}
					}
					if !foundDeep {
						t.Error("deep.txt not found under foo/baz")
					}
				}
			}
			if !foundBar {
				t.Error("bar.txt not found under foo")
			}
			if !foundBaz {
				t.Error("baz not found under foo")
			}
		} else if de.Name == "sym_link" {
			foundSym = true
			if de.FileType != FTSymlink {
				t.Errorf("expected FTSymlink for sym_link, got %d", de.FileType)
			}
			r, err := reader.ReadFileContent(de.NID)
			if err != nil {
				t.Fatalf("failed to read sym_link content: %v", err)
			}
			content, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("failed to read streamed sym_link content: %v", err)
			}
			if string(content) != "hello.txt" {
				t.Errorf("unexpected sym_link target: %q", string(content))
			}
		}
	}

	if !foundHello {
		t.Error("hello.txt not found in root")
	}
	if !foundFoo {
		t.Error("foo directory not found in root")
	}
	if !foundSym {
		t.Error("sym_link not found in root")
	}
}

func TestErofsFsckFailures(t *testing.T) {
	t.Run("Invalid Superblock Magic", func(t *testing.T) {
		corruptData := make([]byte, 2048)
		err := Fsck(bytes.NewReader(corruptData))
		if err == nil {
			t.Error("expected Fsck to fail with invalid superblock magic")
		}
	})

	t.Run("Directory Entry Null Byte", func(t *testing.T) {
		dirents := []Dirent{
			{NID: 1, Name: "bad\x00file", FileType: FTRegFile},
		}
		_, err := BuildDirectoryBlock(dirents, BlockSize4K)
		if err == nil {
			t.Error("expected BuildDirectoryBlock to fail or Fsck to catch null bytes")
		}
	})

	t.Run("Directory Entry Path Separator", func(t *testing.T) {
		dirents := []Dirent{
			{NID: 1, Name: "bad/file", FileType: FTRegFile},
		}
		_, err := BuildDirectoryBlock(dirents, BlockSize4K)
		if err == nil {
			t.Error("expected BuildDirectoryBlock to fail or Fsck to catch path separator")
		}
	})

	t.Run("Cycle Detection", func(t *testing.T) {
		entries := []Entry{
			{Path: "dir1", IsDir: true, Mode: 0755},
		}
		entrySeq := func(yield func(Entry) bool) {
			for _, e := range entries {
				if !yield(e) {
					return
				}
			}
		}
		root, err := BuildTree(entrySeq)
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
		if err != nil {
			t.Errorf("Fsck failed on cyclic image: %v", err)
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

	rootDirents, err := reader.ListDirectory(reader.sb.GetRootNID())
	if err != nil {
		t.Fatalf("failed to list root dirents: %v", err)
	}

	foundFile1 := false
	foundSubdir := false
	foundLink1 := false

	for _, de := range rootDirents {
		if de.Name == "file1.txt" {
			foundFile1 = true
			if de.FileType != FTRegFile {
				t.Errorf("expected FTRegFile for file1.txt, got %d", de.FileType)
			}
			r, err := reader.ReadFileContent(de.NID)
			if err != nil {
				t.Fatalf("failed to read file1: %v", err)
			}
			content, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("failed to read streamed file1 content: %v", err)
			}
			if string(content) != "Physical file 1 content." {
				t.Errorf("unexpected content for file1.txt: %q", string(content))
			}
		} else if de.Name == "subdir" {
			foundSubdir = true
			if de.FileType != FTDir {
				t.Errorf("expected FTDir for subdir, got %d", de.FileType)
			}
			subDirents, err := reader.ListDirectory(de.NID)
			if err != nil {
				t.Fatalf("failed to list subdir: %v", err)
			}
			foundFile2 := false
			for _, sde := range subDirents {
				if sde.Name == "file2.txt" {
					foundFile2 = true
					r, err := reader.ReadFileContent(sde.NID)
					if err != nil {
						t.Fatalf("failed to read file2: %v", err)
					}
					content, err := io.ReadAll(r)
					if err != nil {
						t.Fatalf("failed to read streamed file2 content: %v", err)
					}
					if string(content) != "Physical file 2 content inside subdir." {
						t.Errorf("unexpected content for file2.txt: %q", string(content))
					}
				}
			}
			if !foundFile2 {
				t.Error("file2.txt not found in subdir")
			}
		} else if de.Name == "link1" {
			foundLink1 = true
			if de.FileType != FTSymlink {
				t.Errorf("expected FTSymlink for link1, got %d", de.FileType)
			}
			r, err := reader.ReadFileContent(de.NID)
			if err != nil {
				t.Fatalf("failed to read link1: %v", err)
			}
			content, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("failed to read streamed link1 content: %v", err)
			}
			if string(content) != "file1.txt" {
				t.Errorf("unexpected target for link1: %q", string(content))
			}
		}
	}

	if !foundFile1 {
		t.Error("file1.txt not found in EROFS image root")
	}
	if !foundSubdir {
		t.Error("subdir not found in EROFS image root")
	}
	if !foundLink1 {
		t.Error("link1 symlink not found in EROFS image root")
	}
}
