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
	"testing"
)

func TestErofsSuccess(t *testing.T) {
	// 1. Define a set of files and directories
	entries := []Entry{
		{Path: "hello.txt", IsDir: false, Content: []byte("Hello, EROFS!"), Mode: 0644},
		{Path: "foo/bar.txt", IsDir: false, Content: []byte("Nested file content."), Mode: 0644},
		{Path: "foo/baz", IsDir: true, Mode: 0755},
		{Path: "foo/baz/deep.txt", IsDir: false, Content: []byte("Deeply nested file."), Mode: 0644},
		{Path: "sym_link", IsDir: false, Content: []byte("hello.txt"), Mode: S_IFLNK | 0777},
	}

	// 2. Compile to an EROFS image
	var buf bytes.Buffer
	err := WriteImage(&buf, entries)
	if err != nil {
		t.Fatalf("failed to write EROFS image: %v", err)
	}

	imageBytes := buf.Bytes()
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
			content, err := reader.ReadFileContent(de.NID)
			if err != nil {
				t.Fatalf("failed to read hello.txt: %v", err)
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
					content, err := reader.ReadFileContent(fde.NID)
					if err != nil {
						t.Fatalf("failed to read bar.txt: %v", err)
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
							content, err := reader.ReadFileContent(bde.NID)
							if err != nil {
								t.Fatalf("failed to read deep.txt: %v", err)
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
			content, err := reader.ReadFileContent(de.NID)
			if err != nil {
				t.Fatalf("failed to read sym_link content: %v", err)
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
		// Build directory block with a null byte in the filename
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
		// Create an image, then manually corrupt an inode or directory block to point to a cycle
		entries := []Entry{
			{Path: "dir1", IsDir: true, Mode: 0755},
		}
		var buf bytes.Buffer
		err := WriteImage(&buf, entries)
		if err != nil {
			t.Fatalf("failed to write image: %v", err)
		}

		imageBytes := buf.Bytes()
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

		// Read dir1's content block and modify its entries.
		// Let's modify it to point back to root (NID 0) or itself (NID 1), which Fsck should handle safely without infinite looping.
		// Wait, Fsck hasVisited maps to prevent infinite loop.
		// Let's test that Fsck successfully handles a cyclic structure.
		// Let's build a directory block for dir1 that contains a child pointing back to dir1 itself (NID 1).
		dirents := []Dirent{
			{NID: 1, Name: ".", FileType: FTDir},
			{NID: 0, Name: "..", FileType: FTDir},
			{NID: 1, Name: "loop", FileType: FTDir}, // loop points to itself
		}

		block, err := BuildDirectoryBlock(dirents, BlockSize4K)
		if err != nil {
			t.Fatalf("failed to build directory block: %v", err)
		}

		// Write modified directory block back into imageBytes at inode.RawBlkaddr * BlockSize4K
		dataOffset := int64(inode.RawBlkaddr) * BlockSize4K
		copy(imageBytes[dataOffset:dataOffset+BlockSize4K], block)

		// Run Fsck and verify it terminates successfully (cycles are ignored/prevented by visited map)
		err = Fsck(bytes.NewReader(imageBytes))
		if err != nil {
			t.Errorf("Fsck failed on cyclic image: %v", err)
		}
	})
}
