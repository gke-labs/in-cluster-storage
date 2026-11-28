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

package controller

import (
	"bytes"
	"syscall"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

func setupExistingRegularFile(t *testing.T, volumeID string) (*Server, *Volume, uint64, []byte) {
	t.Helper()
	ctx := t.Context()
	server := NewTestServer(t, NewMemoryBackend())

	created, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "f",
		Mode:        0600,
		Uid:         1000,
		Gid:         1000,
	})
	if err != nil || created.GetError() != 0 {
		t.Fatalf("initial CreateFile: err=%v errno=%d", err, created.GetError())
	}
	ino := created.GetAttr().GetInode().GetIno()
	payload := bytes.Repeat([]byte("abcdefgh"), 1024) // 8 KiB
	written, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     ino,
		Offset:    0,
		Data:      payload,
		WriteMode: pb.WriteMode_FSYNC,
	})
	if err != nil || written.GetError() != 0 {
		t.Fatalf("WriteFile: err=%v errno=%d", err, written.GetError())
	}
	synced, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino})
	if err != nil || synced.GetError() != 0 {
		t.Fatalf("Fsync: err=%v errno=%d", err, synced.GetError())
	}
	return server, mustGetVolume(t, server, volumeID), ino, payload
}

func TestCreateFile_ExistingRegularFile_Plain(t *testing.T) {
	ctx := t.Context()
	const volumeID = "test-create-existing-plain"
	server, volume, origIno, payload := setupExistingRegularFile(t, volumeID)
	posBefore := volume.Position()

	resp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "f",
		Mode:        0644,
	})
	if err != nil || resp.GetError() != 0 {
		t.Fatalf("plain CreateFile on existing: err=%v errno=%d", err, resp.GetError())
	}
	if resp.GetFh() == 0 {
		t.Fatal("plain CreateFile on existing: expected non-zero handle")
	}
	inode := resp.GetAttr().GetInode()
	if inode.GetIno() != origIno {
		t.Fatalf("inode changed: got %d, want %d", inode.GetIno(), origIno)
	}
	if inode.GetMode() != syscall.S_IFREG|0600 {
		t.Fatalf("mode changed: got %o, want %o", inode.GetMode(), syscall.S_IFREG|0600)
	}
	if inode.GetSize() != int64(len(payload)) {
		t.Fatalf("size changed: got %d, want %d", inode.GetSize(), len(payload))
	}
	if volume.Position() != posBefore {
		t.Fatalf("plain CreateFile wrote transaction: position=%d, want %d", volume.Position(), posBefore)
	}
	readResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: origIno, Offset: 0, Size: int64(len(payload))})
	if err != nil || readResp.GetError() != 0 {
		t.Fatalf("ReadFile: err=%v errno=%d", err, readResp.GetError())
	}
	if !bytes.Equal(readResp.GetData(), payload) {
		t.Fatalf("content changed: got %d bytes, want %d bytes matching payload", len(readResp.GetData()), len(payload))
	}
}

func TestCreateFile_ExistingRegularFile_Exclusive(t *testing.T) {
	ctx := t.Context()
	const volumeID = "test-create-existing-excl"
	server, volume, origIno, payload := setupExistingRegularFile(t, volumeID)
	posBefore := volume.Position()

	for _, truncate := range []bool{false, true} {
		resp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        "f",
			Mode:        0644,
			Exclusive:   true,
			Truncate:    truncate,
		})
		if err != nil || resp.GetError() != int32(syscall.EEXIST) {
			t.Fatalf("exclusive CreateFile (truncate=%v): err=%v errno=%d, want EEXIST", truncate, err, resp.GetError())
		}
	}
	if volume.Position() != posBefore {
		t.Fatalf("exclusive CreateFile wrote transaction: position=%d, want %d", volume.Position(), posBefore)
	}
	attrResp, err := server.GetAttr(ctx, &pb.GetAttrRequest{VolumeId: volumeID, Inode: origIno})
	if err != nil || attrResp.GetError() != 0 {
		t.Fatalf("GetAttr: err=%v errno=%d", err, attrResp.GetError())
	}
	if attrResp.GetAttr().GetInode().GetMode() != syscall.S_IFREG|0600 || attrResp.GetAttr().GetInode().GetSize() != int64(len(payload)) {
		t.Fatalf("inode mutated after EEXIST: mode=%o size=%d", attrResp.GetAttr().GetInode().GetMode(), attrResp.GetAttr().GetInode().GetSize())
	}
	readResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: origIno, Offset: 0, Size: int64(len(payload))})
	if err != nil || readResp.GetError() != 0 || !bytes.Equal(readResp.GetData(), payload) {
		t.Fatalf("content mutated after EEXIST: err=%v errno=%d len=%d", err, readResp.GetError(), len(readResp.GetData()))
	}
}

func TestCreateFile_ExistingRegularFile_Truncate(t *testing.T) {
	ctx := t.Context()
	const volumeID = "test-create-existing-trunc"
	server, _, origIno, _ := setupExistingRegularFile(t, volumeID)

	resp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "f",
		Mode:        0644,
		Truncate:    true,
	})
	if err != nil || resp.GetError() != 0 {
		t.Fatalf("truncate CreateFile on existing: err=%v errno=%d", err, resp.GetError())
	}
	if resp.GetFh() == 0 {
		t.Fatal("truncate CreateFile on existing: expected non-zero handle")
	}
	inode := resp.GetAttr().GetInode()
	if inode.GetIno() != origIno {
		t.Fatalf("inode changed: got %d, want %d", inode.GetIno(), origIno)
	}
	if inode.GetSize() != 0 {
		t.Fatalf("size after truncate: got %d, want 0", inode.GetSize())
	}
	if inode.GetMode() != syscall.S_IFREG|0600 {
		t.Fatalf("mode changed on truncate: got %o, want %o", inode.GetMode(), syscall.S_IFREG|0600)
	}
	if inode.GetUid() != 1000 || inode.GetGid() != 1000 {
		t.Fatalf("ownership changed on truncate: got %d:%d, want 1000:1000", inode.GetUid(), inode.GetGid())
	}
	readResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: origIno, Offset: 0, Size: 8192})
	if err != nil || readResp.GetError() != 0 {
		t.Fatalf("ReadFile: err=%v errno=%d", err, readResp.GetError())
	}
	if len(readResp.GetData()) != 0 {
		t.Fatalf("expected empty content after truncate, got %d bytes", len(readResp.GetData()))
	}
}
