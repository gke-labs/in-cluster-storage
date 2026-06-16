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

package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"
)

var (
	port     = flag.Int("port", 50051, "The server port")
	dataPath = flag.String("data-path", "/data", "Path to store snapshots and blobs")
)

type server struct {
	pb.UnimplementedAgentFSControllerServer
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	if _, err = io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}

	return out.Close()
}

func calculateSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func (s *server) GetLatestSnapshot(ctx context.Context, req *pb.GetLatestSnapshotRequest) (*pb.GetLatestSnapshotResponse, error) {
	volumeID := req.GetVolumeId()
	snapshotPath := filepath.Join(*dataPath, "snapshots", volumeID, "latest.pb")

	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &pb.GetLatestSnapshotResponse{}, nil
		}
		return nil, fmt.Errorf("failed to read snapshot: %v", err)
	}

	snapshot := &pb.SnapshotMetadata{}
	if err := proto.Unmarshal(data, snapshot); err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot: %v", err)
	}

	if req.GetWantErofs() && len(snapshot.Files) > 0 {
		stageDir, err := os.MkdirTemp("", "erofs-stage-")
		if err != nil {
			return nil, fmt.Errorf("failed to create staging directory: %v", err)
		}
		defer os.RemoveAll(stageDir)

		for _, file := range snapshot.Files {
			targetFile := filepath.Join(stageDir, file.Path)
			if err := os.MkdirAll(filepath.Dir(targetFile), 0755); err != nil {
				return nil, fmt.Errorf("failed to create target file dir: %v", err)
			}

			if req.GetLazyLoadThreshold() >= 0 && file.Size >= req.GetLazyLoadThreshold() {
				// Create sparse placeholder
				f, err := os.OpenFile(targetFile, os.O_CREATE|os.O_WRONLY, os.FileMode(file.Mode))
				if err != nil {
					return nil, fmt.Errorf("failed to create placeholder for %s: %v", file.Path, err)
				}
				if err := f.Truncate(file.Size); err != nil {
					_ = f.Close()
					return nil, fmt.Errorf("failed to truncate placeholder for %s: %v", file.Path, err)
				}
				if err := f.Close(); err != nil {
					return nil, fmt.Errorf("failed to close placeholder for %s: %v", file.Path, err)
				}
			} else {
				// Copy fully downloaded file
				blobPath := filepath.Join(*dataPath, "blobs", file.Sha256)
				if err := copyFile(blobPath, targetFile, os.FileMode(file.Mode)); err != nil {
					return nil, fmt.Errorf("failed to copy blob for %s: %v", file.Path, err)
				}
			}

			// Restore mod time
			if err := os.Chtimes(targetFile, file.ModTime.AsTime(), file.ModTime.AsTime()); err != nil {
				klog.Warningf("failed to set times for %s: %v", file.Path, err)
			}
		}

		// Compile staging directory to EROFS image
		tmpDir := filepath.Join(*dataPath, "tmp")
		if err := os.MkdirAll(tmpDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create temp directory %s: %v", tmpDir, err)
		}
		imgFile, err := os.CreateTemp(tmpDir, "erofs-img-")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp erofs img: %v", err)
		}
		imgPath := imgFile.Name()
		if err := imgFile.Close(); err != nil {
			os.Remove(imgPath)
			return nil, fmt.Errorf("failed to close temp erofs img: %v", err)
		}
		defer os.Remove(imgPath)

		cmd := exec.Command("mkfs.erofs", imgPath, stageDir)
		if out, err := cmd.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("failed to build EROFS: %v, output: %s", err, string(out))
		}

		// Compute the SHA256 of the EROFS image
		erofsSha, err := calculateSHA256(imgPath)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate erofs SHA256: %v", err)
		}

		// Save the EROFS image as a normal blob
		blobPath := filepath.Join(*dataPath, "blobs", erofsSha)
		if err := os.MkdirAll(filepath.Dir(blobPath), 0755); err != nil {
			return nil, fmt.Errorf("failed to create blobs directory: %v", err)
		}

		// Move/Rename
		if err := os.Rename(imgPath, blobPath); err != nil {
			return nil, fmt.Errorf("failed to rename erofs image to blobs path %s: %v", blobPath, err)
		}

		snapshot.ErofsSha256 = erofsSha
	}

	return &pb.GetLatestSnapshotResponse{Snapshot: snapshot}, nil
}

func (s *server) UploadSnapshot(ctx context.Context, req *pb.UploadSnapshotRequest) (*pb.UploadSnapshotResponse, error) {
	volumeID := req.GetVolumeId()
	snapshot := req.GetSnapshot()

	data, err := proto.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal snapshot: %v", err)
	}

	snapshotDir := filepath.Join(*dataPath, "snapshots", volumeID)
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot dir: %v", err)
	}

	snapshotPath := filepath.Join(snapshotDir, "latest.pb")
	if err := os.WriteFile(snapshotPath, data, 0644); err != nil {
		return nil, fmt.Errorf("failed to write snapshot: %v", err)
	}

	return &pb.UploadSnapshotResponse{Success: true}, nil
}

func (s *server) UploadBlob(stream pb.AgentFSController_UploadBlobServer) error {
	var sha256 string
	var file *os.File
	var tempPath string

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			if file != nil {
				file.Close()
				blobPath := filepath.Join(*dataPath, "blobs", sha256)
				if err := os.Rename(tempPath, blobPath); err != nil {
					return fmt.Errorf("failed to rename blob file: %v", err)
				}
			}
			return stream.SendAndClose(&pb.UploadBlobResponse{Success: true})
		}
		if err != nil {
			if file != nil {
				file.Close()
				os.Remove(tempPath)
			}
			return err
		}

		switch x := req.Data.(type) {
		case *pb.UploadBlobRequest_Sha256:
			sha256 = x.Sha256
			blobDir := filepath.Join(*dataPath, "blobs")
			if err := os.MkdirAll(blobDir, 0755); err != nil {
				return fmt.Errorf("failed to create blob dir: %v", err)
			}
			f, err := os.CreateTemp(blobDir, "upload-")
			if err != nil {
				return fmt.Errorf("failed to create temp file: %v", err)
			}
			file = f
			tempPath = f.Name()
		case *pb.UploadBlobRequest_Content:
			if file == nil {
				return fmt.Errorf("received content before sha256")
			}
			if _, err := file.Write(x.Content); err != nil {
				return fmt.Errorf("failed to write to blob file: %v", err)
			}
		}
	}
}

func (s *server) DownloadBlob(req *pb.DownloadBlobRequest, stream pb.AgentFSController_DownloadBlobServer) error {
	sha256 := req.GetSha256()
	blobPath := filepath.Join(*dataPath, "blobs", sha256)

	file, err := os.Open(blobPath)
	if err != nil {
		return fmt.Errorf("failed to open blob file: %v", err)
	}
	defer file.Close()

	buffer := make([]byte, 1024*1024) // 1MB buffer
	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read from blob file: %v", err)
		}
		if err := stream.Send(&pb.DownloadBlobResponse{Content: buffer[:n]}); err != nil {
			return err
		}
	}

	return nil
}

func (s *server) HasBlob(ctx context.Context, req *pb.HasBlobRequest) (*pb.HasBlobResponse, error) {
	sha256 := req.GetSha256()
	blobPath := filepath.Join(*dataPath, "blobs", sha256)

	_, err := os.Stat(blobPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &pb.HasBlobResponse{Exists: false}, nil
		}
		return nil, fmt.Errorf("failed to stat blob file: %v", err)
	}

	return &pb.HasBlobResponse{Exists: true}, nil
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	if err := os.MkdirAll(*dataPath, 0755); err != nil {
		klog.Fatalf("failed to create data path %s: %v", *dataPath, err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		klog.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterAgentFSControllerServer(s, &server{})

	klog.Infof("Server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		klog.Fatalf("failed to serve: %v", err)
	}
}
