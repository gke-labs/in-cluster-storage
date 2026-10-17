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

package buffer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
)

const (
	// SegmentsPrefix is the object storage key prefix where segment files are stored.
	SegmentsPrefix = "streams/wal/segments/"
)

// ListSegmentsFromBackend lists all segment objects in permanent storage under streams/wal/segments/,
// sorted in ascending order by start position. It returns the list of segment object keys
// and the highest last_position across all discovered segments (0 if no segments exist).
func ListSegmentsFromBackend(ctx context.Context, backend objectstore.Backend) ([]string, uint64, error) {
	keys, err := backend.ListObjects(ctx, "", SegmentsPrefix)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list segments under %s: %w", SegmentsPrefix, err)
	}

	type segEntry struct {
		key      string
		firstPos uint64
		lastPos  uint64
	}

	var validEntries []segEntry
	var maxLastPos uint64

	for _, k := range keys {
		if !strings.HasPrefix(k, SegmentsPrefix) || !strings.HasSuffix(k, ".wal") {
			continue
		}
		firstPos, lastPos, err := wal.ParseSegmentPath(k)
		if err != nil {
			continue
		}
		validEntries = append(validEntries, segEntry{
			key:      k,
			firstPos: firstPos,
			lastPos:  lastPos,
		})
		if lastPos > maxLastPos {
			maxLastPos = lastPos
		}
	}

	sort.Slice(validEntries, func(i, j int) bool {
		if validEntries[i].firstPos != validEntries[j].firstPos {
			return validEntries[i].firstPos < validEntries[j].firstPos
		}
		return validEntries[i].lastPos < validEntries[j].lastPos
	})

	sortedKeys := make([]string, len(validEntries))
	for i, entry := range validEntries {
		sortedKeys[i] = entry.key
	}

	return sortedKeys, maxLastPos, nil
}

// ReadSegmentFromBackend reads and decodes all LogRecords from a segment path in object storage.
func ReadSegmentFromBackend(ctx context.Context, backend objectstore.Backend, segPath string, opts ...wal.ScanOption) ([]*wal.LogRecord, error) {
	var buf bytes.Buffer
	if err := backend.GetObject(ctx, "", segPath, 0, 0, &buf); err != nil {
		return nil, fmt.Errorf("failed to fetch segment %s: %w", segPath, err)
	}

	var opt wal.ScanOptions
	for _, o := range opts {
		o(&opt)
	}

	var records []*wal.LogRecord
	r := bytes.NewReader(buf.Bytes())
	if _, _, err := wal.ReadSegmentHeader(r, opt.AcceptUnversioned); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to read segment header in %s: %w", segPath, err)
	}

	for {
		rec, err := wal.DecodeLogRecord(r)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to decode record in segment %s: %w", segPath, err)
		}
		records = append(records, rec)
	}
	return records, nil
}
