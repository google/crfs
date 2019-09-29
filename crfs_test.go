// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"bazil.org/fuse"
	"github.com/google/crfs/stargz"
)

const (
	chunkSize  int64  = 4
	sampleData string = "0123456789"
)

// Tests *nodeHandle.Read about offset and size calculation.
func TestReadNode(t *testing.T) {
	middleOffset := chunkSize / 2
	sizeCond := map[string]int64{
		"single_chunk": chunkSize - middleOffset,
		"multi_chunks": chunkSize + middleOffset,
	}
	innerOffsetCond := map[string]int64{
		"at_top":    0,
		"at_middle": middleOffset,
	}
	baseOffsetCond := map[string]int64{
		"of_1st_chunk":  chunkSize * 0,
		"of_2nd_chunk":  chunkSize * 1,
		"of_last_chunk": chunkSize * (int64(len(sampleData)) / chunkSize),
	}
	fileSizeCond := map[string]int64{
		"in_1_chunk_file":  chunkSize * 1,
		"in_2_chunks_file": chunkSize * 2,
		"in_max_size_file": int64(len(sampleData)),
	}

	for sn, size := range sizeCond {
		for in, innero := range innerOffsetCond {
			for bo, baseo := range baseOffsetCond {
				for fn, filesize := range fileSizeCond {
					t.Run(strings.Join([]string{"reading", sn, in, bo, fn}, "_"), func(t *testing.T) {
						wantN := size
						offset := baseo + innero
						if remain := filesize - offset; remain < wantN {
							if wantN = remain; wantN < 0 {
								wantN = 0
							}
						}

						// use constant string value as a data source.
						want := strings.NewReader(sampleData)

						// data we want to get.
						wantData := make([]byte, wantN)
						_, err := want.ReadAt(wantData, offset)
						if err != nil && err != io.EOF {
							t.Fatalf("want.ReadAt (offset=%d,size=%d): %v", offset, wantN, err)
						}

						// data we get through nodeHandle.
						h := makeReadableNodeHandle(want, filesize, chunkSize)
						req := &fuse.ReadRequest{
							Offset: offset,
							Size:   int(size),
						}
						resp := &fuse.ReadResponse{}
						h.Read(context.TODO(), req, resp)

						if !bytes.Equal(wantData, resp.Data) {
							t.Errorf("off=%d; read data = (size=%d,data=%s); want (size=%d,data=%s)",
								offset, len(resp.Data), string(resp.Data), wantN, string(wantData))
						}
					})
				}
			}
		}
	}
}

// Makes minimal nodeHandle containing given reader of node and "reg" and "chunk" TOCEntries.
func makeReadableNodeHandle(ra io.ReaderAt, size int64, chunkSize int64) *nodeHandle {
	name := "test"
	reg, sr := stargz.StubRegularFileReader(name, size, chunkSize)
	return &nodeHandle{
		n: &node{
			fs: new(FS),
			te: reg,
			sr: sr,
		},
		isDir: false,
		sr:    io.NewSectionReader(ra, 0, size),
	}
}
