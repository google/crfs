// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"bazil.org/fuse"
	"github.com/google/crfs/stargz"
)

const (
	chunkSize    = 4
	middleOffset = chunkSize / 2
	sampleData   = "0123456789"
)

// Tests *nodeHandle.Read about offset and size calculation.
func TestReadNode(t *testing.T) {
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
					t.Run(fmt.Sprintf("reading_%s_%s_%s_%s", sn, in, bo, fn), func(t *testing.T) {
						if filesize > int64(len(sampleData)) {
							t.Fatal("sample file size is larger than sample data")
						}

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

						// data we get through a nodeHandle.
						h := makeNodeHandle(t, []byte(sampleData)[:filesize], chunkSize)
						req := &fuse.ReadRequest{
							Offset: offset,
							Size:   int(size),
						}
						resp := &fuse.ReadResponse{}
						h.Read(context.TODO(), req, resp)

						if !bytes.Equal(wantData, resp.Data) {
							t.Errorf("off=%d; read data = (size=%d,data=%q); want (size=%d,data=%q)",
								offset, len(resp.Data), string(resp.Data), wantN, string(wantData))
						}
					})
				}
			}
		}
	}
}

// makeNodeHandle makes minimal nodeHandle containing a given data.
func makeNodeHandle(t *testing.T, contents []byte, chunkSize int64) *nodeHandle {
	name := "test"
	if strings.HasSuffix(name, "/") {
		t.Fatalf("bogus trailing slash in file %q", name)
	}

	// builds a sample stargz
	tr, cancel := buildSingleFileTar(t, name, contents)
	defer cancel()
	var stargzBuf bytes.Buffer
	w := stargz.NewWriter(&stargzBuf)
	w.ChunkSize = int(chunkSize)
	if err := w.AppendTar(tr); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}
	stargzData, err := ioutil.ReadAll(&stargzBuf)
	if err != nil {
		t.Fatalf("Read all stargz data: %v", err)
	}

	// opens the sample stargz and makes a nodeHandle
	sr, err := stargz.Open(io.NewSectionReader(bytes.NewReader(stargzData), 0, int64(len(stargzData))))
	if err != nil {
		t.Fatalf("Open the sample stargz file: %v", err)
	}
	te, ok := sr.Lookup(name)
	if !ok {
		t.Fatal("failed to get the sample file from the built stargz")
	}
	h := &nodeHandle{
		n: &node{
			fs: new(FS),
			te: te,
			sr: sr,
		},
	}
	h.sr, err = sr.OpenFile(name)
	if err != nil {
		t.Fatalf("failed to open the sample file %q from the built stargz: %v", name, err)
	}
	return h
}

// buildSingleFileTar makes a tar file which contains a regular file which has
// the name and contents specified by the arguments.
func buildSingleFileTar(t *testing.T, name string, contents []byte) (r io.Reader, cancel func()) {
	pr, pw := io.Pipe()
	go func() {
		tw := tar.NewWriter(pw)
		if err := tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeReg,
			Name:     name,
			Mode:     0644,
			Size:     int64(len(contents)),
		}); err != nil {
			t.Errorf("writing header to the input tar: %v", err)
			pw.Close()
			return
		}
		if _, err := tw.Write(contents); err != nil {
			t.Errorf("writing contents to the input tar: %v", err)
			pw.Close()
			return
		}
		if err := tw.Close(); err != nil {
			t.Errorf("closing write of input tar: %v", err)
		}
		pw.Close()
		return
	}()
	return pr, func() { go pr.Close(); go pw.Close() }
}
