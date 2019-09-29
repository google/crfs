// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stargz

// Makes minimal Reader of "reg" and "chunk" without tar-related information.
// Used for tests of main and stargz packages.
func StubRegularFileReader(name string, size int64, chunkSize int64) (*TOCEntry, *Reader) {
	ent := &TOCEntry{
		Name: name,
		Type: "reg",
	}
	m := ent
	chunks := make([]*TOCEntry, 0, size/chunkSize+1)
	var written int64
	for written < size {
		remain := size - written
		cs := chunkSize
		if remain < cs {
			cs = remain
		}
		ent.ChunkSize = cs
		ent.ChunkOffset = written
		chunks = append(chunks, ent)
		written += cs
		ent = &TOCEntry{
			Name: name,
			Type: "chunk",
		}
	}

	if len(chunks) == 1 {
		chunks = nil
	}
	return m, &Reader{
		m:      map[string]*TOCEntry{name: m},
		chunks: map[string][]*TOCEntry{name: chunks},
	}

}
