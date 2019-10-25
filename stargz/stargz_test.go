// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stargz

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Tests 47 byte footer encoding, size, and parsing.
func TestFooter(t *testing.T) {
	for off := int64(0); off <= 200000; off += 1023 {
		footer := footerBytes(off)
		if len(footer) != FooterSize {
			t.Fatalf("for offset %v, footer length was %d, not expected %d. got bytes: %q", off, len(footer), FooterSize, footer)
		}
		got, ok := parseFooter(footer)
		if !ok {
			t.Fatalf("failed to parse footer for offset %d, footer: %q", off, footer)
		}
		if got != off {
			t.Fatalf("parseFooter(footerBytes(offset %d)) = %d; want %d", off, got, off)

		}
	}
}

func TestWriteAndOpen(t *testing.T) {
	const content = "Some contents"
	invalidUtf8 := "\xff\xfe\xfd"

	xAttrFile := xAttr{"foo": "bar", "invalid-utf8": invalidUtf8}
	sampleOwner := owner{uid: 50, gid: 100}

	tests := []struct {
		name      string
		chunkSize int
		in        []tarEntry
		want      []stargzCheck
		wantNumGz int // expected number of gzip streams
	}{
		{
			name:      "empty",
			in:        tarOf(),
			wantNumGz: 2, // TOC + footer
			want: checks(
				numTOCEntries(0),
			),
		},
		{
			name: "1dir_1empty_file",
			in: tarOf(
				dir("foo/"),
				file("foo/bar.txt", ""),
			),
			wantNumGz: 3, // dir, TOC, footer
			want: checks(
				numTOCEntries(2),
				hasDir("foo/"),
				hasFileLen("foo/bar.txt", 0),
				entryHasChildren("foo", "bar.txt"),
			),
		},
		{
			name: "1dir_1file",
			in: tarOf(
				dir("foo/"),
				file("foo/bar.txt", content, xAttrFile),
			),
			wantNumGz: 4, // var dir, foo.txt alone, TOC, footer
			want: checks(
				numTOCEntries(2),
				hasDir("foo/"),
				hasFileLen("foo/bar.txt", len(content)),
				hasFileContentsRange("foo/bar.txt", 0, content),
				hasFileContentsRange("foo/bar.txt", 1, content[1:]),
				entryHasChildren("", "foo"),
				entryHasChildren("foo", "bar.txt"),
				hasFileXattrs("foo/bar.txt", "foo", "bar"),
				hasFileXattrs("foo/bar.txt", "invalid-utf8", invalidUtf8),
			),
		},
		{
			name: "2meta_2file",
			in: tarOf(
				dir("bar/", sampleOwner),
				dir("foo/", sampleOwner),
				file("foo/bar.txt", content, sampleOwner),
			),
			wantNumGz: 4, // both dirs, foo.txt alone, TOC, footer
			want: checks(
				numTOCEntries(3),
				hasDir("bar/"),
				hasDir("foo/"),
				hasFileLen("foo/bar.txt", len(content)),
				entryHasChildren("", "bar", "foo"),
				entryHasChildren("foo", "bar.txt"),
				hasChunkEntries("foo/bar.txt", 1),
				hasEntryOwner("bar/", sampleOwner),
				hasEntryOwner("foo/", sampleOwner),
				hasEntryOwner("foo/bar.txt", sampleOwner),
			),
		},
		{
			name: "symlink",
			in: tarOf(
				dir("foo/"),
				symlink("foo/bar", "../../x"),
			),
			wantNumGz: 3, // metas + TOC + footer
			want: checks(
				numTOCEntries(2),
				hasSymlink("foo/bar", "../../x"),
				entryHasChildren("", "foo"),
				entryHasChildren("foo", "bar"),
			),
		},
		{
			name:      "chunked_file",
			chunkSize: 4,
			in: tarOf(
				dir("foo/"),
				file("foo/big.txt", "This "+"is s"+"uch "+"a bi"+"g fi"+"le"),
			),
			wantNumGz: 9,
			want: checks(
				numTOCEntries(7), // 1 for foo dir, 6 for the foo/big.txt file
				hasDir("foo/"),
				hasFileLen("foo/big.txt", len("This is such a big file")),
				hasFileContentsRange("foo/big.txt", 0, "This is such a big file"),
				hasFileContentsRange("foo/big.txt", 1, "his is such a big file"),
				hasFileContentsRange("foo/big.txt", 2, "is is such a big file"),
				hasFileContentsRange("foo/big.txt", 3, "s is such a big file"),
				hasFileContentsRange("foo/big.txt", 4, " is such a big file"),
				hasFileContentsRange("foo/big.txt", 5, "is such a big file"),
				hasFileContentsRange("foo/big.txt", 6, "s such a big file"),
				hasFileContentsRange("foo/big.txt", 7, " such a big file"),
				hasFileContentsRange("foo/big.txt", 8, "such a big file"),
				hasFileContentsRange("foo/big.txt", 9, "uch a big file"),
				hasFileContentsRange("foo/big.txt", 10, "ch a big file"),
				hasFileContentsRange("foo/big.txt", 11, "h a big file"),
				hasFileContentsRange("foo/big.txt", 12, " a big file"),
				hasFileContentsRange("foo/big.txt", len("This is such a big file")-1, ""),
				hasChunkEntries("foo/big.txt", 6),
			),
		},
		{
			name: "block_char_fifo",
			in: tarOf(
				tarEntryFunc(func(w *tar.Writer) error {
					return w.WriteHeader(&tar.Header{
						Name:     "b",
						Typeflag: tar.TypeBlock,
						Devmajor: 123,
						Devminor: 456,
					})
				}),
				tarEntryFunc(func(w *tar.Writer) error {
					return w.WriteHeader(&tar.Header{
						Name:     "c",
						Typeflag: tar.TypeChar,
						Devmajor: 111,
						Devminor: 222,
					})
				}),
				tarEntryFunc(func(w *tar.Writer) error {
					return w.WriteHeader(&tar.Header{
						Name:     "f",
						Typeflag: tar.TypeFifo,
					})
				}),
			),
			wantNumGz: 3,
			want: checks(
				lookupMatch("b", &TOCEntry{Name: "b", Type: "block", DevMajor: 123, DevMinor: 456, NumLink: 1}),
				lookupMatch("c", &TOCEntry{Name: "c", Type: "char", DevMajor: 111, DevMinor: 222, NumLink: 1}),
				lookupMatch("f", &TOCEntry{Name: "f", Type: "fifo", NumLink: 1}),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, cancel := buildTarGz(t, tt.in)
			defer cancel()
			var stargzBuf bytes.Buffer
			w := NewWriter(&stargzBuf)
			w.ChunkSize = tt.chunkSize
			if err := w.AppendTar(tr); err != nil {
				t.Fatalf("Append: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Writer.Close: %v", err)
			}
			b := stargzBuf.Bytes()

			diffID := w.DiffID()
			wantDiffID := diffIDOfGz(t, b)
			if diffID != wantDiffID {
				t.Errorf("DiffID = %q; want %q", diffID, wantDiffID)
			}

			got := countGzStreams(t, b)
			if got != tt.wantNumGz {
				t.Errorf("number of gzip streams = %d; want %d", got, tt.wantNumGz)
			}

			r, err := Open(io.NewSectionReader(bytes.NewReader(b), 0, int64(len(b))))
			if err != nil {
				t.Fatalf("stargz.Open: %v", err)
			}
			for _, want := range tt.want {
				want.check(t, r)
			}

		})
	}
}

func diffIDOfGz(t *testing.T, b []byte) string {
	h := sha256.New()
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("diffIDOfGz: %v", err)
	}
	if _, err := io.Copy(h, zr); err != nil {
		t.Fatalf("diffIDOfGz.Copy: %v", err)
	}
	return fmt.Sprintf("sha256:%x", h.Sum(nil))
}

func countGzStreams(t *testing.T, b []byte) (numStreams int) {
	len0 := len(b)
	br := bytes.NewReader(b)
	zr := new(gzip.Reader)
	t.Logf("got gzip streams:")
	for {
		zoff := len0 - br.Len()
		if err := zr.Reset(br); err != nil {
			if err == io.EOF {
				return
			}
			t.Fatalf("countGzStreams, Reset: %v", err)
		}
		zr.Multistream(false)
		n, err := io.Copy(ioutil.Discard, zr)
		if err != nil {
			t.Fatalf("countGzStreams, Copy: %v", err)
		}
		var extra string
		if len(zr.Header.Extra) > 0 {
			extra = fmt.Sprintf("; extra=%q", zr.Header.Extra)
		}
		t.Logf("  [%d] at %d in stargz, uncompressed length %d%s", numStreams, zoff, n, extra)
		numStreams++
	}
}

type numTOCEntries int

func (n numTOCEntries) check(t *testing.T, r *Reader) {
	if r.toc == nil {
		t.Fatal("nil TOC")
	}
	if got, want := len(r.toc.Entries), int(n); got != want {
		t.Errorf("got %d TOC entries; want %d", got, want)
	}
	t.Logf("got TOC entries:")
	for i, ent := range r.toc.Entries {
		entj, _ := json.Marshal(ent)
		t.Logf("  [%d]: %s\n", i, entj)
	}
	if t.Failed() {
		t.FailNow()
	}
}

func tarOf(s ...tarEntry) []tarEntry { return s }

func checks(s ...stargzCheck) []stargzCheck { return s }

type stargzCheck interface {
	check(t *testing.T, r *Reader)
}

type stargzCheckFn func(*testing.T, *Reader)

func (f stargzCheckFn) check(t *testing.T, r *Reader) { f(t, r) }

func hasFileLen(file string, wantLen int) stargzCheck {
	return stargzCheckFn(func(t *testing.T, r *Reader) {
		for _, ent := range r.toc.Entries {
			if ent.Name == file {
				if ent.Type != "reg" {
					t.Errorf("file type of %q is %q; want \"reg\"", file, ent.Type)
				} else if ent.Size != int64(wantLen) {
					t.Errorf("file size of %q = %d; want %d", file, ent.Size, wantLen)
				}
				return
			}
		}
		t.Errorf("file %q not found", file)
	})
}

func hasFileXattrs(file, name, value string) stargzCheck {
	return stargzCheckFn(func(t *testing.T, r *Reader) {
		for _, ent := range r.toc.Entries {
			if ent.Name == file {
				if ent.Type != "reg" {
					t.Errorf("file type of %q is %q; want \"reg\"", file, ent.Type)
				}
				if ent.Xattrs == nil {
					t.Errorf("file %q has no xattrs", file)
					return
				}
				valueFound, found := ent.Xattrs[name]
				if !found {
					t.Errorf("file %q has no xattr %q", file, name)
					return
				}
				if string(valueFound) != value {
					t.Errorf("file %q has xattr %q with value %q instead of %q", file, name, valueFound, value)
				}

				return
			}
		}
		t.Errorf("file %q not found", file)
	})
}

func hasFileContentsRange(file string, offset int, want string) stargzCheck {
	return stargzCheckFn(func(t *testing.T, r *Reader) {
		f, err := r.OpenFile(file)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(want))
		n, err := f.ReadAt(got, int64(offset))
		if err != nil {
			t.Fatalf("ReadAt(len %d, offset %d) = %v, %v", len(got), offset, n, err)
		}
		if string(got) != want {
			t.Fatalf("ReadAt(len %d, offset %d) = %q, want %q", len(got), offset, got, want)
		}
	})
}

func hasChunkEntries(file string, wantChunks int) stargzCheck {
	return stargzCheckFn(func(t *testing.T, r *Reader) {
		ent, ok := r.Lookup(file)
		if !ok {
			t.Fatalf("no file for %q", file)
		}
		if ent.Type != "reg" {
			t.Fatalf("file %q has unexpected type %q; want reg", file, ent.Type)
		}
		chunks := r.getChunks(ent)
		if len(chunks) != wantChunks {
			t.Errorf("len(r.getChunks(%q)) = %d; want %d", file, len(chunks), wantChunks)
			return
		}
		f := chunks[0]

		var gotChunks []*TOCEntry
		var last *TOCEntry
		for off := int64(0); off < f.Size; off++ {
			e, ok := r.ChunkEntryForOffset(file, off)
			if !ok {
				t.Errorf("no ChunkEntryForOffset at %d", off)
				return
			}
			if last != e {
				gotChunks = append(gotChunks, e)
				last = e
			}
		}
		if !reflect.DeepEqual(chunks, gotChunks) {
			t.Errorf("gotChunks=%d, want=%d; contents mismatch", len(gotChunks), wantChunks)
		}

		// And verify the NextOffset
		for i := 0; i < len(gotChunks)-1; i++ {
			ci := gotChunks[i]
			cnext := gotChunks[i+1]
			if ci.NextOffset() != cnext.Offset {
				t.Errorf("chunk %d NextOffset %d != next chunk's Offset of %d", i, ci.NextOffset(), cnext.Offset)
			}
		}
	})
}

func entryHasChildren(dir string, want ...string) stargzCheck {
	return stargzCheckFn(func(t *testing.T, r *Reader) {
		want := append([]string(nil), want...)
		var got []string
		ent, ok := r.Lookup(dir)
		if !ok {
			t.Fatalf("didn't find TOCEntry for dir node %q", dir)
		}
		for baseName := range ent.children {
			got = append(got, baseName)
		}
		sort.Strings(got)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("children of %q = %q; want %q", dir, got, want)
		}
	})
}

func hasDir(file string) stargzCheck {
	return stargzCheckFn(func(t *testing.T, r *Reader) {
		for _, ent := range r.toc.Entries {
			if ent.Name == file {
				if ent.Type != "dir" {
					t.Errorf("file type of %q is %q; want \"dir\"", file, ent.Type)
				}
				return
			}
		}
		t.Errorf("directory %q not found", file)
	})
}

func hasSymlink(file, target string) stargzCheck {
	return stargzCheckFn(func(t *testing.T, r *Reader) {
		for _, ent := range r.toc.Entries {
			if ent.Name == file {
				if ent.Type != "symlink" {
					t.Errorf("file type of %q is %q; want \"symlink\"", file, ent.Type)
				} else if ent.LinkName != target {
					t.Errorf("link target of symlink %q is %q; want %q", file, ent.LinkName, target)
				}
				return
			}
		}
		t.Errorf("symlink %q not found", file)
	})
}

func lookupMatch(name string, want *TOCEntry) stargzCheck {
	return stargzCheckFn(func(t *testing.T, r *Reader) {
		e, ok := r.Lookup(name)
		if !ok {
			t.Fatalf("failed to Lookup entry %q", name)
		}
		if !reflect.DeepEqual(e, want) {
			t.Errorf("entry %q mismatch.\n got: %+v\nwant: %+v\n", name, e, want)
		}

	})
}

func hasEntryOwner(entry string, owner owner) stargzCheck {
	return stargzCheckFn(func(t *testing.T, r *Reader) {
		ent, ok := r.Lookup(strings.TrimSuffix(entry, "/"))
		if !ok {
			t.Errorf("entry %q not found", entry)
			return
		}
		if ent.Uid != owner.uid || ent.Gid != owner.gid {
			t.Errorf("entry %q has invalid owner (uid:%d, gid:%d) instead of (uid:%d, gid:%d)", entry, ent.Uid, ent.Gid, owner.uid, owner.gid)
			return
		}
	})
}

type tarEntry interface {
	appendTar(*tar.Writer) error
}

type tarEntryFunc func(*tar.Writer) error

func (f tarEntryFunc) appendTar(tw *tar.Writer) error { return f(tw) }

func buildTarGz(t *testing.T, ents []tarEntry) (r io.Reader, cancel func()) {
	pr, pw := io.Pipe()
	go func() {
		tw := tar.NewWriter(pw)
		for _, ent := range ents {
			if err := ent.appendTar(tw); err != nil {
				t.Errorf("building input tar: %v", err)
				pw.Close()
				return
			}
		}
		if err := tw.Close(); err != nil {
			t.Errorf("closing write of input tar: %v", err)
		}
		pw.Close()
		return
	}()
	return pr, func() { go pr.Close(); go pw.Close() }
}

func dir(d string, opts ...interface{}) tarEntry {
	return tarEntryFunc(func(tw *tar.Writer) error {
		var o owner
		for _, opt := range opts {
			if v, ok := opt.(owner); ok {
				o = v
			} else {
				return errors.New("unsupported opt")
			}
		}
		name := string(d)
		if !strings.HasSuffix(name, "/") {
			panic(fmt.Sprintf("missing trailing slash in dir %q ", name))
		}
		return tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeDir,
			Name:     name,
			Mode:     0755,
			Uid:      o.uid,
			Gid:      o.gid,
		})
	})
}

// xAttr are extended attributes to set on test files created with the file func.
type xAttr map[string]string

// owner is owner ot set on test files and directories with the file and dir functions.
type owner struct {
	uid int
	gid int
}

func file(name, contents string, opts ...interface{}) tarEntry {
	return tarEntryFunc(func(tw *tar.Writer) error {
		var xattrs xAttr
		var o owner
		for _, opt := range opts {
			switch v := opt.(type) {
			case xAttr:
				xattrs = v
			case owner:
				o = v
			default:
				return errors.New("unsupported opt")
			}
		}
		if strings.HasSuffix(name, "/") {
			return fmt.Errorf("bogus trailing slash in file %q", name)
		}
		if err := tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeReg,
			Name:     name,
			Mode:     0644,
			Xattrs:   xattrs,
			Size:     int64(len(contents)),
			Uid:      o.uid,
			Gid:      o.gid,
		}); err != nil {
			return err
		}
		_, err := io.WriteString(tw, contents)
		return err
	})
}

func symlink(name, target string) tarEntry {
	return tarEntryFunc(func(tw *tar.Writer) error {
		return tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeSymlink,
			Name:     name,
			Linkname: target,
			Mode:     0644,
		})
	})
}

// Tests *Reader.ChunkEntryForOffset about offset and size calculation.
func TestChunkEntryForOffset(t *testing.T) {
	const chunkSize = 4
	tests := []struct {
		name            string
		fileSize        int64
		reqOffset       int64
		wantOk          bool
		wantChunkOffset int64
		wantChunkSize   int64
	}{
		{
			name:            "1st_chunk_in_1_chunk_reg",
			fileSize:        chunkSize * 1,
			reqOffset:       chunkSize * 0,
			wantChunkOffset: chunkSize * 0,
			wantChunkSize:   chunkSize,
			wantOk:          true,
		},
		{
			name:      "2nd_chunk_in_1_chunk_reg",
			fileSize:  chunkSize * 1,
			reqOffset: chunkSize * 1,
			wantOk:    false,
		},
		{
			name:            "1st_chunk_in_2_chunks_reg",
			fileSize:        chunkSize * 2,
			reqOffset:       chunkSize * 0,
			wantChunkOffset: chunkSize * 0,
			wantChunkSize:   chunkSize,
			wantOk:          true,
		},
		{
			name:            "2nd_chunk_in_2_chunks_reg",
			fileSize:        chunkSize * 2,
			reqOffset:       chunkSize * 1,
			wantChunkOffset: chunkSize * 1,
			wantChunkSize:   chunkSize,
			wantOk:          true,
		},
		{
			name:      "3rd_chunk_in_2_chunks_reg",
			fileSize:  chunkSize * 2,
			reqOffset: chunkSize * 2,
			wantOk:    false,
		},
	}

	for _, te := range tests {
		t.Run(te.name, func(t *testing.T) {
			name := "test"
			_, r := regularFileReader(name, te.fileSize, chunkSize)
			ce, ok := r.ChunkEntryForOffset(name, te.reqOffset)
			if ok != te.wantOk {
				t.Errorf("ok = %v; want (%v)", ok, te.wantOk)
			} else if ok {
				if !(ce.ChunkOffset == te.wantChunkOffset && ce.ChunkSize == te.wantChunkSize) {
					t.Errorf("chunkOffset = %d, ChunkSize = %d; want (chunkOffset = %d, chunkSize = %d)",
						ce.ChunkOffset, ce.ChunkSize, te.wantChunkOffset, te.wantChunkSize)
				}
			}
		})
	}
}

// regularFileReader makes a minimal Reader of "reg" and "chunk" without tar-related information.
func regularFileReader(name string, size int64, chunkSize int64) (*TOCEntry, *Reader) {
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
