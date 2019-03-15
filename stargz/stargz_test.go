// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stargz

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
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
	tests := []struct {
		name      string
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
			name: "1dir_1file",
			in: tarOf(
				dir("foo/"),
				file("foo/bar.txt", "Some contents"),
			),
			wantNumGz: 4, // var dir, foo.txt alone, TOC, footer
			want: checks(
				numTOCEntries(2),
				hasDir("foo/"),
				hasFileLen("foo/bar.txt", len("Some contents")),
			),
		},
		{
			name: "2meta_2file",
			in: tarOf(
				dir("bar/"),
				dir("foo/"),
				file("foo/bar.txt", "Some contents"),
			),
			wantNumGz: 4, // both dirs, foo.txt alone, TOC, footer
			want: checks(
				numTOCEntries(3),
				hasDir("bar/"),
				hasDir("foo/"),
				hasFileLen("foo/bar.txt", len("Some contents")),
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
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, cancel := buildTarGz(t, tt.in)
			defer cancel()
			var stargzBuf bytes.Buffer
			w := NewWriter(&stargzBuf)
			if err := w.AppendTar(tr); err != nil {
				t.Fatalf("Append: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Writer.Close: %v", err)
			}
			b := stargzBuf.Bytes()

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

func countGzStreams(t *testing.T, b []byte) (numStreams int) {
	br := bufio.NewReader(bytes.NewReader(b))
	zr := new(gzip.Reader)
	for {
		if err := zr.Reset(br); err != nil {
			if err == io.EOF {
				return
			}
			t.Fatalf("countGzStreams, Reset: %v", err)
		}
		zr.Multistream(false)
		_, err := io.Copy(ioutil.Discard, zr)
		if err != nil {
			t.Fatalf("countGzStreams, Copy: %v", err)
		}
		numStreams++
	}
}

type numTOCEntries int

func (n numTOCEntries) check(t *testing.T, r *Reader) {
	if r.TOC == nil {
		t.Fatal("nil TOC")
	}
	if got, want := len(r.TOC.Entries), int(n); got != want {
		t.Errorf("got %d TOC entries; want %d", got, want)
		t.Logf("got TOC entries:")
		for i, ent := range r.TOC.Entries {
			entj, _ := json.Marshal(ent)
			t.Logf("  [%d]: %s\n", i, entj)
		}
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
		for _, ent := range r.TOC.Entries {
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

func hasDir(file string) stargzCheck {
	return stargzCheckFn(func(t *testing.T, r *Reader) {
		for _, ent := range r.TOC.Entries {
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
		for _, ent := range r.TOC.Entries {
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

func dir(d string) tarEntry {
	return tarEntryFunc(func(tw *tar.Writer) error {
		name := string(d)
		if !strings.HasSuffix(name, "/") {
			panic(fmt.Sprintf("missing trailing slash in dir %q ", name))
		}
		return tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeDir,
			Name:     name,
			Mode:     0755,
		})
	})
}

func file(name, contents string, extraAttr ...interface{}) tarEntry {
	return tarEntryFunc(func(tw *tar.Writer) error {
		if len(extraAttr) > 0 {
			return errors.New("unsupported extraAttr")
		}
		if strings.HasSuffix(name, "/") {
			return fmt.Errorf("bogus trailing slash in file %q", name)
		}
		if err := tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeReg,
			Name:     name,
			Mode:     0644,
			Size:     int64(len(contents)),
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
