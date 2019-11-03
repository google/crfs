// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"bazil.org/fuse"
	fspkg "bazil.org/fuse/fs"
	"github.com/google/crfs/stargz"
	"golang.org/x/sys/unix"
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

// Tests if whiteouts are overlayfs-compatible.
func TestWhiteout(t *testing.T) {
	tests := []struct {
		name string
		in   []tarEntry
		want []crfsCheck
	}{
		{
			name: "1_whiteout_with_sibling",
			in: tarOf(
				dir("foo/"),
				file("foo/bar.txt", ""),
				file("foo/.wh.foo.txt", ""),
			),
			want: checks(
				hasValidWhiteout("foo/foo.txt"),
				fileNotExist("foo/.wh.foo.txt"),
			),
		},
		{
			name: "1_whiteout_with_duplicated_name",
			in: tarOf(
				dir("foo/"),
				file("foo/bar.txt", "test"),
				file("foo/.wh.bar.txt", ""),
			),
			want: checks(
				hasFileDigest("foo/bar.txt", digestFor("test")),
				fileNotExist("foo/.wh.bar.txt"),
			),
		},
		{
			name: "1_opaque",
			in: tarOf(
				dir("foo/"),
				file("foo/.wh..wh..opq", ""),
			),
			want: checks(
				hasNodeXattrs("foo/", opaqueXattr, opaqueXattrValue),
				fileNotExist("foo/.wh..wh..opq"),
			),
		},
		{
			name: "1_opaque_with_sibling",
			in: tarOf(
				dir("foo/"),
				file("foo/.wh..wh..opq", ""),
				file("foo/bar.txt", "test"),
			),
			want: checks(
				hasNodeXattrs("foo/", opaqueXattr, opaqueXattrValue),
				hasFileDigest("foo/bar.txt", digestFor("test")),
				fileNotExist("foo/.wh..wh..opq"),
			),
		},
		{
			name: "1_opaque_with_xattr",
			in: tarOf(
				dir("foo/", xAttr{"foo": "bar"}),
				file("foo/.wh..wh..opq", ""),
			),
			want: checks(
				hasNodeXattrs("foo/", opaqueXattr, opaqueXattrValue),
				hasNodeXattrs("foo/", "foo", "bar"),
				fileNotExist("foo/.wh..wh..opq"),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, cancel := buildTarGz(t, tt.in)
			defer cancel()
			var stargzBuf bytes.Buffer
			w := stargz.NewWriter(&stargzBuf)
			if err := w.AppendTar(tr); err != nil {
				t.Fatalf("Append: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Writer.Close: %v", err)
			}
			b := stargzBuf.Bytes()

			r, err := stargz.Open(io.NewSectionReader(bytes.NewReader(b), 0, int64(len(b))))
			if err != nil {
				t.Fatalf("stargz.Open: %v", err)
			}
			root, ok := r.Lookup("")
			if !ok {
				t.Fatalf("failed to find root in stargz")
			}
			for _, want := range tt.want {
				want.check(t, &node{
					te:    root,
					sr:    r,
					child: make(map[string]fspkg.Node),
				})
			}
		})
	}
}

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

func tarOf(s ...tarEntry) []tarEntry { return s }

func checks(s ...crfsCheck) []crfsCheck { return s }

type tarEntry interface {
	appendTar(*tar.Writer) error
}

type tarEntryFunc func(*tar.Writer) error

func (f tarEntryFunc) appendTar(tw *tar.Writer) error { return f(tw) }

func file(name, contents string) tarEntry {
	return tarEntryFunc(func(tw *tar.Writer) error {
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

func dir(d string, opts ...interface{}) tarEntry {
	return tarEntryFunc(func(tw *tar.Writer) error {
		var xattrs xAttr
		for _, opt := range opts {
			if v, ok := opt.(xAttr); ok {
				xattrs = v
			} else {
				return fmt.Errorf("unsupported opt")
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
			Xattrs:   xattrs,
		})
	})
}

type xAttr map[string]string

type crfsCheck interface {
	check(t *testing.T, root *node)
}

type crfsCheckFn func(*testing.T, *node)

func (f crfsCheckFn) check(t *testing.T, root *node) { f(t, root) }

func fileNotExist(file string) crfsCheck {
	return crfsCheckFn(func(t *testing.T, root *node) {
		_, _, err := getDirentAndNode(root, file)
		if err == nil {
			t.Errorf("Node %q exists", file)
		}
	})
}

func hasFileDigest(file string, digest string) crfsCheck {
	return crfsCheckFn(func(t *testing.T, root *node) {
		_, ni, err := getDirentAndNode(root, file)
		if err != nil {
			t.Fatalf("failed to get node %q: %v", file, err)
		}
		n, ok := ni.(*node)
		if !ok {
			t.Fatalf("file %q isn't a normal node", file)
		}
		if n.te.Digest != digest {
			t.Fatalf("Digest(%q) = %q, want %q", file, n.te.Digest, digest)
		}
	})
}

func hasValidWhiteout(name string) crfsCheck {
	return crfsCheckFn(func(t *testing.T, root *node) {
		ent, n, err := getDirentAndNode(root, name)
		if err != nil {
			t.Fatalf("failed to get node %q: %v", name, err)
		}
		var a fuse.Attr
		if err := n.Attr(context.Background(), &a); err != nil {
			t.Fatalf("failed to get attributes of file %q: %v", name, err)
		}
		if a.Inode != ent.Inode {
			t.Errorf("inconsistent inodes %d(Node) != %d(Dirent)", a.Inode, ent.Inode)
			return
		}

		// validate the direntry
		if ent.Type != fuse.DT_Char {
			t.Errorf("whiteout %q isn't a char device", name)
			return
		}

		// validate the node
		if a.Mode != os.ModeDevice|os.ModeCharDevice {
			t.Errorf("whiteout %q has an invalid mode %o; want %o",
				name, a.Mode, os.ModeDevice|os.ModeCharDevice)
			return
		}
		if a.Rdev != uint32(unix.Mkdev(0, 0)) {
			t.Errorf("whiteout %q has invalid device numbers (%d, %d); want (0, 0)",
				name, unix.Major(uint64(a.Rdev)), unix.Minor(uint64(a.Rdev)))
			return
		}
	})
}

func hasNodeXattrs(entry, name, value string) crfsCheck {
	return crfsCheckFn(func(t *testing.T, root *node) {
		_, ni, err := getDirentAndNode(root, entry)
		if err != nil {
			t.Fatalf("failed to get node %q: %v", entry, err)
		}
		n, ok := ni.(*node)
		if !ok {
			t.Fatalf("node %q isn't a normal node", entry)
		}

		// check xattr exists in the xattrs list.
		listres := fuse.ListxattrResponse{}
		if err := n.Listxattr(context.Background(), &fuse.ListxattrRequest{}, &listres); err != nil {
			t.Fatalf("failed to get xattrs list of node %q: %v", entry, err)
		}
		xattrs := bytes.Split(listres.Xattr, []byte{0})
		var found bool
		for _, x := range xattrs {
			if string(x) == name {
				found = true
			}
		}
		if !found {
			t.Errorf("node %q doesn't have an xattr %q", entry, name)
			return
		}

		// check the xattr has valid value.
		getres := fuse.GetxattrResponse{}
		if err := n.Getxattr(context.Background(), &fuse.GetxattrRequest{Name: name}, &getres); err != nil {
			t.Fatalf("failed to get xattr %q of node %q: %v", name, entry, err)
		}
		if string(getres.Xattr) != value {
			t.Errorf("node %q has an invalid xattr %q; want %q", entry, getres.Xattr, value)
			return
		}
	})
}

// getDirentAndNode gets dirent and node at the specified path at once and makes
// sure that the both of them exist.
func getDirentAndNode(root *node, path string) (ent fuse.Dirent, n fspkg.Node, err error) {
	dir, base := filepath.Split(filepath.Clean(path))

	// get the target's parent directory.
	d := root
	for _, name := range strings.Split(dir, "/") {
		if len(name) == 0 {
			continue
		}
		var di fspkg.Node
		di, err = d.Lookup(context.Background(), name)
		if err != nil {
			return
		}
		var ok bool
		d, ok = di.(*node)
		if !ok {
			err = fmt.Errorf("directory %q isn't a normal node", name)
			return
		}
	}

	// get the target's direntry.
	dhi, err := d.Open(context.Background(), &fuse.OpenRequest{Dir: true}, &fuse.OpenResponse{})
	if err != nil {
		return
	}
	dh, ok := dhi.(*nodeHandle)
	if !ok {
		err = fmt.Errorf("the parent directory of %q isn't a normal node", path)
		return
	}
	var ents []fuse.Dirent
	ents, err = dh.ReadDirAll(context.Background())
	if err != nil {
		return
	}
	var found bool
	for _, e := range ents {
		if e.Name == base {
			ent, found = e, true
		}
	}
	if !found {
		err = fmt.Errorf("direntry %q not found in the parent directory of %q", base, path)
		return
	}

	// get the target's node.
	if n, err = d.Lookup(context.Background(), base); err != nil {
		return
	}

	return
}

func digestFor(content string) string {
	sum := sha256.Sum256([]byte(content))
	return fmt.Sprintf("sha256:%x", sum)
}
