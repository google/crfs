// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The stargz package reads & writes tar.gz ("tarball") files in a
// seekable, indexed format call "stargz". A stargz file is still a
// valid tarball, but it's slightly bigger with new gzip streams for
// each new file & throughout large files, and has an index in a magic
// file at the end.
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
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

// TOCTarName is the name of the JSON file in the tar archive in the
// table of contents gzip stream.
const TOCTarName = "stargz.index.json"

// FooterSize is the number of bytes in the stargz footer.
//
// The footer is an empty gzip stream with no compression and an Extra
// header of the form "%016xSTARGZ", where the 64 bit hex-encoded
// number is the offset to the gzip stream of JSON TOC.
//
// 47 comes from:
//
//   10 byte gzip header +
//   2 byte (LE16) length of extra, encoding 22 (16 hex digits + len("STARGZ")) == "\x16\x00" +
//   22 bytes of extra (fmt.Sprintf("%016xSTARGZ", tocGzipOffset))
//   5 byte flate header
//   8 byte gzip footer (two little endian uint32s: digest, size)
const FooterSize = 47

// A Reader permits random access reads from a stargz file.
type Reader struct {
	sr  *io.SectionReader
	toc *jtoc

	// m stores all non-chunk entries, keyed by name.
	m map[string]*TOCEntry

	// chunks stores all TOCEntry values for regular files that
	// are split up. For a file with a single chunk, it's only
	// stored in m.
	chunks map[string][]*TOCEntry
}

// Open opens a stargz file for reading.
func Open(sr *io.SectionReader) (*Reader, error) {
	if sr.Size() < FooterSize {
		return nil, fmt.Errorf("stargz size %d is smaller than the stargz footer size", sr.Size())
	}
	// TODO: read a bigger chunk (1MB?) at once here to hopefully
	// get the TOC + footer in one go.
	var footer [FooterSize]byte
	if _, err := sr.ReadAt(footer[:], sr.Size()-FooterSize); err != nil {
		return nil, fmt.Errorf("error reading footer: %v", err)
	}
	tocOff, ok := parseFooter(footer[:])
	if !ok {
		return nil, fmt.Errorf("error parsing footer")
	}
	tocTargz := make([]byte, sr.Size()-tocOff-FooterSize)
	if _, err := sr.ReadAt(tocTargz, tocOff); err != nil {
		return nil, fmt.Errorf("error reading %d byte TOC targz: %v", len(tocTargz), err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(tocTargz))
	if err != nil {
		return nil, fmt.Errorf("malformed TOC gzip header: %v", err)
	}
	zr.Multistream(false)
	tr := tar.NewReader(zr)
	h, err := tr.Next()
	if err != nil {
		return nil, fmt.Errorf("failed to find tar header in TOC gzip stream: %v", err)
	}
	if h.Name != TOCTarName {
		return nil, fmt.Errorf("TOC tar entry had name %q; expected %q", h.Name, TOCTarName)
	}
	toc := new(jtoc)
	if err := json.NewDecoder(tr).Decode(&toc); err != nil {
		return nil, fmt.Errorf("error decoding TOC JSON: %v", err)
	}
	r := &Reader{sr: sr, toc: toc}
	r.initFields()
	return r, nil
}

// TOCEntry is an entry in the stargz file's TOC (Table of Contents).
type TOCEntry struct {
	// Name is the tar entry's name. It is the complete path
	// stored in the tar file, not just the base name.
	Name string `json:"name"`

	// Type is one of "dir", "reg", "symlink", "hardlink", or "chunk".
	// The "chunk" type is used for regular file data chunks past the first
	// TOCEntry; the 2nd chunk and on have only Type ("chunk"), Offset,
	// ChunkOffset, and ChunkSize populated.
	Type string `json:"type"`

	// Size, for regular files, is the logical size of the file.
	Size int64 `json:"size,omitempty"`

	// ModTime3339 is the modification time of the tar entry. Empty
	// means zero or unknown. Otherwise it's in UTC RFC3339
	// format. Use the ModTime method to access the time.Time value.
	ModTime3339 string `json:"modtime,omitempty"`
	modTime     time.Time

	// LinkName, for symlinks and hardlinks, is the link target.
	LinkName string `json:"linkName,omitempty"`

	// Mode is the permission and mode bits.
	Mode int64 `json:"mode,omitempty"`

	// Uid is the user ID of the owner.
	Uid int `json:"uid,omitempty"`

	// Gid is the group ID of the owner.
	Gid int `json:"gid,omitempty"`

	// Uname is the username of the owner.
	//
	// In the serialized JSON, this field may only be present for
	// the first entry with the same Uid.
	Uname string `json:"userName,omitempty"`

	// Gname is the group name of the owner.
	//
	// In the serialized JSON, this field may only be present for
	// the first entry with the same Gid.
	Gname string `json:"groupName,omitempty"`

	// Offset, for regular files, provides the offset in the
	// stargz file to the file's data bytes. See ChunkOffset and
	// ChunkSize.
	Offset int64 `json:"offset,omitempty"`

	// ChunkOffset is non-zero if this is a chunk of a large,
	// regular file. If so, the Offset is where the gzip header of
	// ChunkSize bytes at ChunkOffset in Name begin. If both
	// ChunkOffset and ChunkSize are zero, the file contents are
	// completely represented at the tar gzip stream starting at
	// Offset.
	ChunkOffset int64 `json:"chunkOffset,omitempty"`
	ChunkSize   int64 `json:"chunkSize,omitempty"`

	children map[string]*TOCEntry
}

// ModTime returns the entry's modification time.
func (e *TOCEntry) ModTime() time.Time { return e.modTime }

func (e *TOCEntry) addChild(baseName string, child *TOCEntry) {
	if e.children == nil {
		e.children = make(map[string]*TOCEntry)
	}
	e.children[baseName] = child
}

// jtoc is the JSON-serialized table of contents index of the files in the stargz file.
type jtoc struct {
	Version int         `json:"version"`
	Entries []*TOCEntry `json:"entries"`
}

// Stat returns a FileInfo value representing e.
func (e *TOCEntry) Stat() os.FileInfo { return fileInfo{e} }

// ForeachChild calls f for each child item. If f returns false, iteration ends.
// If e is not a directory, f is not called.
func (e *TOCEntry) ForeachChild(f func(baseName string, ent *TOCEntry) bool) {
	for name, ent := range e.children {
		if !f(name, ent) {
			return
		}
	}
}

// LookupChild returns the directory e's child by its base name.
func (e *TOCEntry) LookupChild(baseName string) (child *TOCEntry, ok bool) {
	child, ok = e.children[baseName]
	return
}

// fileInfo implements os.FileInfo using the wrapped *TOCEntry.
type fileInfo struct{ e *TOCEntry }

var _ os.FileInfo = fileInfo{}

func (fi fileInfo) Name() string       { return path.Base(fi.e.Name) }
func (fi fileInfo) IsDir() bool        { return fi.e.Type == "dir" }
func (fi fileInfo) Size() int64        { return fi.e.Size }
func (fi fileInfo) ModTime() time.Time { return fi.e.ModTime() }
func (fi fileInfo) Sys() interface{}   { return fi.e }
func (fi fileInfo) Mode() (m os.FileMode) {
	m = os.FileMode(fi.e.Mode) & os.ModePerm
	switch fi.e.Type {
	case "dir":
		m |= os.ModeDir
	case "symlink":
		m |= os.ModeSymlink
	}
	return m
}

// initFields populates the Reader from r.toc after decoding it from
// JSON.
//
// Unexported fields are populated and TOCEntry fields that were
// implicit in the JSON are populated.
func (r *Reader) initFields() {
	r.m = make(map[string]*TOCEntry, len(r.toc.Entries))
	r.chunks = make(map[string][]*TOCEntry)
	var lastPath string
	uname := map[int]string{}
	gname := map[int]string{}
	for _, ent := range r.toc.Entries {
		ent.Name = strings.TrimPrefix(ent.Name, "./")
		if ent.Type == "chunk" {
			ent.Name = lastPath
			r.chunks[ent.Name] = append(r.chunks[ent.Name], ent)
		} else {
			lastPath = ent.Name

			if ent.Uname != "" {
				uname[ent.Uid] = ent.Uname
			} else {
				ent.Uname = uname[ent.Uid]
			}
			if ent.Gname != "" {
				gname[ent.Gid] = ent.Gname
			} else {
				ent.Gname = uname[ent.Gid]
			}

			ent.modTime, _ = time.Parse(time.RFC3339, ent.ModTime3339)

			r.m[ent.Name] = ent
		}
		if ent.Type == "reg" && ent.ChunkSize > 0 && ent.ChunkSize < ent.Size {
			r.chunks[ent.Name] = make([]*TOCEntry, 0, ent.Size/ent.ChunkSize+1)
			r.chunks[ent.Name] = append(r.chunks[ent.Name], ent)
		}
	}

	// Populate children, add implicit directories:
	for _, ent := range r.toc.Entries {
		if ent.Type == "chunk" {
			continue
		}
		// add "foo/":
		//    add "foo" child to "" (creating "" if necessary)
		//
		// add "foo/bar/":
		//    add "bar" child to "foo" (creating "foo" if necessary)
		//
		// add "foo/bar.txt":
		//    add "bar.txt" child to "foo" (creating "foo" if necessary)
		//
		// add "a/b/c/d/e/f.txt":
		//    create "a/b/c/d/e" node
		//    add "f.txt" child to "e"

		name := ent.Name
		if ent.Type == "dir" {
			name = strings.TrimSuffix(name, "/")
		}
		pdir := r.getOrCreateDir(parentDir(name))
		pdir.addChild(path.Base(name), ent)
	}

}

func parentDir(p string) string {
	dir, _ := path.Split(p)
	return strings.TrimSuffix(dir, "/")
}

func (r *Reader) getOrCreateDir(d string) *TOCEntry {
	e, ok := r.m[d]
	if !ok {
		e = &TOCEntry{
			Name: d,
			Type: "dir",
			Mode: 0755,
		}
		r.m[d] = e
		if d != "" {
			pdir := r.getOrCreateDir(parentDir(d))
			pdir.addChild(path.Base(d), e)
		}
	}
	return e
}

// Lookup returns the Table of Contents entry for the given path.
//
// To get the root directory, use the empty string.
func (r *Reader) Lookup(path string) (e *TOCEntry, ok bool) {
	if r == nil {
		return
	}
	// TODO: decide at which stage to handle hard links. Probably
	// here? And it probably needs a link count field stored in
	// the TOCEntry.
	e, ok = r.m[path]
	return
}

func (r *Reader) OpenFile(name string) (*io.SectionReader, error) {
	ent, ok := r.Lookup(name)
	if !ok {
		// TODO: come up with some error plan. This is lazy:
		return nil, &os.PathError{
			Path: name,
			Op:   "OpenFile",
			Err:  os.ErrNotExist,
		}
	}
	if ent.Type != "reg" {
		return nil, &os.PathError{
			Path: name,
			Op:   "OpenFile",
			Err:  errors.New("not a regular file"),
		}
	}
	fr := &fileReader{
		r:    r,
		size: ent.Size,
		ents: []*TOCEntry{ent},
	}
	if ents, ok := r.chunks[name]; ok {
		fr.ents = ents
	}
	return io.NewSectionReader(fr, 0, fr.size), nil
}

type fileReader struct {
	r    *Reader
	size int64
	ents []*TOCEntry // 1 or more reg/chunk entries
}

func (fr *fileReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= fr.size {
		return 0, io.EOF
	}
	if off < 0 {
		return 0, errors.New("invalid offset")
	}
	var i int
	if len(fr.ents) > 1 {
		i = sort.Search(len(fr.ents), func(i int) bool {
			return fr.ents[i].ChunkOffset >= off
		})
		if i == -1 {
			return 0, errors.New("internal error; error finding chunk given offset")
		}
	}
	ent := fr.ents[i]
	if ent.ChunkOffset > off {
		if i == 0 {
			return 0, errors.New("internal error; first chunk offset is non-zero")
		}
		ent = fr.ents[i-1]
	}

	//  If ent is a chunk of a large file, adjust the ReadAt
	//  offset by the chunk's offset.
	off -= ent.ChunkOffset

	gzOff := ent.Offset
	sr := io.NewSectionReader(fr.r.sr, gzOff, fr.r.sr.Size()-gzOff)
	gz, err := gzip.NewReader(sr)
	if err != nil {
		return 0, fmt.Errorf("fileReader.ReadAt.gzipNewReader: %v", err)
	}
	if n, err := io.CopyN(ioutil.Discard, gz, off); n != off || err != nil {
		return 0, fmt.Errorf("discard of %d bytes = %v, %v", off, n, err)
	}
	return io.ReadFull(gz, p)
}

// A Writer writes stargz files.
//
// Use NewWriter to create a new Writer.
type Writer struct {
	bw  *bufio.Writer
	cw  *countWriter
	toc *jtoc

	closed        bool
	gz            *gzip.Writer
	lastUsername  map[int]string
	lastGroupname map[int]string

	// ChunkSize optionally controls the maximum number of bytes
	// of data of a regular file that can be written in one gzip
	// stream before a new gzip stream is started.
	// Zero means to use a default, currently 4 MiB.
	ChunkSize int
}

// currentGzipWriter writes to the current w.gz field, can change
// throughout writing a tar entry.
type currentGzipWriter struct{ w *Writer }

func (cgw currentGzipWriter) Write(p []byte) (int, error) { return cgw.w.gz.Write(p) }

func (w *Writer) chunkSize() int {
	if w.ChunkSize <= 0 {
		return 4 << 20
	}
	return w.ChunkSize
}

// NewWriter returns a new stargz writer writing to w.
//
// The writer must be closed to write its trailing table of contents.
func NewWriter(w io.Writer) *Writer {
	bw := bufio.NewWriter(w)
	cw := &countWriter{w: bw}
	return &Writer{
		bw:  bw,
		cw:  cw,
		toc: &jtoc{Version: 1},
	}
}

// Close writes the stargz's table of contents and flushes all the
// buffers, returning any error.
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	defer func() { w.closed = true }()

	if err := w.closeGz(); err != nil {
		return err
	}

	// Write the TOC index.
	tocOff := w.cw.n
	w.gz, _ = gzip.NewWriterLevel(w.cw, gzip.BestCompression)
	w.gz.Extra = []byte("stargz.toc")
	tw := tar.NewWriter(w.gz)
	tocJSON, err := json.MarshalIndent(w.toc, "", "\t")
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     TOCTarName,
		Size:     int64(len(tocJSON)),
	}); err != nil {
		return err
	}
	if _, err := tw.Write(tocJSON); err != nil {
		return err
	}

	if err := tw.Close(); err != nil {
		return err
	}
	if err := w.closeGz(); err != nil {
		return err
	}

	// And a little footer with pointer to the TOC gzip stream.
	if _, err := w.bw.Write(footerBytes(tocOff)); err != nil {
		return err
	}

	if err := w.bw.Flush(); err != nil {
		return err
	}

	return nil
}

func (w *Writer) closeGz() error {
	if w.closed {
		return errors.New("write on closed Writer")
	}
	if w.gz != nil {
		if err := w.gz.Close(); err != nil {
			return err
		}
		w.gz = nil
	}
	return nil
}

// nameIfChanged returns name, unless it was the already the value of (*mp)[id],
// in which case it returns the empty string.
func (w *Writer) nameIfChanged(mp *map[int]string, id int, name string) string {
	if name == "" {
		return ""
	}
	if *mp == nil {
		*mp = make(map[int]string)
	}
	if (*mp)[id] == name {
		return ""
	}
	(*mp)[id] = name
	return name
}

func (w *Writer) condOpenGz() {
	if w.gz == nil {
		w.gz, _ = gzip.NewWriterLevel(w.cw, gzip.BestCompression)
	}
}

// AppendTar reads the tar or tar.gz file from r and appends
// each of its contents to w.
//
// The input r can optionally be gzip compressed but the output will
// always be gzip compressed.
func (w *Writer) AppendTar(r io.Reader) error {
	br := bufio.NewReader(r)
	var tr *tar.Reader
	if isGzip(br) {
		// NewReader can't fail if isGzip returned true.
		zr, _ := gzip.NewReader(br)
		tr = tar.NewReader(zr)
	} else {
		tr = tar.NewReader(br)
	}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading from source tar: tar.Reader.Next: %v", err)
		}
		ent := &TOCEntry{
			Name:        h.Name,
			Mode:        h.Mode,
			Uid:         h.Uid,
			Gid:         h.Gid,
			Uname:       w.nameIfChanged(&w.lastUsername, h.Uid, h.Uname),
			Gname:       w.nameIfChanged(&w.lastGroupname, h.Gid, h.Gname),
			ModTime3339: formatModtime(h.ModTime),
		}
		w.condOpenGz()
		tw := tar.NewWriter(currentGzipWriter{w})
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		switch h.Typeflag {
		case tar.TypeLink:
			ent.Type = "hardlink"
			ent.LinkName = h.Linkname
		case tar.TypeSymlink:
			ent.Type = "symlink"
			ent.LinkName = h.Linkname
		case tar.TypeDir:
			ent.Type = "dir"
		case tar.TypeReg:
			ent.Type = "reg"
			ent.Size = h.Size
		default:
			return fmt.Errorf("unsupported input tar entry %q", h.Typeflag)
		}

		if h.Typeflag == tar.TypeReg {
			var written int64
			totalSize := ent.Size // save it before we destroy ent
			for written < totalSize {
				if err := w.closeGz(); err != nil {
					return err
				}

				chunkSize := int64(w.chunkSize())
				remain := totalSize - written
				if remain < chunkSize {
					chunkSize = remain
				} else {
					ent.ChunkSize = chunkSize
				}
				ent.Offset = w.cw.n
				ent.ChunkOffset = written

				w.condOpenGz()

				if _, err := io.CopyN(tw, tr, chunkSize); err != nil {
					return fmt.Errorf("error copying %q: %v", h.Name, err)
				}
				w.toc.Entries = append(w.toc.Entries, ent)
				written += chunkSize
				ent = &TOCEntry{
					Name: h.Name,
					Type: "chunk",
				}
			}
		} else {
			w.toc.Entries = append(w.toc.Entries, ent)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// footerBytes the 47 byte footer.
func footerBytes(tocOff int64) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, FooterSize))
	gz, _ := gzip.NewWriterLevel(buf, gzip.NoCompression)
	gz.Header.Extra = []byte(fmt.Sprintf("%016xSTARGZ", tocOff))
	gz.Close()
	if buf.Len() != FooterSize {
		panic(fmt.Sprintf("footer buffer = %d, not %d", buf.Len(), FooterSize))
	}
	return buf.Bytes()
}

func parseFooter(p []byte) (tocOffset int64, ok bool) {
	if len(p) != FooterSize {
		return 0, false
	}
	zr, err := gzip.NewReader(bytes.NewReader(p))
	if err != nil {
		return 0, false
	}
	extra := zr.Header.Extra
	if len(extra) != 16+len("STARGZ") {
		return 0, false
	}
	if string(extra[16:]) != "STARGZ" {
		return 0, false
	}
	tocOffset, err = strconv.ParseInt(string(extra[:16]), 16, 64)
	return tocOffset, err == nil
}

func formatModtime(t time.Time) string {
	if t.IsZero() || t.Unix() == 0 {
		return ""
	}
	return t.UTC().Round(time.Second).Format(time.RFC3339)
}

// countWriter counts how many bytes have been written to its wrapped
// io.Writer.
type countWriter struct {
	w io.Writer
	n int64
}

func (cw *countWriter) Write(p []byte) (n int, err error) {
	n, err = cw.w.Write(p)
	cw.n += int64(n)
	return
}

// isGzip reports whether br is positioned right before an upcoming gzip stream.
// It does not consume any bytes from br.
func isGzip(br *bufio.Reader) bool {
	const (
		gzipID1     = 0x1f
		gzipID2     = 0x8b
		gzipDeflate = 8
	)
	peek, _ := br.Peek(3)
	return len(peek) >= 3 && peek[0] == gzipID1 && peek[1] == gzipID2 && peek[2] == gzipDeflate
}
