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
	"strconv"
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
	TOC *TOC
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
	toc := new(TOC)
	if err := json.NewDecoder(tr).Decode(&toc); err != nil {
		return nil, fmt.Errorf("error decoding TOC JSON: %v", err)
	}
	return &Reader{sr: sr, TOC: toc}, nil
}

// TOCEntry is an entry in the stargz file's TOC (Table of Contents).
type TOCEntry struct {
	Offset   int64  `json:"offset,omitempty"` // offset to gzip stream of tar entry (for regular files only)
	Name     string `json:"name"`
	Type     string `json:"type"` // "dir", "reg", TODO
	Size     int64  `json:"size,omitempty"`
	LinkName string `json:"linkName,omitempty"`  // for symlinks
	Mode     int64  `json:"mode,omitempty"`      // Permission and mode bits
	Uid      int    `json:"uid,omitempty"`       // User ID of owner
	Gid      int    `json:"gid,omitempty"`       // Group ID of owner
	Uname    string `json:"userName,omitempty"`  // User name of owner
	Gname    string `json:"groupName,omitempty"` // Group name of owner
	ModTime  string `json:"modtime,omitempty"`

	// ChunkOffset is non-zero if this is a chunk of a large,
	// regular file. If so, the Offset is where the gzip header of
	// ChunkSize bytes at ChunkOffset in Name begin. If both
	// ChunkOffset and ChunkSize are zero, the file contents are
	// completely represented at the tar gzip stream starting at
	// Offset.
	ChunkOffset int64 `json:"chunkOffset,omitempty"`
	ChunkSize   int64 `json:"chunkSize,omitempty"`
}

// TOC is the table of contents index of the files in the stargz file.
type TOC struct {
	Version int        `json:"version"`
	Entries []TOCEntry `json:"entries"`
}

// A Writer writes stargz files.
//
// Use NewWriter to create a new Writer.
type Writer struct {
	bw  *bufio.Writer
	cw  *countWriter
	toc *TOC

	closed bool
	gz     *gzip.Writer
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
		toc: &TOC{Version: 1},
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
		ent := TOCEntry{
			Name:    h.Name,
			Mode:    h.Mode,
			Uid:     h.Uid,
			Gid:     h.Gid,
			Uname:   h.Uname,
			Gname:   h.Gname,
			ModTime: formatModtime(h.ModTime),
		}
		switch h.Typeflag {
		case tar.TypeLink:
			return fmt.Errorf("TODO: unsupported hardlink %q => %q", h.Name, h.Linkname)
		case tar.TypeSymlink:
			ent.Type = "symlink"
			ent.LinkName = h.Linkname
		case tar.TypeDir:
			ent.Type = "dir"
		case tar.TypeReg:
			ent.Offset = w.cw.n
			ent.Type = "reg"
			ent.Size = h.Size

			// Start a new gzip stream for regular files.
			if err := w.closeGz(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported input tar entry %q", h.Typeflag)
		}
		w.toc.Entries = append(w.toc.Entries, ent)
		if w.gz == nil {
			w.gz, err = gzip.NewWriterLevel(w.cw, gzip.BestCompression)
			if err != nil {
				return err
			}
		}
		tw := tar.NewWriter(w.gz)
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		if _, err := io.Copy(tw, tr); err != nil {
			return err
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
	t = t.UTC()
	if t.Equal(t.Round(time.Second)) {
		return t.UTC().Format(time.RFC3339)
	}
	return t.UTC().Format(time.RFC3339Nano)
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
