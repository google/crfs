// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The crfs command runs the Container Registry Filesystem, providing a read-only
// FUSE filesystem for container images.
//
// For purposes of documentation, we'll assume you've mounted this at /crfs.
//
// Currently (as of 2019-03-21) it only mounts a single layer at the top level.
// In the future it'll have paths like:
//
//    /crfs/image/gcr.io/foo-proj/image/latest
//    /crfs/layer/gcr.io/foo-proj/image/latest/xxxxxxxxxxxxxx
//
// For mounting a squashed image and a layer, respectively, with the
// host, owner, image name, and version encoded in the path
// components.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"bazil.org/fuse"
	fspkg "bazil.org/fuse/fs"
	"github.com/google/crfs/stargz"
	"golang.org/x/sys/unix"
)

const debug = false

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "   %s <MOUNT_POINT>  (defaults to /crfs)\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Parse()
	mntPoint := "/crfs"
	if flag.NArg() > 1 {
		usage()
		os.Exit(2)
	}
	if flag.NArg() == 1 {
		mntPoint = flag.Arg(0)
	}

	c, err := fuse.Mount(mntPoint, fuse.FSName("crfs"), fuse.Subtype("crfs"), fuse.ReadOnly())
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	defer fuse.Unmount(mntPoint)

	fs := new(FS)
	err = fspkg.Serve(c, fs)
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

// FS is the CRFS filesystem.
// It implements https://godoc.org/bazil.org/fuse/fs#FS
type FS struct {
	// TODO: options, probably. logger, etc.
}

// Root returns the root filesystem node for the CRFS filesystem.
// See https://godoc.org/bazil.org/fuse/fs#FS
func (fs *FS) Root() (fspkg.Node, error) {
	return &rootNode{fs}, nil
}

// rootNode is the contents of /crfs.
// Children include:
//    layers/ -- individual layers; directories by hostname/user/layer
//    images/ -- merged layers; directories by hostname/user/layer
//    README-crfs.txt
type rootNode struct {
	fs *FS
}

func (n *rootNode) Attr(ctx context.Context, a *fuse.Attr) error {
	setDirAttr(a)
	a.Valid = 30 * 24 * time.Hour
	return nil
}

func (n *rootNode) ReadDirAll(ctx context.Context) (ents []fuse.Dirent, err error) {
	ents = append(ents,
		fuse.Dirent{Type: fuse.DT_Dir, Name: "layers"},
		fuse.Dirent{Type: fuse.DT_Dir, Name: "images"},
		fuse.Dirent{Type: fuse.DT_File, Name: "README-crfs.txt"},
	)
	return
}

func (n *rootNode) Lookup(ctx context.Context, name string) (fspkg.Node, error) {
	switch name {
	case "layers":
		return &layersRoot{n.fs}, nil
	case "images":
		return &imagesRoot{n.fs}, nil
	case "README-crfs.txt":
		return &staticFile{"This is CRFS. See https://github.com/google/crfs.\n"}, nil
	}
	return nil, os.ErrNotExist
}

func setDirAttr(a *fuse.Attr) {
	a.Mode = 0755 | os.ModeDir
	// TODO: more?
}

// layersRoot is the contents of /crfs/layers/
//
// Its children are hostnames (such as "gcr.io").
//
// A special directory, "local", permits walking into stargz files on
// disk, local to the directory where crfs is running. This is useful for
// debugging.
type layersRoot struct {
	fs *FS
}

func (n *layersRoot) Attr(ctx context.Context, a *fuse.Attr) error {
	setDirAttr(a)
	a.Valid = 30 * 24 * time.Hour
	return nil
}

var commonRegistryHostnames = []string{
	"gcr.io",
	"us.gcr.io",
	"eu.gcr.io",
	"asia.gcr.io",
	"index.docker.io",
}

func (n *layersRoot) ReadDirAll(ctx context.Context) (ents []fuse.Dirent, err error) {
	for _, n := range commonRegistryHostnames {
		ents = append(ents, fuse.Dirent{Type: fuse.DT_Dir, Name: n})
	}
	ents = append(ents, fuse.Dirent{Type: fuse.DT_Dir, Name: "local"})
	return
}

func (n *layersRoot) Lookup(ctx context.Context, name string) (fspkg.Node, error) {
	if name == "local" {
		return &layerDebugRoot{n.fs}, nil
	}
	// TODO: validation that it's a halfway valid looking hostname?
	return &layerHost{n.fs, name}, nil
}

// layerDebugRoot is /crfs/layers/local/
// Its contents are *.star.gz files in the current directory.
type layerDebugRoot struct {
	fs *FS
}

func (n *layerDebugRoot) Attr(ctx context.Context, a *fuse.Attr) error {
	setDirAttr(a)
	return nil
}

func (n *layerDebugRoot) ReadDirAll(ctx context.Context) (ents []fuse.Dirent, err error) {
	fis, err := ioutil.ReadDir(".")
	for _, fi := range fis {
		name := fi.Name()
		if !strings.HasSuffix(name, ".stargz") {
			continue
		}
		ents = append(ents, fuse.Dirent{Type: fuse.DT_Dir, Name: name})
	}
	return ents, err
}

func (n *layerDebugRoot) Lookup(ctx context.Context, name string) (fspkg.Node, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	r, err := stargz.Open(io.NewSectionReader(f, 0, fi.Size()))
	if err != nil {
		f.Close()
		log.Printf("error opening local stargz: %v", err)
		return nil, err
	}
	root, ok := r.Lookup("")
	if !ok {
		f.Close()
		return nil, errors.New("failed to find root in stargz")
	}
	return &node{
		fs: n.fs,
		te: root,
		sr: r,
		f:  f,
	}, nil
}

// layerHost is, say, /crfs/layers/gcr.io/ (with host == "gcr.io")
//
// Its children are the next level (GCP project, docker hub owner).
type layerHost struct {
	fs   *FS
	host string
}

func (n *layerHost) Attr(ctx context.Context, a *fuse.Attr) error {
	setDirAttr(a)
	a.Valid = 15 * time.Second
	return nil
}

// imagesRoot is the contents of /crfs/images/
// Its children are hostnames (such as "gcr.io").
type imagesRoot struct {
	fs *FS
}

func (n *imagesRoot) Attr(ctx context.Context, a *fuse.Attr) error {
	setDirAttr(a)
	a.Valid = 30 * 24 * time.Hour
	return nil
}

func (n *imagesRoot) ReadDirAll(ctx context.Context) (ents []fuse.Dirent, err error) {
	for _, n := range commonRegistryHostnames {
		ents = append(ents, fuse.Dirent{Type: fuse.DT_Dir, Name: n})
	}
	return
}

type staticFile struct {
	contents string
}

func (f *staticFile) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = 0644
	a.Size = uint64(len(f.contents))
	a.Blocks = blocksOf(a.Size)
	return nil
}

func (f *staticFile) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	if req.Offset < 0 {
		return syscall.EINVAL
	}
	if req.Offset > int64(len(f.contents)) {
		resp.Data = nil
		return nil
	}
	bufSize := int64(req.Size)
	remain := int64(len(f.contents)) - req.Offset
	if bufSize > remain {
		bufSize = remain
	}
	resp.Data = make([]byte, bufSize)
	n := copy(resp.Data, f.contents[req.Offset:])
	resp.Data = resp.Data[:n] // redundant, but for clarity
	return nil
}

func inodeOfEnt(ent *stargz.TOCEntry) uint64 {
	return uint64(uintptr(unsafe.Pointer(ent)))
}

func direntType(ent *stargz.TOCEntry) fuse.DirentType {
	switch ent.Type {
	case "dir":
		return fuse.DT_Dir
	case "reg":
		return fuse.DT_File
	case "symlink":
		return fuse.DT_Link
	case "block":
		return fuse.DT_Block
	case "char":
		return fuse.DT_Char
	case "fifo":
		return fuse.DT_FIFO
	}
	return fuse.DT_Unknown
}

// node is a CRFS node in the FUSE filesystem.
// See https://godoc.org/bazil.org/fuse/fs#Node
type node struct {
	fs *FS
	te *stargz.TOCEntry
	sr *stargz.Reader
	f  *os.File // non-nil if root & in debug mode
}

var (
	_ fspkg.HandleReadDirAller = (*node)(nil)
	_ fspkg.Node               = (*node)(nil)
	_ fspkg.NodeStringLookuper = (*node)(nil)
	_ fspkg.NodeReadlinker     = (*node)(nil)
	_ fspkg.HandleReader       = (*node)(nil)
	// TODO: implement NodeReleaser and n.f.Close() when n.f is non-nil
)

func blocksOf(size uint64) (blocks uint64) {
	blocks = size / 512
	if size%512 > 0 {
		blocks++
	}
	return
}

// Attr populates a with the attributes of n.
// See https://godoc.org/bazil.org/fuse/fs#Node
func (n *node) Attr(ctx context.Context, a *fuse.Attr) error {
	fi := n.te.Stat()
	a.Valid = 30 * 24 * time.Hour
	a.Inode = inodeOfEnt(n.te)
	a.Size = uint64(fi.Size())
	a.Blocks = blocksOf(a.Size)
	a.Mtime = fi.ModTime()
	a.Mode = fi.Mode()
	a.Uid = uint32(n.te.Uid)
	a.Gid = uint32(n.te.Gid)
	a.Rdev = uint32(unix.Mkdev(uint32(n.te.DevMajor), uint32(n.te.DevMinor)))
	a.Nlink = 1 // TODO: get this from te once hardlinks are more supported
	if debug {
		log.Printf("attr of %s: %s", n.te.Name, *a)
	}
	return nil
}

// ReadDirAll returns all directory entries in the directory node n.
//
// https://godoc.org/bazil.org/fuse/fs#HandleReadDirAller
func (n *node) ReadDirAll(ctx context.Context) (ents []fuse.Dirent, err error) {
	n.te.ForeachChild(func(baseName string, ent *stargz.TOCEntry) bool {
		ents = append(ents, fuse.Dirent{
			Inode: inodeOfEnt(ent),
			Type:  direntType(ent),
			Name:  baseName,
		})
		return true
	})
	sort.Slice(ents, func(i, j int) bool { return ents[i].Name < ents[j].Name })
	return ents, nil
}

// Lookup looks up a child entry of the directory node n.
//
// See https://godoc.org/bazil.org/fuse/fs#NodeStringLookuper
func (n *node) Lookup(ctx context.Context, name string) (fspkg.Node, error) {
	e, ok := n.te.LookupChild(name)
	if !ok {
		return nil, syscall.ENOENT
	}
	return &node{n.fs, e, n.sr, nil}, nil
}

// Readlink reads the target of a symlink.
//
// See https://godoc.org/bazil.org/fuse/fs#NodeReadlinker
func (n *node) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	if n.te.Type != "symlink" {
		return "", syscall.EINVAL
	}
	return n.te.LinkName, nil
}

// Read reads data from a regular file n.
//
// See https://godoc.org/bazil.org/fuse/fs#HandleReader
func (n *node) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	sr, err := n.sr.OpenFile(n.te.Name)
	if err != nil {
		return err
	}

	resp.Data = make([]byte, req.Size)
	nr, err := sr.ReadAt(resp.Data, req.Offset)
	if nr < req.Size {
		resp.Data = resp.Data[:nr]
	}
	if debug {
		log.Printf("Read response: size=%d @ %d, read %d", req.Size, req.Offset, nr)
	}
	return nil
}
