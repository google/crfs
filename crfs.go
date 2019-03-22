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
	"log"
	"os"
	"sort"
	"syscall"
	"time"
	"unsafe"

	"bazil.org/fuse"
	fspkg "bazil.org/fuse/fs"
	"golang.org/x/build/crfs/stargz"
)

const debug = false

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "   %s <MOUNT_POINT>  (defaults to /crfs)\n", os.Args[0])
	flag.PrintDefaults()
}

var stargzFile = flag.String("test_stargz", "", "local stargz file for testing a single layer mount, without hitting a container registry")

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

	if *stargzFile == "" {
		log.Fatalf("TODO: network mode not done yet. Use --test_stargz for now")
	}
	fs, err := NewLocalStargzFileFS(*stargzFile)
	if err != nil {
		log.Fatal(err)
	}

	c, err := fuse.Mount(mntPoint, fuse.FSName("crfs"), fuse.Subtype("crfs"))
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

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
	r *stargz.Reader
}

func NewLocalStargzFileFS(file string) (*FS, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	r, err := stargz.Open(io.NewSectionReader(f, 0, fi.Size()))
	if err != nil {
		return nil, err
	}
	return &FS{r: r}, nil
}

// Root returns the root filesystem node for the CRFS filesystem.
// See https://godoc.org/bazil.org/fuse/fs#FS
func (fs *FS) Root() (fspkg.Node, error) {
	te, ok := fs.r.Lookup("")
	if !ok {
		return nil, errors.New("failed to find root in stargz")
	}
	return &node{fs, te}, nil
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
	}
	// TODO: socket, block, char, fifo as needed
	return fuse.DT_Unknown
}

// node is a CRFS node in the FUSE filesystem.
// See https://godoc.org/bazil.org/fuse/fs#Node
type node struct {
	fs *FS
	te *stargz.TOCEntry
}

var (
	_ fspkg.HandleReadDirAller = (*node)(nil)
	_ fspkg.Node               = (*node)(nil)
	_ fspkg.NodeStringLookuper = (*node)(nil)
	_ fspkg.NodeReadlinker     = (*node)(nil)
	_ fspkg.HandleReader       = (*node)(nil)
)

// Attr populates a with the attributes of n.
// See https://godoc.org/bazil.org/fuse/fs#Node
func (n *node) Attr(ctx context.Context, a *fuse.Attr) error {
	fi := n.te.Stat()
	a.Valid = 30 * 24 * time.Hour
	a.Inode = inodeOfEnt(n.te)
	a.Size = uint64(fi.Size())
	a.Blocks = a.Size / 512
	a.Mtime = fi.ModTime()
	a.Mode = fi.Mode()
	a.Uid = uint32(n.te.Uid)
	a.Gid = uint32(n.te.Gid)
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
	return &node{n.fs, e}, nil
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
	sr, err := n.fs.r.OpenFile(n.te.Name)
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
