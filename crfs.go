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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"bazil.org/fuse"
	fspkg "bazil.org/fuse/fs"
	"cloud.google.com/go/compute/metadata"
	"github.com/google/crfs/stargz"
	namepkg "github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/google"
	"golang.org/x/sys/unix"
)

const debug = false

var (
	fuseDebug = flag.Bool("fuse_debug", false, "enable verbose FUSE debugging")
)

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
	if *fuseDebug {
		fuse.Debug = func(msg interface{}) {
			log.Printf("fuse debug: %v", msg)
		}
	}

	log.Printf("crfs: mounting")
	c, err := fuse.Mount(mntPoint, fuse.FSName("crfs"), fuse.Subtype("crfs"), fuse.ReadOnly(), fuse.AllowOther())
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	defer fuse.Unmount(mntPoint)

	log.Printf("crfs: serving")
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
	return &rootNode{
		fs: fs,
		dirEnts: dirEnts{initChildren: func(de *dirEnts) {
			de.m["layers"] = &dirEnt{
				dtype: fuse.DT_Dir,
				lookupNode: func(inode uint64) (fspkg.Node, error) {
					return newLayersRoot(fs, inode), nil
				},
			}
			de.m["images"] = &dirEnt{
				dtype: fuse.DT_Dir,
				lookupNode: func(inode uint64) (fspkg.Node, error) {
					return &imagesRoot{fs: fs, inode: inode}, nil
				},
			}
			de.m["README-crfs.txt"] = &dirEnt{
				dtype: fuse.DT_File,
				lookupNode: func(inode uint64) (fspkg.Node, error) {
					return &staticFile{
						inode:    inode,
						contents: "This is CRFS. See https://github.com/google/crfs.\n",
					}, nil
				},
			}
		}},
	}, nil
}

// imagesOfHost returns the images for the given registry host and
// owner (e.g. GCP project name).
//
// Note that this is gcr.io specific as there's no way in the Registry
// protocol to do this. So this won't work for index.docker.io. We'll
// need to do something else there.
// TODO: something else for docker hub.
func (fs *FS) imagesOfHost(ctx context.Context, host, owner string) (imageNames []string, err error) {
	req, err := http.NewRequest("GET", "https://"+host+"/v2/"+owner+"/tags/list", nil)
	if err != nil {
		return nil, err
	}
	// TODO: auth. This works for public stuff so far, though.
	req = req.WithContext(ctx)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, errors.New(res.Status)
	}
	var resj struct {
		Images []string `json:"child"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resj); err != nil {
		return nil, err
	}
	sort.Strings(resj.Images)
	return resj.Images, nil
}

type manifest struct {
	SchemaVersion int        `json:"schemaVersion"`
	MediaType     string     `json:"mediaType"`
	Config        *blobRef   `json:"config"`
	Layers        []*blobRef `json:"layers"`
}

type blobRef struct {
	Size      int64  `json:"size"`
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
}

func (fs *FS) getManifest(ctx context.Context, host, owner, image, ref string) (*manifest, error) {
	urlStr := "https://" + host + "/v2/" + owner + "/" + image + "/manifests/" + ref
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, err
	}
	// TODO: auth. This works for public stuff so far, though.
	req = req.WithContext(ctx)
	req.Header.Set("Accept", "*") // application/vnd.docker.distribution.manifest.v2+json
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		slurp, _ := ioutil.ReadAll(res.Body)
		return nil, fmt.Errorf("non-200 for %q: %v, %q", urlStr, res.Status, slurp)
	}
	resj := new(manifest)
	if err := json.NewDecoder(res.Body).Decode(resj); err != nil {
		return nil, err
	}
	return resj, nil
}

func (fs *FS) getConfig(ctx context.Context, host, owner, image, ref string) (string, error) {
	urlStr := "https://" + host + "/v2/" + owner + "/" + image + "/blobs/" + ref
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return "", err
	}
	req = req.WithContext(ctx)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		slurp, _ := ioutil.ReadAll(res.Body)
		return "", fmt.Errorf("non-200 for %q: %v, %q", urlStr, res.Status, slurp)
	}
	slurp, err := ioutil.ReadAll(res.Body)
	return string(slurp), err
}

type dirEnt struct {
	lazyInode
	dtype      fuse.DirentType
	lookupNode func(inode uint64) (fspkg.Node, error)
}

type dirEnts struct {
	initOnce     sync.Once
	initChildren func(*dirEnts)
	mu           sync.Mutex
	m            map[string]*dirEnt
}

func (de *dirEnts) Lookup(ctx context.Context, name string) (fspkg.Node, error) {
	de.condInit()
	de.mu.Lock()
	defer de.mu.Unlock()
	e, ok := de.m[name]
	if !ok {
		log.Printf("returning ENOENT for name %q", name)
		return nil, fuse.ENOENT
	}
	if e.lookupNode == nil {
		log.Printf("node %q has no lookupNode defined", name)
		return nil, fuse.ENOENT
	}
	return e.lookupNode(e.inode())
}

func (de *dirEnts) ReadDirAll(ctx context.Context) (ents []fuse.Dirent, err error) {
	de.condInit()
	de.mu.Lock()
	defer de.mu.Unlock()
	ents = make([]fuse.Dirent, 0, len(de.m))
	for name, e := range de.m {
		ents = append(ents, fuse.Dirent{
			Name:  name,
			Inode: e.inode(),
			Type:  e.dtype,
		})
	}
	sort.Slice(ents, func(i, j int) bool { return ents[i].Name < ents[j].Name })
	return ents, nil
}

func (de *dirEnts) condInit() { de.initOnce.Do(de.doInit) }
func (de *dirEnts) doInit() {
	de.m = map[string]*dirEnt{}
	if de.initChildren != nil {
		de.initChildren(de)
	}
}

// atomicInodeIncr holds the most previously allocate global inode number.
// It should only be accessed/incremented with sync/atomic.
var atomicInodeIncr uint32

// lazyInode is a lazily-allocated inode number.
//
// We only use 32 bits out of 64 to leave room for overlayfs to play
// games with the upper bits. TODO: maybe that's not necessary.
type lazyInode struct{ v uint32 }

func (si *lazyInode) inode() uint64 {
	for {
		v := atomic.LoadUint32(&si.v)
		if v != 0 {
			return uint64(v)
		}
		v = atomic.AddUint32(&atomicInodeIncr, 1)
		if atomic.CompareAndSwapUint32(&si.v, 0, v) {
			return uint64(v)
		}
	}
}

// childInodeNumberCache is a temporary, lazily solution to having
// stable inode numbers in node types where we haven't yet pushed it
// down properly. This map grows forever (which is bad) and maps the
// tuple (parent directory inode, child name string) to the child's
// inode number. Its map key type is inodeAndString
var childInodeNumberCache sync.Map

type inodeAndString struct {
	inode     uint64
	childName string
}

func getOrMakeChildInode(inode uint64, childName string) uint64 {
	key := inodeAndString{inode, childName}
	if v, ok := childInodeNumberCache.Load(key); ok {
		log.Printf("re-using inode %v/%q = %v", inode, childName, v)
		return v.(uint64)
	}
	actual, loaded := childInodeNumberCache.LoadOrStore(key, uint64(atomic.AddUint32(&atomicInodeIncr, 1)))
	if loaded {
		log.Printf("race lost creating inode %v/%q = %v", inode, childName, actual)
	} else {
		log.Printf("created inode %v/%q = %v", inode, childName, actual)
	}
	return actual.(uint64)
}

// rootNode is the contents of /crfs.
// Children include:
//    layers/ -- individual layers; directories by hostname/user/layer
//    images/ -- merged layers; directories by hostname/user/layer
//    README-crfs.txt
type rootNode struct {
	fs *FS
	dirEnts
	lazyInode
}

func (n *rootNode) Attr(ctx context.Context, a *fuse.Attr) error {
	setDirAttr(a)
	a.Inode = n.inode()
	a.Valid = 30 * 24 * time.Hour
	return nil
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
	fs    *FS
	inode uint64
	dirEnts
}

func newLayersRoot(fs *FS, inode uint64) *layersRoot {
	lr := &layersRoot{fs: fs, inode: inode}
	lr.dirEnts.initChildren = func(de *dirEnts) {
		de.m["local"] = &dirEnt{
			dtype: fuse.DT_Dir,
			lookupNode: func(inode uint64) (fspkg.Node, error) {
				return &layerDebugRoot{fs: fs, inode: inode}, nil
			},
		}
		for _, n := range commonRegistryHostnames {
			lr.addHostDirLocked(n)
		}
	}
	return lr
}

func (n *layersRoot) Attr(ctx context.Context, a *fuse.Attr) error {
	setDirAttr(a)
	a.Valid = 30 * 24 * time.Hour
	a.Inode = n.inode
	return nil
}

var commonRegistryHostnames = []string{
	"gcr.io",
	"us.gcr.io",
	"eu.gcr.io",
	"asia.gcr.io",
	"index.docker.io",
}

func isGCR(host string) bool {
	return host == "gcr.io" || strings.HasSuffix(host, ".gcr.io")
}

func (n *layersRoot) addHostDirLocked(name string) {
	n.dirEnts.m[name] = &dirEnt{
		dtype: fuse.DT_Dir,
		lookupNode: func(inode uint64) (fspkg.Node, error) {
			return newLayerHost(n.fs, name, inode), nil
		},
	}
}

func (n *layersRoot) Lookup(ctx context.Context, name string) (fspkg.Node, error) {
	child, err := n.dirEnts.Lookup(ctx, name)
	if err != fuse.ENOENT {
		return child, err
	}
	// TODO: validate name looks like a hostname?
	n.dirEnts.mu.Lock()
	if _, ok := n.dirEnts.m[name]; !ok {
		n.addHostDirLocked(name)
	}
	n.dirEnts.mu.Unlock()
	return n.dirEnts.Lookup(ctx, name)
}

// layerDebugRoot is /crfs/layers/local/
// Its contents are *.star.gz files in the current directory.
type layerDebugRoot struct {
	fs    *FS
	inode uint64
}

func (n *layerDebugRoot) Attr(ctx context.Context, a *fuse.Attr) error {
	setDirAttr(a)
	a.Inode = n.inode
	return nil
}

func (n *layerDebugRoot) ReadDirAll(ctx context.Context) (ents []fuse.Dirent, err error) {
	fis, err := ioutil.ReadDir(".")
	for _, fi := range fis {
		name := fi.Name()
		if !strings.HasSuffix(name, ".stargz") {
			continue
		}
		// TODO: populate inode number
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
		fs:    n.fs,
		te:    root,
		sr:    r,
		f:     f,
		child: make(map[string]*node),
	}, nil
}

// layerHost is, say, /crfs/layers/gcr.io/ (with host == "gcr.io")
//
// Its children are the next level (GCP project, docker hub owner), a layerHostOwner.
type layerHost struct {
	fs    *FS
	host  string
	inode uint64
	dirEnts
}

func newLayerHost(fs *FS, host string, inode uint64) *layerHost {
	n := &layerHost{
		fs:    fs,
		host:  host,
		inode: inode,
	}
	n.dirEnts = dirEnts{
		initChildren: func(de *dirEnts) {
			if !isGCR(n.host) || !metadata.OnGCE() {
				return
			}
			if proj, _ := metadata.ProjectID(); proj != "" {
				n.addLayerHostOwnerLocked(proj)
			}
		},
	}
	return n
}

func (n *layerHost) Attr(ctx context.Context, a *fuse.Attr) error {
	setDirAttr(a)
	a.Valid = 15 * time.Second
	a.Inode = n.inode
	return nil
}

var gcpProjRE = regexp.MustCompile(`^[a-z]([-a-z0-9]*[a-z0-9])?$`)

func (n *layerHost) addLayerHostOwnerLocked(owner string) { // owner == GCP project on gcr.io
	n.dirEnts.m[owner] = &dirEnt{
		dtype: fuse.DT_Dir,
		lookupNode: func(inode uint64) (fspkg.Node, error) {
			return &layerHostOwner{
				fs:    n.fs,
				host:  n.host,
				owner: owner,
				inode: inode,
			}, nil
		},
	}
}

func (n *layerHost) Lookup(ctx context.Context, name string) (fspkg.Node, error) {
	child, err := n.dirEnts.Lookup(ctx, name)
	if err != fuse.ENOENT {
		return child, err
	}

	// For gcr.io hosts, the next level lookup is the GCP project name,
	// which we can validate.
	if isGCR(n.host) {
		proj := name
		if len(name) < 6 || len(name) > 30 || !gcpProjRE.MatchString(proj) {
			return nil, fuse.ENOENT
		}
	} else {
		// TODO: validate index.docker.io next level lookups
	}

	n.dirEnts.mu.Lock()
	if _, ok := n.dirEnts.m[name]; !ok {
		n.addLayerHostOwnerLocked(name)
	}
	n.dirEnts.mu.Unlock()

	return n.dirEnts.Lookup(ctx, name)
}

// layerHostOwner is, say, /crfs/layers/gcr.io/foo-proj/
//
// Its children are image names in that project.
type layerHostOwner struct {
	fs    *FS
	inode uint64
	host  string // "gcr.io"
	owner string // "foo-proj" (GCP project, docker hub owner)
}

func (n *layerHostOwner) Attr(ctx context.Context, a *fuse.Attr) error {
	setDirAttr(a)
	a.Inode = n.inode
	return nil
}

func (n *layerHostOwner) ReadDirAll(ctx context.Context) (ents []fuse.Dirent, err error) {
	images, err := n.fs.imagesOfHost(ctx, n.host, n.owner)
	if err != nil {
		return nil, err
	}
	for _, name := range images {
		ents = append(ents, fuse.Dirent{Type: fuse.DT_Dir, Name: name})
	}
	return ents, nil
}

func (n *layerHostOwner) Lookup(ctx context.Context, imageName string) (fspkg.Node, error) {
	// TODO: auth, dockerhub, context
	repo, err := namepkg.NewRepository(n.host + "/" + n.owner + "/" + imageName)
	if err != nil {
		log.Printf("bad name: %v", err)
		return nil, err
	}
	tags, err := google.List(repo)
	if err != nil {
		log.Printf("list: %v", err)
		return nil, err
	}
	m := map[string]string{}
	for k, mi := range tags.Manifests {
		for _, tag := range mi.Tags {
			m[tag] = k
		}
	}
	return &layerHostOwnerImage{
		fs:      n.fs,
		inode:   getOrMakeChildInode(n.inode, imageName),
		host:    n.host,
		owner:   n.owner,
		image:   imageName,
		tags:    tags,
		tagsMap: m,
	}, nil
}

// layerHostOwnerImage is, say, /crfs/layers/gcr.io/foo-proj/ubuntu
//
// Its children are specific version of that image (in the form
// "sha256-7de52a7970a2d0a7d355c76e4f0e02b0e6ebc2841f64040062a27313761cc978",
// with hyphens instead of colons, for portability).
//
// And then also symlinks of tags to said ugly directories.
type layerHostOwnerImage struct {
	fs      *FS
	inode   uint64
	host    string // "gcr.io"
	owner   string // "foo-proj" (GCP project, docker hub owner)
	image   string // "ubuntu"
	tags    *google.Tags
	tagsMap map[string]string // "latest" -> "sha256:fooo"
}

func (n *layerHostOwnerImage) Attr(ctx context.Context, a *fuse.Attr) error {
	setDirAttr(a)
	a.Inode = n.inode
	return nil
}

func uncolon(s string) string { return strings.Replace(s, ":", "-", 1) }
func recolon(s string) string { return strings.Replace(s, "-", ":", 1) }

func (n *layerHostOwnerImage) ReadDirAll(ctx context.Context) (ents []fuse.Dirent, err error) {
	for k := range n.tags.Manifests {
		ents = append(ents, fuse.Dirent{Type: fuse.DT_Dir, Name: uncolon(k)})
	}
	for k := range n.tagsMap {
		ents = append(ents, fuse.Dirent{Type: fuse.DT_Link, Name: k})
	}
	return ents, nil
}

func (n *layerHostOwnerImage) Lookup(ctx context.Context, name string) (fspkg.Node, error) {
	if targ, ok := n.tagsMap[name]; ok {
		return symlinkNode(uncolon(targ)), nil
	}

	withColon := recolon(name)
	if _, ok := n.tags.Manifests[withColon]; ok {
		mf, err := n.fs.getManifest(ctx, n.host, n.owner, n.image, withColon)
		if err != nil {
			log.Printf("getManifest: %v", err)
			return nil, err
		}
		return &layerHostOwnerImageReference{
			fs:    n.fs,
			inode: getOrMakeChildInode(n.inode, name),
			host:  n.host,
			owner: n.owner,
			image: n.image,
			ref:   withColon,
			mf:    mf,
		}, nil
	}
	return nil, fuse.ENOENT
}

// layerHostOwnerImageReference is a specific version of an image:
// /crfs/layers/gcr.io/foo-proj/ubuntu/sha256-7de52a7970a2d0a7d355c76e4f0e02b0e6ebc2841f64040062a27313761cc978
type layerHostOwnerImageReference struct {
	fs    *FS
	inode uint64
	host  string // "gcr.io"
	owner string // "foo-proj" (GCP project, docker hub owner)
	image string // "ubuntu"
	ref   string // "sha256:xxxx" (with colon)
	mf    *manifest
}

func (n *layerHostOwnerImageReference) Attr(ctx context.Context, a *fuse.Attr) error {
	setDirAttr(a)
	a.Valid = 30 * 24 * time.Hour
	a.Inode = n.inode
	return nil
}

func (n *layerHostOwnerImageReference) ReadDirAll(ctx context.Context) (ents []fuse.Dirent, err error) {
	for i, layer := range n.mf.Layers {
		ents = append(ents, fuse.Dirent{Type: fuse.DT_Dir, Name: uncolon(layer.Digest)})
		ents = append(ents, fuse.Dirent{Type: fuse.DT_Link, Name: strconv.Itoa(i)})
	}
	ents = append(ents, fuse.Dirent{Type: fuse.DT_Link, Name: "top"})
	ents = append(ents, fuse.Dirent{Type: fuse.DT_Link, Name: "bottom"})
	ents = append(ents, fuse.Dirent{Type: fuse.DT_File, Name: "config"})
	return
}

func (n *layerHostOwnerImageReference) Lookup(ctx context.Context, name string) (fspkg.Node, error) {
	i, err := strconv.Atoi(name)
	if err == nil && i >= 0 && i < len(n.mf.Layers) {
		return symlinkNode(uncolon(n.mf.Layers[i].Digest)), nil
	}
	if name == "top" {
		return symlinkNode(fmt.Sprint(len(n.mf.Layers) - 1)), nil
	}
	if name == "bottom" {
		return symlinkNode("0"), nil
	}
	if name == "config" {
		conf, err := n.fs.getConfig(ctx, n.host, n.owner, n.image, n.mf.Config.Digest)
		if err != nil {
			log.Printf("getConfig: %v", err)
			return nil, err
		}
		return &staticFile{contents: conf}, nil // TODO: add inode for staticFile
	}

	refColon := recolon(name)
	var layerSize int64
	for _, layer := range n.mf.Layers {
		if layer.Digest == refColon {
			layerSize = layer.Size
			break
		}
	}
	if layerSize == 0 {
		return nil, fuse.ENOENT
	}

	// Probe the tar.gz. URL to see if it serves a redirect.
	//
	// gcr.io serves a redirect for the layer tar.gz blobs, but only on GET, not HEAD.
	// So add a Range header to bound response size. gcr.io ignores the Range request header,
	// but if gcr changes its behavior or we're hitting a different registry implementation,
	// then we don't want to download the full thing.
	urlStr := "https://" + n.host + "/v2/" + n.owner + "/" + n.image + "/blobs/" + refColon
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Range", "bytes=0-1")
	// TODO: auth
	res, err := http.DefaultTransport.RoundTrip(req) // NOT DefaultClient; don't want redirects
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode >= 400 {
		log.Printf("hitting %s: %v", urlStr, res.Status)
		return nil, syscall.EIO
	}
	if redir := res.Header.Get("Location"); redir != "" && res.StatusCode/100 == 3 {
		urlStr = redir
	}

	sr := io.NewSectionReader(&urlReaderAt{url: urlStr}, 0, layerSize)
	r, err := stargz.Open(sr)
	if err != nil {
		log.Printf("error opening remote stargz in %s: %v", urlStr, err)
		return nil, err
	}
	root, ok := r.Lookup("")
	if !ok {
		return nil, errors.New("failed to find root in stargz")
	}
	return &node{
		fs:    n.fs,
		te:    root,
		sr:    r,
		child: make(map[string]*node),
	}, nil
}

type urlReaderAt struct {
	url string
}

func (r *urlReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequest("GET", r.url, nil)
	if err != nil {
		return 0, err
	}
	req = req.WithContext(ctx)
	rangeVal := fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)
	req.Header.Set("Range", rangeVal)
	log.Printf("Fetching %s (%d at %d) of %s ...\n", rangeVal, len(p), off, r.url)
	// TODO: auth
	res, err := http.DefaultTransport.RoundTrip(req) // NOT DefaultClient; don't want redirects
	if err != nil {
		log.Printf("range read of %s: %v", r.url, err)
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusPartialContent {
		log.Printf("range read of %s: %v", r.url, res.Status)
		return 0, err
	}
	return io.ReadFull(res.Body, p)
}

type symlinkNode string // underlying is target

func (s symlinkNode) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeSymlink | 0644
	// TODO: inode
	return nil
}

func (s symlinkNode) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	return string(s), nil
}

// imagesRoot is the contents of /crfs/images/
// Its children are hostnames (such as "gcr.io").
type imagesRoot struct {
	fs    *FS
	inode uint64
}

func (n *imagesRoot) Attr(ctx context.Context, a *fuse.Attr) error {
	setDirAttr(a)
	a.Valid = 30 * 24 * time.Hour
	a.Inode = n.inode
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
	inode    uint64
}

func (f *staticFile) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = 0644
	a.Inode = f.inode
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

	mu sync.Mutex // guards child, below
	// child maps from previously-looked up base names (like "foo.txt") to the *node
	// that was previously returned. This prevents FUSE inode numbers from getting
	// out of sync
	child map[string]*node
}

var (
	_ fspkg.Node               = (*node)(nil)
	_ fspkg.NodeStringLookuper = (*node)(nil)
	_ fspkg.NodeReadlinker     = (*node)(nil)
	_ fspkg.NodeOpener         = (*node)(nil)
	// TODO: implement NodeReleaser and n.f.Close() when n.f is non-nil

	_ fspkg.HandleReadDirAller = (*nodeHandle)(nil)
	_ fspkg.HandleReader       = (*nodeHandle)(nil)

	_ fspkg.HandleReadDirAller = (*rootNode)(nil)
	_ fspkg.NodeStringLookuper = (*rootNode)(nil)
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
	a.Nlink = uint32(n.te.NumLink)
	if a.Nlink == 0 {
		a.Nlink = 1 // zero "NumLink" means one so we map them here.
	}
	if debug {
		log.Printf("attr of %s: %s", n.te.Name, *a)
	}
	return nil
}

// ReadDirAll returns all directory entries in the directory node n.
//
// https://godoc.org/bazil.org/fuse/fs#HandleReadDirAller
func (h *nodeHandle) ReadDirAll(ctx context.Context) (ents []fuse.Dirent, err error) {
	n := h.n
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
	n.mu.Lock()
	defer n.mu.Unlock()
	if c, ok := n.child[name]; ok {
		return c, nil
	}

	e, ok := n.te.LookupChild(name)
	if !ok {
		return nil, fuse.ENOENT
	}

	c := &node{
		fs:    n.fs,
		te:    e,
		sr:    n.sr,
		child: make(map[string]*node),
	}
	n.child[name] = c

	return c, nil
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

func (n *node) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fspkg.Handle, error) {
	h := &nodeHandle{
		n:     n,
		isDir: req.Dir,
	}
	resp.Handle = h.HandleID()
	if !req.Dir {
		var err error
		h.sr, err = n.sr.OpenFile(n.te.Name)
		if err != nil {
			return nil, err
		}
	}
	return h, nil
}

// nodeHandle is a node that's been opened (opendir or for read).
type nodeHandle struct {
	n     *node
	isDir bool
	sr    *io.SectionReader // of file bytes

	mu            sync.Mutex
	lastChunkOff  int64
	lastChunkSize int
	lastChunk     []byte
}

func (h *nodeHandle) HandleID() fuse.HandleID {
	return fuse.HandleID(uintptr(unsafe.Pointer(h)))
}

func (h *nodeHandle) chunkData(offset int64, size int) ([]byte, error) {
	h.mu.Lock()
	if h.lastChunkOff == offset && h.lastChunkSize == size {
		defer h.mu.Unlock()
		if debug {
			log.Printf("cache HIT, chunk off=%d/size=%d", offset, size)
		}
		return h.lastChunk, nil
	}
	h.mu.Unlock()

	if debug {
		log.Printf("reading chunk for offset=%d, size=%d", offset, size)
	}
	buf := make([]byte, size)
	n, err := h.sr.ReadAt(buf, offset)
	if debug {
		log.Printf("... ReadAt = %v, %v", n, err)
	}
	if err == nil {
		h.mu.Lock()
		h.lastChunkOff = offset
		h.lastChunkSize = size
		h.lastChunk = buf
		h.mu.Unlock()
	}
	return buf, err
}

// See https://godoc.org/bazil.org/fuse/fs#HandleReader
func (h *nodeHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	n := h.n

	resp.Data = make([]byte, req.Size)
	nr := 0
	offset := req.Offset
	for nr < req.Size {
		ce, ok := n.sr.ChunkEntryForOffset(n.te.Name, offset+int64(nr))
		if !ok {
			break
		}
		if debug {
			log.Printf("need chunk data for %q at %d (size=%d, for chunk from log %d-%d (%d), phys %d-%d (%d)) ...",
				n.te.Name, req.Offset, req.Size, ce.ChunkOffset, ce.ChunkOffset+ce.ChunkSize, ce.ChunkSize, ce.Offset, ce.NextOffset(), ce.NextOffset()-ce.Offset)
		}
		chunkData, err := h.chunkData(ce.ChunkOffset, int(ce.ChunkSize))
		if err != nil {
			return err
		}
		n := copy(resp.Data[nr:], chunkData[offset+int64(nr)-ce.ChunkOffset:])
		nr += n
	}
	resp.Data = resp.Data[:nr]
	if debug {
		log.Printf("Read response: size=%d @ %d, read %d", req.Size, req.Offset, nr)
	}
	return nil
}
