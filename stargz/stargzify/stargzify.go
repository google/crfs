// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The stargzify command converts a remote container image into an equivalent
// image with its layers transformed into stargz files instead of gzipped tar
// files. The image is still a valid container image, but its layers contain
// multiple gzip streams instead of one and have a Table of Contents at the end.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/google/crfs/stargz"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/logs"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/stream"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

var (
	upgrade  = flag.Bool("upgrade", false, "upgrade the image in-place by overwriting the tag")
	flatten  = flag.Bool("flatten", false, "flatten the image's layers into a single layer")
	insecure = flag.Bool("insecure", false, "allow HTTP connections to the registry which has the prefix \"http://\"")

	usage = `usage: %[1]s [-upgrade] [-flatten] input [output]

Converting images:
  # converts "ubuntu" from dockerhub and uploads to your GCR project
  %[1]s ubuntu gcr.io/<your-project>/ubuntu:stargz

  # converts and overwrites :latest
  %[1]s -upgrade gcr.io/<your-project>/ubuntu:latest

  # converts and flattens "ubuntu"
  %[1]s -flatten ubuntu gcr.io/<your-project>/ubuntu:flattened

  # converts "ubuntu" from dockerhub and uploads to your registry using HTTP
  %[1]s -insecure ubuntu http://registry:5000/<path>/ubuntu:stargz

Converting files:
  %[1]s file:/tmp/input.tar.gz file:output.stargz

  # writes to /tmp/input.stargz
  %[1]s file:/tmp/input.tar.gz
`
)

func main() {
	flag.Parse()
	if len(flag.Args()) < 1 {
		printUsage()
	}

	// Set up logs package to get useful messages i.e. progress.
	logs.Warn.SetOutput(os.Stderr)
	logs.Progress.SetOutput(os.Stderr)

	if strings.HasPrefix(flag.Args()[0], "file:") {
		// We'll use "file:" prefix as a signal to convert single files.
		convertFile()
	} else {
		convertImage()
	}
}

func printUsage() {
	log.Fatalf(usage, os.Args[0])
}

func convertFile() {
	var in, out string
	if len(flag.Args()) > 0 {
		in = strings.TrimPrefix(flag.Args()[0], "file:")
	}
	if len(flag.Args()) > 1 {
		out = strings.TrimPrefix(flag.Args()[1], "file:")
	}

	var f, fo *os.File // file in, file out
	var err error
	switch in {
	case "":
		printUsage()
	case "-":
		f = os.Stdin
	default:
		f, err = os.Open(in)
		if err != nil {
			log.Fatal(err)
		}
	}
	defer f.Close()

	if out == "" {
		if in == "-" {
			out = "-"
		} else {
			base := strings.TrimSuffix(in, ".gz")
			base = strings.TrimSuffix(base, ".tgz")
			base = strings.TrimSuffix(base, ".tar")
			out = base + ".stargz"
		}
	}
	if out == "-" {
		fo = os.Stdout
	} else {
		fo, err = os.Create(out)
		if err != nil {
			log.Fatal(err)
		}
	}
	w := stargz.NewWriter(fo)
	if err := w.AppendTar(f); err != nil {
		log.Fatal(err)
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}
	if err := fo.Close(); err != nil {
		log.Fatal(err)
	}
}

func parseFlags(args []string) (string, string) {
	if len(args) < 1 {
		printUsage()
	}

	var src, dst string
	src = args[0]

	if len(args) < 2 {
		if *upgrade {
			dst = src
		} else {
			printUsage()
		}
	} else if len(args) == 2 {
		if *upgrade {
			log.Println("expected one argument with -upgrade")
			printUsage()
		} else {
			dst = args[1]
		}
	} else {
		log.Println("too many arguments")
		printUsage()
	}

	return src, dst
}

func convertImage() {
	src, dst := parseFlags(flag.Args())

	srcRef, err := parseReference(src)
	if err != nil {
		log.Fatal(err)
	}

	// Pull source image.
	srcImg, err := remote.Image(srcRef, remote.WithAuthFromKeychain(authn.DefaultKeychain))
	if err != nil {
		log.Fatal(err)
	}

	// Grab original config, clear the layer info from the config file. We want to
	// preserve the relevant config.
	srcCfg, err := srcImg.ConfigFile()
	if err != nil {
		log.Fatal(err)
	}
	srcCfg.RootFS.DiffIDs = []v1.Hash{}
	srcCfg.History = []v1.History{}

	// Use an empty image with the rest of src's config file as a base.
	img, err := mutate.ConfigFile(empty.Image, srcCfg)
	if err != nil {
		log.Fatal(err)
	}

	layers, err := convertLayers(srcImg)
	if err != nil {
		log.Fatal(err)
	}

	for _, layer := range layers {
		img, err = mutate.Append(img, mutate.Addendum{
			Layer: layer,
			History: v1.History{
				// Leave our mark.
				CreatedBy: fmt.Sprintf("stargzify %s %s", src, dst),
			},
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	// Push the stargzified image to dst.
	dstRef, err := parseReference(dst)
	if err != nil {
		log.Fatal(err)
	}
	dstAuth, err := authn.DefaultKeychain.Resolve(dstRef.Context().Registry)
	if err != nil {
		log.Fatal(err)
	}

	if err := remote.Write(dstRef, img, remote.WithAuth(dstAuth), remote.WithTransport(http.DefaultTransport)); err != nil {
		log.Fatal(err)
	}
}

func convertLayers(img v1.Image) ([]v1.Layer, error) {
	if *flatten {
		r := mutate.Extract(img)
		return []v1.Layer{newLayer(r)}, nil
	}

	layers, err := img.Layers()
	if err != nil {
		return nil, err
	}

	converted := []v1.Layer{}
	for _, layer := range layers {
		r, err := layer.Uncompressed()
		if err != nil {
			return nil, err
		}
		converted = append(converted, newLayer(r))
	}

	return converted, nil
}

type layer struct {
	rc     io.ReadCloser
	d      *digester
	diff   *v1.Hash
	digest *v1.Hash
}

// parseReference is like go-containerregistry/pkg/name.ParseReference but additionally
// supports the reference starting with "http://" to mean insecure.
func parseReference(ref string) (name.Reference, error) {
	var opts []name.Option
	if strings.HasPrefix(ref, "http://") {
		if !*insecure {
			return nil, fmt.Errorf("-insecure flag required when connecting using HTTP to %q", ref)
		}
		ref = strings.TrimPrefix(ref, "http://")
		opts = append(opts, name.Insecure)
	}
	return name.ParseReference(ref, opts...)
}

// newLayer converts the given io.ReadCloser to a stargz layer.
func newLayer(rc io.ReadCloser) v1.Layer {
	return &layer{
		rc: rc,
		d: &digester{
			h: sha256.New(),
		},
	}
}

func (l *layer) Digest() (v1.Hash, error) {
	if l.digest == nil {
		return v1.Hash{}, stream.ErrNotComputed
	}
	return *l.digest, nil
}

func (l *layer) Size() (int64, error) {
	if l.digest == nil {
		return -1, stream.ErrNotComputed
	}
	return l.d.n, nil
}

func (l *layer) DiffID() (v1.Hash, error) {
	if l.diff == nil {
		return v1.Hash{}, stream.ErrNotComputed
	}
	return *l.diff, nil
}

func (l *layer) MediaType() (types.MediaType, error) {
	// TODO: We might want to set our own media type to indicate stargz layers,
	// but that has the potential to break registry compatibility.
	return types.DockerLayer, nil
}

func (l *layer) Compressed() (io.ReadCloser, error) {
	pr, pw := io.Pipe()

	// Convert input blob to stargz while computing diffid, digest, and size.
	go func() {
		w := stargz.NewWriter(io.MultiWriter(pw, l.d))
		if err := w.AppendTar(l.rc); err != nil {
			pw.CloseWithError(err)
			return
		}
		if err := w.Close(); err != nil {
			pw.CloseWithError(err)
			return
		}
		diffid, err := v1.NewHash(w.DiffID())
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		l.diff = &diffid
		l.digest = &v1.Hash{
			Algorithm: "sha256",
			Hex:       hex.EncodeToString(l.d.h.Sum(nil)),
		}
		pw.Close()
	}()

	return ioutil.NopCloser(pr), nil
}

func (l *layer) Uncompressed() (io.ReadCloser, error) {
	return l.rc, nil
}

// digester tracks the sha256 and length of what is written to it.
type digester struct {
	h hash.Hash
	n int64
}

func (d *digester) Write(b []byte) (int, error) {
	n, err := d.h.Write(b)
	d.n += int64(n)
	return n, err
}
