// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The stargzify command converts a tarball into a seekable stargz
// tarball. The output is still a valid tarball, but has new gzip
// streams throughout the file and and an Table of Contents (TOC)
// index at the end pointing into those streams.
package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"golang.org/x/build/crfs/stargz"
)

var (
	in  = flag.String("in", "", "input file in tar or tar.gz format. Use \"-\" for stdin.")
	out = flag.String("out", "", "output file. If empty, it's the input base + \".stargz\", or stdout if the input is stdin. Use \"-\" for stdout.")
)

func main() {
	flag.Parse()
	var f, fo *os.File // file in, file out
	var err error
	switch *in {
	case "":
		log.Fatal("missing required --in flag")
	case "-":
		f = os.Stdin
	default:
		f, err = os.Open(*in)
		if err != nil {
			log.Fatal(err)
		}
	}
	defer f.Close()

	if *out == "" {
		if *in == "-" {
			*out = "-"
		} else {
			base := strings.TrimSuffix(*in, ".gz")
			base = strings.TrimSuffix(base, ".tgz")
			base = strings.TrimSuffix(base, ".tar")
			*out = base + ".stargz"
		}
	}
	if *out == "-" {
		fo = os.Stdout
	} else {
		fo, err = os.Create(*out)
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
