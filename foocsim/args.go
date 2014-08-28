//
// Copyright (c) 2014 The foocsim Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
)

// Command line
type Args struct {
	blocksize, numfiles, apps    int
	blocksizekb                  int
	numios, read_percent         int
	dataperiod, deletion_percent int
	pagecachesize, cachesize     int
	randomfilesize, writethrough bool
	cachetype                    string
	maxfilesize                  uint64
	bcpercent                    float64
	pagecacheblocks, cacheblocks uint64
	maxfileblocks                uint64
	warmupstats, warmup          bool
}

// Command line arguments variable
var args Args

func init() {
	flag.IntVar(&args.blocksizekb, "blocksize", 64, "\n\tBlock size in KB.")
	flag.Uint64Var(&args.maxfilesize, "maxfilesize", 8*1024*1024, "\n\tMaximum file size MB. Default 8TB.")
	flag.BoolVar(&args.randomfilesize, "randomfilesize", false,
		"\n\tCreate files of random size with a maximum of maxfilesize."+
			"\n\tIf false, set the file size exactly to maxfilesize.")
	flag.IntVar(&args.cachesize, "cachesize", 8, "\n\tCache size in GB.")
	flag.Float64Var(&args.bcpercent, "bcpercent", 0.1, "\n\tBuffer Cache size as a percentage of the cache size")
	flag.IntVar(&args.numfiles, "numfiles", 1, "\n\tNumber of files")
	flag.IntVar(&args.numios, "ios", 5000000, "\n\tNumber of IOs for each client")
	flag.IntVar(&args.deletion_percent, "deletions", 0, "\n\t% of File deletions")
	flag.IntVar(&args.read_percent, "reads", 65, "\n\t% of Reads")
	flag.BoolVar(&args.writethrough, "writethrough", true, "\n\tWritethrough or read miss")
	flag.IntVar(&args.dataperiod, "dataperiod", 1000, "\n\tNumber of IOs per data collected")
	flag.StringVar(&args.cachetype, "cachetype", "simple", "\n\tCache type to use."+
		"\n\tCache types with no IO backend:"+
		"\n\t\tsimple, null, iocache."+
		"\n\tCache types with IO backends using iocache frontend:"+
		"\n\t\tleveldb, rocksdb, boltdb, iodb")
	flag.IntVar(&args.pagecachesize, "pagecachesize", 0, "\n\tSize of VM page cache above the IO cache in MB")
	flag.IntVar(&args.apps, "clients", 1, "\n\tNumber of clients")
	flag.BoolVar(&args.warmupstats, "warmupstats", false, "\n\tPrint stats after warmup stage")
	flag.BoolVar(&args.warmup, "warmup", true, "\n\tWarmup cache before running simulation")
}

func NewArgs() *Args {
	if !flag.Parsed() {
		flag.Parse()
		args.calc()
	}
	return &args
}

func (a *Args) calc() {

	// Config
	a.blocksize = a.blocksizekb * KB
	a.cacheblocks = uint64(GB*a.cachesize) / uint64(a.blocksize)
	a.maxfileblocks = a.maxfilesize * uint64(MB) / uint64(a.blocksize*KB)
	a.pagecacheblocks = uint64(a.pagecachesize * MB / (a.blocksize * KB))

}
