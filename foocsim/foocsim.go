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
	"bufio"
	"flag"
	"fmt"
	"github.com/lpabon/foocsim/caches"
	"github.com/lpabon/foocsim/zipfworkload"
	"github.com/lpabon/godbc"
	"math/rand"
	"os"
	"runtime/pprof"
	"strconv"
	"time"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
	TB = 1024 * GB
)

type SimFile struct {
	iogen *zipfworkload.ZipfWorkload
	size  uint64
}

// Command line
var fchunksize = flag.Int("chunksize", 64, "\n\tChunk size in KB.")
var fmaxfilesize = flag.Int64("maxfilesize", 8*1024*1024, "\n\tMaximum file size MB. Default 8TB.")
var frandomfilesize = flag.Bool("randomfilesize", false,
	"\n\tCreate files of random size with a maximum of maxfilesize."+
		"\n\tIf false, set the file size exactly to maxfilesize.")
var fcachesize = flag.Uint64("cachesize", 8, "\n\tCache size in GB.")
var fnumfiles = flag.Int("numfiles", 1, "\n\tNumber of files")
var fnumios = flag.Int("ios", 5000000, "\n\tNumber of IOs")
var fdeletion_percent = flag.Int("deletions", 0, "\n\t% of File deletions")
var fread_percent = flag.Int("reads", 65, "\n\t% of Reads")
var fwritethrough = flag.Bool("writethrough", true, "\n\tWritethrough or read miss")
var ffiledistribution_zipf = flag.Bool("zipf_filedistribution", true, "\n\tUse a Zipf or Random distribution")
var fdataperiod = flag.Int("dataperiod", 1000, "\n\tNumber of IOs per data collected")
var fcachetype = flag.String("cachetype", "simple", "\n\tCache type to use."+
	"\n\tCache types with no IO backend:"+
	"\n\t\tsimple, null, iocache."+
	"\n\tCache types with IO backends using iocache frontend:"+
	"\n\t\tleveldb, rocksdb, boltdb, iodb")

func main() {

	var filezipf *zipfworkload.ZipfWorkload

	// Parse flags
	flag.Parse()

	// Check parameters
	godbc.Check(*fchunksize > 0, "chunksize must be greater than 0")
	godbc.Check(*fmaxfilesize > 0, "maxfilesize must be greater than 0")
	godbc.Check(0 <= (*fread_percent) && (*fread_percent) <= 100, "reads must be between 0 and 100")
	godbc.Check(0 <= (*fdeletion_percent) && (*fdeletion_percent) <= 100, "deletions must be between 0 and 100")

	// Setup seed for random numbers
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Config
	chunksize := *fchunksize * KB
	maxfilesize := (int64(MB) * (*fmaxfilesize)) / int64(chunksize) // Up to 1 TB in 256k chunks
	cachesize := (uint64(GB) * (*fcachesize)) / uint64(chunksize)   // 16 GB divided into 256k chunks
	numfiles := *fnumfiles
	filedistribution_zipf := *ffiledistribution_zipf

	// Determine distribution type
	if filedistribution_zipf {
		filezipf = zipfworkload.NewZipfWorkload(uint64(numfiles), 0)
	}

	// Create simulated files
	files := make([]*SimFile, numfiles)
	for file := 0; file < numfiles; file++ {
		files[file] = &SimFile{}
		if *frandomfilesize {
			files[file].size = uint64(r.Int63n(maxfilesize)) + uint64(1) // in case we get 0
		} else {
			files[file].size = uint64(maxfilesize)
		}
		files[file].iogen = zipfworkload.NewZipfWorkload(files[file].size, (*fread_percent))
	}

	// Print here Simulation information, also Mean file size and std deviation

	// Setup file to write cache metrics
	fp, err := os.Create("cache.data")
	godbc.Check(err == nil)
	defer fp.Close()
	metrics := bufio.NewWriter(fp)

	// Create the cache
	var cache caches.Caches
	switch *fcachetype {
	case "simple":
		cache = caches.NewSimpleCache(cachesize, (*fwritethrough))
	case "null":
		cache = caches.NewNullCache()
	case "iocache":
		cache = caches.NewIoCache(cachesize, (*fwritethrough))
	default:
		cache = caches.NewIoCacheKvDB(cachesize, (*fwritethrough), uint32(chunksize), *fcachetype)
	}
	defer cache.Close()

	// Initialize the stats used for delta calculations
	prev_stats := cache.Stats()
	f, _ := os.Create("cpuprofile")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	// Begin the simulation
	start := time.Now()
	for io := 0; io < (*fnumios); io++ {

		// Save metrics
		if (io % (*fdataperiod)) == 0 {
			stats := cache.Stats()
			_, err := metrics.WriteString(fmt.Sprintf("%d,", io) + stats.DumpDelta(prev_stats))
			godbc.Check(err == nil)

			// Now copy the data
			prev_stats = stats
		}

		// Get the file
		var file uint64
		if filedistribution_zipf {
			file, _ = filezipf.ZipfGenerate()
		} else {
			// Random Distribution
			file = uint64(r.Int63n(int64(numfiles)))
		}
		godbc.Check(int(file) <= numfiles, fmt.Sprintf("file = %v", file))

		// Check if we need to delete this file
		if rand.Intn(100) < (*fdeletion_percent) {
			cache.Delete(strconv.FormatUint(file, 10))
			continue
		}

		// Which block on the file
		chunk, isread := files[file].iogen.ZipfGenerate()
		if isread {
			cache.Read(strconv.FormatUint(file, 10), strconv.FormatUint(chunk, 10))
		} else {
			cache.Write(strconv.FormatUint(file, 10), strconv.FormatUint(chunk, 10))
		}
	}
	end := time.Now()
	metrics.Flush()
	fmt.Print(cache)
	fmt.Print("\nTotal Time: " + end.Sub(start).String() + "\n")
}
