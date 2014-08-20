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
var fbcpercent = flag.Float64("bcpercent", 0.1, "\n\tBuffer Cache size as a percentage of the cache size")
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
var fpagecachesize = flag.Int("pagecachesize", 0, "\n\tSize of VM page cache above the IO cache in MB")

func simulate(cache, pc caches.Caches, metrics *bufio.Writer, files []*SimFile) {
	prev_stats := cache.Stats()
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
		file, _ := filezipf.ZipfGenerate()
		godbc.Check(int(file) <= numfiles, fmt.Sprintf("file = %v", file))

		// Check if we need to delete this file
		if rand.Intn(100) < (*fdeletion_percent) {
			cache.Delete(strconv.FormatUint(file, 10))
			continue
		}

		// Which block on the file
		chunk, isread := files[file].iogen.ZipfGenerate()
		str_file := strconv.FormatUint(file, 10)
		str_chunk := strconv.FormatUint(chunk, 10)
		if isread {
			if !pc.Read(str_file, str_chunk) {
				cache.Read(str_file, str_chunk)
			}
		} else {
			pc.Write(str_file, str_chunk)
			cache.Write(str_file, str_chunk)
		}
	}
}

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
	maxfilesize := (int64(MB) * (*fmaxfilesize)) / int64(chunksize)
	cachesize := (uint64(GB) * (*fcachesize)) / uint64(chunksize)
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
		// buffer cache = cache size * fbcpercent %
		bcsize := uint64(float64((uint64(GB) * (*fcachesize))) * (*fbcpercent / 100.0))
		cache = caches.NewIoCacheKvDB(cachesize, bcsize, (*fwritethrough), uint32(chunksize), *fcachetype)
	}

	// Initialize the stats used for delta calculations

	f, _ := os.Create("cpuprofile")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	// Add a pagecache
	if *fpagecachesize != 0 {
		pc = caches.NewIoCache(uint64((*fpagecachesize)*MB/(*fchunksize)), (*fwritethrough))
	} else {
		pc = caches.NewNullCache()
	}

	fmt.Println("== Warmup ==")
	simulate(cache, pc, metrics, files)

	// Begin the simulation
	start := time.Now()
	simulate(cache, pc, metrics, files)
	cache.Close()
	end := time.Now()
	metrics.Flush()

	if *fpagecachesize != 0 {
		fmt.Println("== Page Cache ==")
		fmt.Print(pc)
	}

	fmt.Println("== Cache ==")
	fmt.Print(cache)
	fmt.Print("\nTotal Time: " + end.Sub(start).String() + "\n")
}
