package main

import (
	"bitbucket.org/lpabon/filecc/caches"
	"bitbucket.org/lpabon/filecc/zipfworkload"
	"bufio"
	"flag"
	"fmt"
	"github.com/lpabon/godbc"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const (
	KB = 1024
	MB = 1024 * 1024
	GB = 1024 * 1024 * 1024
	TB = 1024 * 1024 * 1024 * 1024
)

type SimFile struct {
	iogen *zipfworkload.ZipfWorkload
	size  uint64
}

// Command line
var fchunksize = flag.Int("chunksize", 256, "Chunk size in KB. Default 256 KB")
var fmaxfilesize = flag.Int64("maxfilesize", 1*1024*1024, "Maximum file size MB. Default 1 TB")
var fcachesize = flag.Uint64("cachesize", 64, "Cache size in GB. Default 8 GB")
var fnumfiles = flag.Int("numfiles", 100000, "Number of files")
var fnumios = flag.Int("ios", 5000000, "Number of IOs")
var fdeletion_percent = flag.Int("deletions", 15, "% of File deletions")
var fread_percent = flag.Int("reads", 65, "% of Reads")
var fwritethrough = flag.Bool("writethrough", true, "Writethrough or read miss")
var ffiledistribution_zipf = flag.Bool("zipf_filedistribution", true, "Use a Zipf or Random distribution")

func main() {

	var filezipf *zipfworkload.ZipfWorkload

	// Parse flags
	flag.Parse()

	// Setup seed for random numbers
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Config
	chunksize := *fchunksize * KB
	maxfilesize := (int64(MB) * (*fmaxfilesize)) / int64(chunksize) // Up to 1 TB in 256k chunks
	cachesize := (uint64(GB) * (*fcachesize)) / uint64(chunksize)   // 16 GB divided into 256k chunks
	numfiles := *fnumfiles
	numios := *fnumios
	deletion_chance := *fdeletion_percent // percent
	read_chance := *fread_percent         // percent
	writethrough := *fwritethrough
	filedistribution_zipf := *ffiledistribution_zipf

	// Determine distribution type
	if filedistribution_zipf {
		filezipf = zipfworkload.NewZipfWorkload(uint64(numfiles), 0)
	}

	// Create simulated files
	files := make([]*SimFile, numfiles)
	for file := 0; file < numfiles; file++ {
		files[file] = &SimFile{}
		files[file].size = uint64(r.Int63n(maxfilesize)) + uint64(1) // in case we get 0
		files[file].iogen = zipfworkload.NewZipfWorkload(files[file].size, read_chance)
	}

	// Print here Simulation information, also Mean file size and std deviation

	// Setup file to write cache metrics
	fp, err := os.Create("cache.data")
	godbc.Check(err == nil)
	defer fp.Close()
	metrics := bufio.NewWriter(fp)

	// Create the cache
	cache := caches.NewCache(cachesize, writethrough)
	cache_prev := cache.Copy()

	for io := 0; io < numios; io++ {

		// Save metrics
		if (io % 100) == 0 {
			_, err := metrics.WriteString(fmt.Sprintf("%d,", io) + cache.DumpDelta(cache_prev))
			godbc.Check(err == nil)

			// Now copy the data
			*cache_prev = *cache
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
		if rand.Intn(100) < deletion_chance {
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
	metrics.Flush()
	fmt.Print(cache)
}
