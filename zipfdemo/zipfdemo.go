package main

import (
	"bitbucket.org/lpabon/filecc/caches"
	"bitbucket.org/lpabon/filecc/zipfworkload"
	"bufio"
	"fmt"
	"github.com/lpabon/godbc"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type SimFile struct {
	iogen *zipfworkload.ZipfWorkload
	size  uint64
}

func main() {

	var filezipf *zipfworkload.ZipfWorkload

	// Setup seed for random numbers
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Config
	chunksize := 256 * 1024
	maxfilesize := int64(1*1024*1024*1024*1024) / int64(chunksize) // Up to 1 TB in 256k chunks
	cachesize := uint64(64*1024*1024*1024) / uint64(chunksize)     // 16 GB divided into 256k chunks
	numfiles := 100000
	numios := 5000000
	deletion_chance := 15 // percent
	read_chance := 65     // percent
	writethrough := true
	filedistribution_zipf := true

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
