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
	iogen              *zipfworkload.ZipfWorkload
	size               uint64
	name               string
	reads, writes, ios uint64
}

func main() {

	// Setup seed for random numbers
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Config
	chunksize := 256 * 1024
	maxfilesize := int64(10*1024*1024*1024) / int64(chunksize) // Up to 10 GB in 256k chunks
	cachesize := uint64(1*1024*1024*1024) / uint64(chunksize)  // 1 GB divided into 256k chunks
	numfiles := 1000
	numios := 5000000
	deletion_chance := 15 // percent
	read_chance := 65     // percent
	writethrough := true

	// Create environment
	filezipf := zipfworkload.NewZipfWorkload(uint64(numfiles), 0)
	files := make([]*SimFile, numfiles)
	for file := 0; file < numfiles; file++ {
		files[file] = &SimFile{}
		files[file].size = uint64(r.Int63n(maxfilesize))
		files[file].iogen = zipfworkload.NewZipfWorkload(files[file].size, read_chance)
		files[file].name = strconv.Itoa(file)
	}

	// Setup file to write cache metrics
	fp, err := os.Create("cache.data")
	godbc.Check(err == nil)
	metrics := bufio.NewWriter(fp)

	// XXX Don't know if this works.. probably does not
	defer metrics.Flush()
	defer fp.Close()

	// Create the cache
	cache := caches.NewCache(cachesize, writethrough)
	cache_prev := cache.Copy()

	for io := 0; io < numios; io++ {

		// Save metrics
		if (io % 10000) == 0 {
			_, err := metrics.WriteString(fmt.Sprintf("%d,", io) + cache.DumpDelta(cache_prev))
			godbc.Check(err == nil)

			// Now copy the data
			*cache_prev = *cache
		}

		// Get the file
		file, _ := filezipf.ZipfGenerate()
		godbc.Check(int(file) <= numfiles, fmt.Sprintf("file = %v", file))

		// Check if we need to delete this file
		if rand.Intn(100) < deletion_chance {
			cache.Delete(strconv.FormatUint(file, 10))
			continue
		}

		// Which block on the file
		chunk, isread := files[file].iogen.ZipfGenerate()

		// Track the number of IOs to this file
		files[file].ios++

		if isread {
			files[file].reads++
			cache.Read(strconv.FormatUint(file, 10), strconv.FormatUint(chunk, 10))
		} else {
			files[file].writes++
			cache.Write(strconv.FormatUint(file, 10), strconv.FormatUint(chunk, 10))
		}
	}

	fmt.Print(cache)
}
