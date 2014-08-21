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
	"github.com/lpabon/foocsim/iogenerator"
	"github.com/lpabon/godbc"
	"os"
	"runtime/pprof"
	"time"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
	TB = 1024 * GB
)

// Command line
type Args struct {
	blocksize, numfiles, apps    int
	numios, read_percent         int
	dataperiod, deletion_percent int
	pagecachesize, cachesize     int
	randomfilesize, writethrough bool
	cachetype                    string
	maxfilesize                  uint64
	bcpercent                    float64
}

var args Args

func init() {
	flag.IntVar(&args.blocksize, "blocksize", 64, "\n\tBlock size in KB.")
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
}

func simulate(cache caches.Caches, metrics *bufio.Writer, seed int64) {

	// Create applications
	apps := make([]*iogenerator.App, args.apps)
	for app := 0; app < len(apps); app++ {
		apps[app] = iogenerator.NewApp(args.numfiles,
			args.maxfilesize*uint64(MB)/uint64(args.blocksize*KB),
			args.randomfilesize,
			args.read_percent,
			seed,
			args.deletion_percent,
			uint64(args.pagecachesize*MB/(args.blocksize*KB)),
			cache)
	}

	// Initialize the delta stats
	prev_stats := cache.Stats()
	for io := 0; io < args.numios; io++ {

		// Save metrics
		if (io % (args.dataperiod)) == 0 {
			stats := cache.Stats()
			_, err := metrics.WriteString(fmt.Sprintf("%d,", io) + stats.DumpDelta(prev_stats))
			godbc.Check(err == nil)

			// Now copy the data
			prev_stats = stats
		}

		// Generate I/O for each app
		for app := 0; app < len(apps); app++ {
			apps[app].Gen()
		}

	}

	// Generate I/O for each app
	for app := 0; app < len(apps); app++ {
		fmt.Printf("## App %d ##\n", app)
		fmt.Print(apps[app])
	}

	fmt.Println("== Cache ==")
	fmt.Print(cache)
}

func main() {

	// Parse flags
	flag.Parse()

	// Check parameters
	godbc.Check(args.blocksize > 0, "blocksize must be greater than 0")
	godbc.Check(args.maxfilesize > 0, "maxfilesize must be greater than 0")
	godbc.Check(0 <= (args.read_percent) && (args.read_percent) <= 100, "reads must be between 0 and 100")
	godbc.Check(0 <= (args.deletion_percent) && (args.deletion_percent) <= 100, "deletions must be between 0 and 100")

	// Setup seed for random numbers
	seed := time.Now().UnixNano()

	// Config
	blocksize := args.blocksize * KB
	cachesize := uint64(GB*args.cachesize) / uint64(blocksize)

	// Print here Simulation information, also Mean file size and std deviation

	// Create the cache
	var cache caches.Caches
	switch args.cachetype {
	case "simple":
		cache = caches.NewSimpleCache(cachesize, (args.writethrough))
	case "null":
		cache = caches.NewNullCache()
	case "iocache":
		cache = caches.NewIoCache(cachesize, (args.writethrough))
	default:
		// buffer cache = cache size * fbcpercent %
		bcsize := uint64(float64(GB*args.cachesize) * (args.bcpercent / 100.0))
		cache = caches.NewIoCacheKvDB(cachesize, bcsize, (args.writethrough), uint32(blocksize), args.cachetype)
	}

	// Initialize the stats used for delta calculations

	// Start cpu profiling
	f, _ := os.Create("cpuprofile")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	// ------------------- WARMUP --------------------
	// Setup file to write cache metrics
	fp, err := os.Create("cache-warmup.data")
	godbc.Check(err == nil)
	metrics := bufio.NewWriter(fp)

	fmt.Println("== Warmup ==")
	simulate(cache, metrics, seed)
	metrics.Flush()
	fp.Close()

	// ----------------- SIMULATION ------------------
	// Setup file to write cache metrics
	fp, err = os.Create("cache.data")
	godbc.Check(err == nil)
	defer fp.Close()
	metrics = bufio.NewWriter(fp)

	// Begin the simulation
	cache.StatsClear()
	start := time.Now()
	simulate(cache, metrics, seed)
	cache.Close()
	end := time.Now()
	metrics.Flush()

	fmt.Print("\nTotal Time: " + end.Sub(start).String() + "\n")
}
