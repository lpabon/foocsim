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

func simulate(args *Args, cache caches.Caches, metrics *bufio.Writer, seed int64, printstats bool) {

	// Create applications
	apps := make([]*iogenerator.App, args.apps)
	for app := 0; app < len(apps); app++ {
		apps[app] = iogenerator.NewApp(args.numfiles,
			args.maxfileblocks,
			args.randomfilesize,
			args.read_percent,
			seed,
			args.deletion_percent,
			args.pagecacheblocks,
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

	if printstats {
		// Print app stats
		for app := 0; app < len(apps); app++ {
			fmt.Printf("## App %d ##\n", app)
			fmt.Print(apps[app])
		}

		// Print cache stats
		fmt.Println("== Cache ==")
		fmt.Print(cache)
	}
}

func main() {

	// Parse flags
	args := NewArgs()

	// Check parameters
	godbc.Check(args.blocksize > 0, "blocksize must be greater than 0")
	godbc.Check(args.maxfilesize > 0, "maxfilesize must be greater than 0")
	godbc.Check(0 <= (args.read_percent) && (args.read_percent) <= 100, "reads must be between 0 and 100")
	godbc.Check(0 <= (args.deletion_percent) && (args.deletion_percent) <= 100, "deletions must be between 0 and 100")

	// Setup seed for random numbers
	seed := time.Now().UnixNano()

	// Print here Simulation information, also Mean file size and std deviation

	// Create the cache
	var cache caches.Caches
	switch args.cachetype {
	case "simple":
		cache = caches.NewSimpleCache(args.cacheblocks, (args.writethrough))
	case "null":
		cache = caches.NewNullCache()
	case "iocache":
		cache = caches.NewIoCache(args.cacheblocks, (args.writethrough))
	default:
		// buffer cache = cache size * fbcpercent %
		bcsize := uint64(float64(GB*args.cachesize) * (args.bcpercent / 100.0))
		cache = caches.NewIoCacheKvDB(args.cacheblocks,
			bcsize,
			args.writethrough,
			uint32(args.blocksize),
			args.cachetype)
	}

	// Initialize the stats used for delta calculations

	// Start cpu profiling
	f, _ := os.Create("cpuprofile")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	if args.warmup {
		// ------------------- WARMUP --------------------
		// Setup file to write cache metrics
		fp, err := os.Create("cache-warmup.data")
		godbc.Check(err == nil)
		defer fp.Close()
		metrics := bufio.NewWriter(fp)

		fmt.Println("== Warmup ==")
		simulate(args, cache, metrics, seed, args.warmupstats)
		metrics.Flush()
	}

	// ----------------- SIMULATION ------------------
	// Setup file to write cache metrics
	fp, err := os.Create("cache.data")
	godbc.Check(err == nil)
	defer fp.Close()
	metrics := bufio.NewWriter(fp)

	// Begin the simulation
	fmt.Println("== Simulation ==")
	cache.StatsClear()
	start := time.Now()
	simulate(args, cache, metrics, seed, true /* print stats */)
	cache.Close()
	end := time.Now()
	metrics.Flush()

	fmt.Print("\nTotal Time: " + end.Sub(start).String() + "\n")
}
