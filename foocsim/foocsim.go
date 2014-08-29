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
	"github.com/lpabon/foocsim/args"
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

func simulate(config *args.Args, cache caches.Caches, metrics *bufio.Writer, seed int64, printstats bool) {

	// Create applications
	apps := make([]*iogenerator.App, config.Apps())
	for app := 0; app < len(apps); app++ {
		apps[app] = iogenerator.NewApp(config, seed, cache)
	}

	// Initialize the delta stats
	prev_stats := cache.Stats()
	for io := 0; io < config.Ios(); io++ {

		// Save metrics
		if (io % (config.DataPeriod())) == 0 {
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
	config := args.NewArgs()

	// Setup seed for random numbers
	seed := time.Now().UnixNano()

	// Print here Simulation information, also Mean file size and std deviation

	// Create the cache
	var cache caches.Caches
	switch config.CacheType() {
	case "simple":
		cache = caches.NewSimpleCache(config.CacheBlocks(), config.Writethrough())
	case "null":
		cache = caches.NewNullCache()
	case "iocache":
		cache = caches.NewIoCache(config.CacheBlocks(), config.Writethrough())
	default:
		// buffer cache = cache size * fbcpercent %
		cache = caches.NewIoCacheKvDB(config.CacheBlocks(),
			config.BufferCacheSize(),
			config.Writethrough(),
			config.Blocksize(),
			config.CacheType())
	}

	// Initialize the stats used for delta calculations

	// Start cpu profiling
	f, _ := os.Create("cpuprofile")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	if config.UseWarmup() {
		// ------------------- WARMUP --------------------
		// Setup file to write cache metrics
		fp, err := os.Create("cache-warmup.data")
		godbc.Check(err == nil)
		defer fp.Close()
		metrics := bufio.NewWriter(fp)

		fmt.Println("== Warmup ==")
		simulate(config, cache, metrics, seed, config.ShowWarmupStats())
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
	simulate(config, cache, metrics, seed, true /* print stats */)
	cache.Close()
	end := time.Now()
	metrics.Flush()

	fmt.Print("\nTotal Time: " + end.Sub(start).String() + "\n")
}
