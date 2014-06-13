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

package caches

import (
	"fmt"
	"github.com/lpabon/godbc"
	"strconv"
)

type Cache struct {
	cacheobjids              map[string]string
	cachemap                 map[string]int
	cachesize                uint64
	readhits, writehits      int
	reads, writes            int
	deletions, deletionhits  int
	evictions, invalidations int
	insertions               int
	writethrough             bool
}

func cacheCreateObjKey(obj string) func() string {
	counter := 0
	return func() string {
		godbc.Require(counter >= 0)

		counter += 1
		return strconv.Itoa(counter)
	}
}

func NewCache(cachesize uint64, writethrough bool) *Cache {

	godbc.Require(cachesize > 0)

	cache := &Cache{}
	cache.cachesize = cachesize
	cache.writethrough = writethrough
	cache.cacheobjids = make(map[string]string)
	cache.cachemap = make(map[string]int)

	godbc.Ensure(cache.cacheobjids != nil)
	godbc.Ensure(cache.cachemap != nil)
	godbc.Ensure(cache.cachesize > 0)

	return cache
}

func (c *Cache) getObjKey(obj string) string {
	if val, ok := c.cacheobjids[obj]; ok {
		return val
	} else {
		newid := cacheCreateObjKey(obj)
		c.cacheobjids[obj] = newid()
		return c.cacheobjids[obj]
	}
}

func (c *Cache) Copy() *Cache {
	cachecopy := &Cache{}
	*cachecopy = *c

	return cachecopy
}

func (c *Cache) Invalidate(chunkkey string) {
	if _, ok := c.cachemap[chunkkey]; ok {
		c.writehits++
		c.invalidations++
		delete(c.cachemap, chunkkey)
	}
}

func (c *Cache) Evict() {
	c.evictions++

	// BIG ASSUMPTION! I have no idea
	// if Go keeps track of the iteration
	// through a map
	for {
		for key, val := range c.cachemap {
			if val == 1 {

				// Clock Algorithm: We looked at it
				// and set to zero for next time
				c.cachemap[key] = 0
			} else {
				delete(c.cachemap, key)
				return
			}
		}
	}
}

func (c *Cache) Insert(chunkkey string) {
	c.insertions++

	if uint64(len(c.cachemap)) >= c.cachesize {
		c.Evict()
	}

	c.cachemap[chunkkey] = 1
}

func (c *Cache) Write(obj string, chunk string) {
	c.writes++

	key := c.getObjKey(obj) + chunk

	// Invalidate
	c.Invalidate(key)

	// We would do back end IO here

	// Insert
	if c.writethrough {
		c.Insert(key)
	}
}

func (c *Cache) Read(obj, chunk string) {
	c.reads++

	key := c.getObjKey(obj) + chunk

	if _, ok := c.cachemap[key]; ok {
		// Read Hit
		c.readhits++

		// Clock Algorithm: Set that we looked
		// at it
		c.cachemap[key] = 1
	} else {
		// Read miss
		// We would do IO here
		c.Insert(key)
	}
}

func (c *Cache) Delete(obj string) {
	c.deletions++

	if _, ok := c.cacheobjids[obj]; ok {
		c.deletionhits++
		delete(c.cacheobjids, obj)
	}
}

func (c *Cache) ReadHitRate() float64 {
	if c.reads == 0 {
		return 0.0
	} else {
		return float64(c.readhits) / float64(c.reads)
	}
}

func (c *Cache) WriteHitRate() float64 {
	if c.writes == 0 {
		return 0.0
	} else {
		return float64(c.writehits) / float64(c.writes)
	}

}

func (c *Cache) ReadHitRateDelta(prev *Cache) float64 {
	reads := c.reads - prev.reads
	readhits := c.readhits - prev.readhits
	if reads == 0 {
		return 0.0
	} else {
		return float64(readhits) / float64(reads)
	}
}

func (c *Cache) WriteHitRateDelta(prev *Cache) float64 {
	writes := c.writes - prev.writes
	writehits := c.writehits - prev.writehits
	if writes == 0 {
		return 0.0
	} else {
		return float64(writehits) / float64(writes)
	}

}

func (c *Cache) String() string {
	return fmt.Sprintf(
		"== Cache Information ==\n"+
			"Cache Utilization: %v\n"+
			"Read Hit Rate: %v\n"+
			"Write Hit Rate: %v\n"+
			"Read hits: %d\n"+
			"Write hits: %d\n"+
			"Delete hits: %d\n"+
			"Reads: %d\n"+
			"Writes: %d\n"+
			"Deletions: %d\n"+
			"Insertions: %d\n"+
			"Evictions: %d\n"+
			"Invalidations: %d\n",
		float64(len(c.cachemap))/float64(c.cachesize),
		c.ReadHitRate(),
		c.WriteHitRate(),
		c.readhits,
		c.writehits,
		c.deletionhits,
		c.reads,
		c.writes,
		c.deletions,
		c.insertions,
		c.evictions,
		c.invalidations)
}

func (c *Cache) Dump() string {
	return fmt.Sprintf(
		"%v,"+ // Read Hit Rate 1
			"%v,"+ // Write Hit Rate 2
			"%d,"+ // Read Hits 3
			"%d,"+ // Write Hits 4
			"%d,"+ // Deletion Hits 5
			"%d,"+ // Reads 6
			"%d,"+ // Writes 7
			"%d,"+ // Deletions 8
			"%d,"+ // Insertions 9
			"%d,"+ // Evictions 10
			"%d\n", // Invalidations 11
		c.ReadHitRate(),
		c.WriteHitRate(),
		c.readhits,
		c.writehits,
		c.deletionhits,
		c.reads,
		c.writes,
		c.deletions,
		c.insertions,
		c.evictions,
		c.invalidations)

}

func (c *Cache) DumpDelta(prev *Cache) string {
	return fmt.Sprintf(
		"%v,"+ // Read Hit Rate 1
			"%v,"+ // Write Hit Rate 2
			"%d,"+ // Read Hits 3
			"%d,"+ // Write Hits 4
			"%d,"+ // Deletion Hits 5
			"%d,"+ // Reads 6
			"%d,"+ // Writes 7
			"%d,"+ // Deletions 8
			"%d,"+ // Insertions 9
			"%d,"+ // Evictions 10
			"%d\n", // Invalidations 11
		c.ReadHitRateDelta(prev),
		c.WriteHitRateDelta(prev),
		c.readhits-prev.readhits,
		c.writehits-prev.writehits,
		c.deletionhits-prev.deletionhits,
		c.reads-prev.reads,
		c.writes-prev.writes,
		c.deletions-prev.deletions,
		c.insertions-prev.insertions,
		c.evictions-prev.evictions,
		c.invalidations-prev.invalidations)

}
