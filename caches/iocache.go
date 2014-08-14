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
)

/* -------------------------------------------------------- */
type IoCacheBlockInfo struct {
	key  string
	mru  bool
	used bool
}

type IoCacheBlocks struct {
	cacheblocks []IoCacheBlockInfo
	size        uint64
	index       uint64
}

func NewIoCacheBlocks(cachesize uint64) *IoCacheBlocks {
	icb := &IoCacheBlocks{}
	icb.cacheblocks = make([]IoCacheBlockInfo, cachesize)
	icb.size = cachesize
	return icb
}

func (c *IoCacheBlocks) Insert(key string) (evictkey string, newindex uint64, err error) {
	for {
		for ; c.index < c.size; c.index++ {
			if c.cacheblocks[c.index].mru {
				c.cacheblocks[c.index].mru = false
			} else {
				if c.cacheblocks[c.index].used {
					evictkey = c.cacheblocks[c.index].key
				} else {
					evictkey = ""
				}
				newindex = c.index
				err = nil
				c.cacheblocks[c.index].key = key
				c.cacheblocks[c.index].mru = true
				c.cacheblocks[c.index].used = true
				c.index++
				return
			}
		}
		c.index = 0
	}
}

func (c *IoCacheBlocks) Using(index uint64) {
	c.cacheblocks[index].mru = true
}

func (c *IoCacheBlocks) Free(index uint64) {
	c.cacheblocks[index].mru = false
	c.cacheblocks[index].used = false
	c.cacheblocks[index].key = ""
}

/* -------------------------------------------------------- */

type IoCache struct {
	stats        *CacheStats
	cachemap     map[string]uint64
	cachesize    uint64
	writethrough bool
	cacheblocks  *IoCacheBlocks
}

func NewIoCache(cachesize uint64, writethrough bool) *IoCache {
	godbc.Require(cachesize > 0)

	cache := &IoCache{}
	cache.stats = NewCacheStats()
	cache.cacheblocks = NewIoCacheBlocks(cachesize)
	cache.cachemap = make(map[string]uint64)
	cache.cachesize = cachesize
	cache.writethrough = writethrough

	godbc.Ensure(cache.cachesize > 0)

	return cache
}

func (s *IoCache) Close() {

}

func (c *IoCache) Invalidate(key string) {
	if val, ok := c.cachemap[key]; ok {
		c.stats.writehits++
		c.stats.invalidations++
		c.cacheblocks.Free(val)
		delete(c.cachemap, key)
	}
}

func (c *IoCache) Insert(key string) {
	c.stats.insertions++

	evictkey, index, _ := c.cacheblocks.Insert(key)

	// Check for evictions
	if evictkey != "" {
		c.stats.evictions++
		delete(c.cachemap, evictkey)
	}

	// Insert new key in cache map
	c.cachemap[key] = index
}

func (c *IoCache) Write(obj string, chunk string) {
	c.stats.writes++

	key := obj + chunk

	// Invalidate
	c.Invalidate(key)

	// We would do back end IO here

	// Insert
	if c.writethrough {
		c.Insert(key)
	}
}

func (c *IoCache) Read(obj, chunk string) {
	c.stats.reads++

	key := obj + chunk

	if val, ok := c.cachemap[key]; ok {
		// Read Hit
		c.stats.readhits++

		// Clock Algorithm: Set that we looked
		// at it
		c.cacheblocks.Using(val)
	} else {
		// Read miss
		// We would do IO here
		c.Insert(key)
	}
}

func (c *IoCache) Delete(obj string) {
	// Not supported
}

func (c *IoCache) String() string {
	return fmt.Sprintf(
		"== Cache Information ==\n"+
			"Cache Utilization: %.2f %%\n",
		float64(len(c.cachemap))/float64(c.cachesize)*100.0) +
		c.stats.String()
}

func (c *IoCache) Stats() *CacheStats {
	return c.stats.Copy()
}
