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
	"github.com/lpabon/foocsim/kvdb"
	"github.com/lpabon/godbc"
)

var buf []byte

func init() {
	buf = make([]byte, 4096)
}

type IoCacheKvDB struct {
	stats        CacheStats
	cachemap     map[string]uint64
	cachesize    uint64
	writethrough bool
	cacheblocks  *IoCacheBlocks
	db           kvdb.Kvdb
}

func NewIoCacheKvDB(cachesize uint64, writethrough bool, dbtype string) *IoCacheKvDB {

	godbc.Require(cachesize > 0)

	cache := &IoCacheKvDB{}
	cache.cacheblocks = NewIoCacheBlocks(cachesize)
	cache.cachemap = make(map[string]uint64)
	cache.cachesize = cachesize
	cache.writethrough = writethrough

	switch dbtype {
	case "leveldb":
		cache.db = kvdb.NewKVLevelDB("cache.ioleveldb")
	case "rocksdb":
		cache.db = kvdb.NewKVRocksDB("cache.iorocksdb")
	case "boltdb":
		cache.db = kvdb.NewKVBoltDB("cache.ioboltdb")
	default:
		godbc.Check(false, "Unkonwn db type")
	}
	godbc.Check(cache.db != nil)
	godbc.Ensure(cache.cachesize > 0)

	return cache
}

func (c *IoCacheKvDB) Close() {
	c.db.Close()
}

func (c *IoCacheKvDB) Invalidate(key string) {
	if val, ok := c.cachemap[key]; ok {
		c.stats.writehits++
		c.stats.invalidations++
		delete(c.cachemap, key)
		c.cacheblocks.Free(val)
		c.db.Delete([]byte(key))
	}
}

func (c *IoCacheKvDB) Insert(key string) {
	c.stats.insertions++

	evictkey, index, _ := c.cacheblocks.Insert(key)

	// Check for evictions
	if evictkey != "" {
		c.stats.evictions++
		delete(c.cachemap, evictkey)
		c.db.Delete([]byte(evictkey))
	}

	// Insert new key in cache map
	c.cachemap[key] = index
	c.db.Put([]byte(key), buf)
}

func (c *IoCacheKvDB) Write(obj string, chunk string) {
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

func (c *IoCacheKvDB) Read(obj, chunk string) {
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

func (c *IoCacheKvDB) Delete(obj string) {
	// Not supported
}

func (c *IoCacheKvDB) String() string {
	return fmt.Sprintf(
		"== Cache Information ==\n"+
			"Cache Utilization: %.2f %%\n",
		float64(len(c.cachemap))/float64(c.cachesize)*100.0) +
		c.stats.String()
}

func (c *IoCacheKvDB) Stats() *CacheStats {
	return c.stats.Copy()
}