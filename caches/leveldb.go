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
	"github.com/jmhodges/levigo"
	"github.com/lpabon/godbc"
	"os"
)

var leveldbvalset []byte
var leveldbunvalset []byte

func init() {
	leveldbvalset = make([]byte, 64)
	leveldbunvalset = make([]byte, 64)
	leveldbvalset[0] = 1
}

type LevelDBCache struct {
	stats        CacheStats
	db           *levigo.DB
	ro           *levigo.ReadOptions
	wo           *levigo.WriteOptions
	bloomf       *levigo.FilterPolicy
	cachesize    uint64
	keyn         uint64
	writethrough bool
}

func NewLevelDBCache(cachesize uint64, writethrough bool) *LevelDBCache {

	var err error

	godbc.Require(cachesize > 0)

	db := &LevelDBCache{}
	db.writethrough = writethrough
	db.cachesize = cachesize

	os.RemoveAll("cache.leveldb")

	// Set bloom filter
	db.bloomf = levigo.NewBloomFilter(10)

	// Set Options
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)
	opts.SetFilterPolicy(db.bloomf)

	db.db, err = levigo.Open("cache.leveldb", opts)
	godbc.Check(err == nil)

	// Set read and write options
	db.ro = levigo.NewReadOptions()
	db.wo = levigo.NewWriteOptions()

	godbc.Ensure(db.ro != nil)
	godbc.Ensure(db.wo != nil)
	godbc.Ensure(db.cachesize > 0)

	return db
}

func (c *LevelDBCache) Close() {
	c.bloomf.Close()
	c.wo.Close()
	c.ro.Close()
	c.db.Close()
}

func (c *LevelDBCache) Invalidate(key string) {
	if val, _ := c.db.Get(c.ro, []byte(key)); val != nil {
		c.stats.writehits++
		c.stats.invalidations++
		c.keyn--
		c.db.Delete(c.wo, []byte(key))
	}
}

func (c *LevelDBCache) Evict() {
	c.stats.evictions++

	for evicted := false; !evicted; {
		it := c.db.NewIterator(c.ro)
		defer it.Close()

		for it.SeekToFirst(); it.Valid(); it.Next() {
			if it.Value()[0] == 1 {
				c.db.Put(c.wo, it.Key(), leveldbunvalset)
			} else {
				c.db.Delete(c.wo, it.Key())
				c.keyn--
				evicted = true
				break
			}
		}
	}
}

func (c *LevelDBCache) Insert(key string) {
	c.stats.insertions++

	// Check for evictions
	if c.keyn >= c.cachesize {
		c.Evict()
	}

	// Insert new key in cache map
	c.db.Put(c.wo, []byte(key), leveldbvalset)
	c.keyn++
}

func (c *LevelDBCache) Write(obj string, chunk string) {
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

func (c *LevelDBCache) Read(obj, chunk string) {
	c.stats.reads++

	key := obj + chunk

	if val, _ := c.db.Get(c.ro, []byte(key)); val != nil {
		// Read Hit
		c.stats.readhits++

		// Clock Algorithm: Set that we looked
		// at it
		c.db.Put(c.wo, []byte(key), leveldbvalset)
	} else {
		// Read miss
		// We would do IO here
		c.Insert(key)
	}
}

func (c *LevelDBCache) Delete(obj string) {
	// Not supported
}

func (c *LevelDBCache) String() string {
	return fmt.Sprintf(
		"== Cache Information ==\n"+
			"Cache Utilization: %.2f %%\n",
		float64(c.keyn)/float64(c.cachesize)*100.0) +
		c.stats.String()
}

func (c *LevelDBCache) Stats() *CacheStats {
	return c.stats.Copy()
}
