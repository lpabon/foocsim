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
	"github.com/boltdb/bolt"
	"github.com/lpabon/godbc"
	"os"
)

func init() {
	buf = make([]byte, 4096)
}

type IoCacheBoltDB struct {
	stats        CacheStats
	cachemap     map[string]uint64
	cachesize    uint64
	writethrough bool
	cacheblocks  *IoCacheBlocks
	db           *bolt.DB
}

func NewIoCacheBoltDB(cachesize uint64, writethrough bool) *IoCacheBoltDB {

	var err error

	godbc.Require(cachesize > 0)

	cache := &IoCacheBoltDB{}
	cache.cacheblocks = NewIoCacheBlocks(cachesize)
	cache.cachemap = make(map[string]uint64)
	cache.cachesize = cachesize
	cache.writethrough = writethrough

	os.Remove("cache.db")
	cache.db, err = bolt.Open("cache.db", 0600, nil)
	godbc.Check(err == nil)

	godbc.Ensure(cache.cachesize > 0)

	return cache
}

func (c *IoCacheBoltDB) boltput(bucket string, key string, val []byte) (err error) {
	err = c.db.Update(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(bucket))
		if b == nil {
			b, err = tx.CreateBucket([]byte(bucket))
			if err != nil {
				return err
			}
		}
		return b.Put([]byte(key), val)

	})

	return err
}

func (c *IoCacheBoltDB) boltget(bucket string, key string) (val []byte, err error) {
	err = c.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(bucket))
		if b == nil {
			val = nil
			err = ErrKeyMissing
			return err
		}
		val = b.Get([]byte(key))
		return nil
	})

	if nil == val {
		err = ErrKeyMissing
	}

	return
}

func (c *IoCacheBoltDB) boltdelete(bucket string, key string) (err error) {
	err = c.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucket)).Delete([]byte(key))
	})

	return
}

func (c *IoCacheBoltDB) Close() {
	c.db.Close()
}

func (c *IoCacheBoltDB) Invalidate(key string) {
	if val, ok := c.cachemap[key]; ok {
		c.stats.writehits++
		c.stats.invalidations++
		delete(c.cachemap, key)
		c.cacheblocks.Free(val)
		c.boltdelete("bucket", key)
	}
}

func (c *IoCacheBoltDB) Insert(key string) {
	c.stats.insertions++

	evictkey, index, _ := c.cacheblocks.Insert(key)

	// Check for evictions
	if evictkey != "" {
		c.stats.evictions++
		delete(c.cachemap, evictkey)
		c.boltdelete("bucket", key)
	}

	// Insert new key in cache map
	c.cachemap[key] = index
	c.boltput("bucket", key, buf)
}

func (c *IoCacheBoltDB) Write(obj string, chunk string) {
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

func (c *IoCacheBoltDB) Read(obj, chunk string) {
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

func (c *IoCacheBoltDB) Delete(obj string) {
	// Not supported
}

func (c *IoCacheBoltDB) String() string {
	return fmt.Sprintf(
		"== Cache Information ==\n"+
			"Cache Utilization: %.2f %%\n",
		float64(len(c.cachemap))/float64(c.cachesize)*100.0) +
		c.stats.String()
}

func (c *IoCacheBoltDB) Stats() *CacheStats {
	return c.stats.Copy()
}
