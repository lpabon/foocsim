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
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/lpabon/godbc"
	"os"
)

var valueset = []byte{1}
var valueunset = []byte{0}
var ErrKeyMissing = errors.New("No Key Found")

type BoltDBCache struct {
	stats        CacheStats
	buckets      map[string]int
	db           *bolt.DB
	cachesize    uint64
	writethrough bool
	keyn         uint64
}

func NewBoltDBCache(cachesize uint64, writethrough bool) *BoltDBCache {

	godbc.Require(cachesize > 0)

	var err error
	bdc := &BoltDBCache{}
	bdc.buckets = make(map[string]int)
	bdc.writethrough = writethrough
	bdc.cachesize = cachesize
	os.Remove("cache.db")
	bdc.db, err = bolt.Open("cache.db", 0600, nil)
	godbc.Check(err == nil)

	godbc.Ensure(bdc.buckets != nil)
	godbc.Ensure(bdc.cachesize > 0)

	return bdc
}

func (c *BoltDBCache) boltput(bucket string, key string, val []byte) (err error) {
	err = c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if nil == b {
			b, _ = tx.CreateBucket([]byte(bucket))
			c.buckets[bucket] = 1
		}
		return b.Put([]byte(key), val)
	})
	c.keyn++
	return
}

func (c *BoltDBCache) boltget(bucket string, key string) (val []byte, err error) {
	err = c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if nil == b {
			val = nil
			return ErrKeyMissing
		}
		val = b.Get([]byte(key))
		return nil
	})
	if nil == val {
		err = ErrKeyMissing
	}
	return
}

func (c *BoltDBCache) boltdelete(bucket string, key string) (err error) {
	err = c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if nil == b {
			return nil
		}
		return b.Delete([]byte(key))
	})
	c.keyn--
	return
}

func (c *BoltDBCache) Close() {
	c.db.Close()
}

func (c *BoltDBCache) Invalidate(obj, chunk string) {
	if v, _ := c.boltget(obj, chunk); v != nil {
		c.stats.writehits++
		c.stats.invalidations++

		c.boltdelete(obj, chunk)
	}
}

func (c *BoltDBCache) Evict() {
	c.stats.evictions++

	// BIG ASSUMPTION! I have no idea
	// if Go keeps track of the iteration
	// through a map
	for evicted := false; !evicted; {
		for bucket, _ := range c.buckets {
			c.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(bucket))
				c := b.Cursor()

				for k, v := c.First(); k != nil; k, v = c.Next() {
					if v[0] == 1 {
						b.Put([]byte(k), valueunset)
					} else {
						b.Delete(k)
						evicted = true
						return nil
					}
				}

				return nil
			})
		}
	}
}

func (c *BoltDBCache) Insert(obj, chunk string) {
	c.stats.insertions++

	if c.keyn >= c.cachesize {
		c.Evict()
	}

	c.boltput(obj, chunk, valueset)
}

func (c *BoltDBCache) Write(obj string, chunk string) {
	c.stats.writes++

	// Invalidate
	c.Invalidate(obj, chunk)

	// We would do back end IO here

	// Insert
	if c.writethrough {
		c.Insert(obj, chunk)
	}
}

func (c *BoltDBCache) Read(obj, chunk string) {
	c.stats.reads++

	if v, _ := c.boltget(obj, chunk); v != nil {
		// Read Hit
		c.stats.readhits++

		// Clock Algorithm: Set that we looked
		// at it
		c.boltput(obj, chunk, valueset)
	} else {
		// Read miss
		// We would do IO here
		c.Insert(obj, chunk)
	}

}

func (c *BoltDBCache) Delete(obj string) {
	c.stats.deletions++

	c.db.Update(func(tx *bolt.Tx) (err error) {
		err = tx.DeleteBucket([]byte(obj))
		if err == nil {
			c.stats.deletionhits++
			delete(c.buckets, obj)
		}

		return err
	})
}

func (c *BoltDBCache) String() string {
	return fmt.Sprintf(
		"== Cache Information ==\n"+
			"Cache Utilization: 0\n") +
		c.stats.String() +
		c.db.String()
}

func (c *BoltDBCache) Stats() *CacheStats {
	return c.stats.Copy()
}
