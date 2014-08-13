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
	"github.com/lpabon/bufferio"
	"github.com/lpabon/foocsim/kvdb"
	"github.com/lpabon/godbc"
	"sync"
)

var buf []byte

type IoCacheRequest struct {
	put   bool
	key   string
	index uint64
}

type IoCacheKvDB struct {
	stats        CacheStats
	cachemap     map[string]uint64
	cachesize    uint64
	writethrough bool
	cacheblocks  *IoCacheBlocks
	chunksize    uint32
	db           kvdb.Kvdb
	chwrite      chan File
	chread       chan File
	chstats      chan *CacheStats
	chstatsreq   chan int
	chquit       chan int
	chdbsend     chan IoCacheRequest
	wg           sync.WaitGroup
}

func NewIoCacheKvDB(cachesize uint64, writethrough bool, chunksize uint32, dbtype string) *IoCacheKvDB {

	godbc.Require(cachesize > 0)

	cache := &IoCacheKvDB{}
	cache.cacheblocks = NewIoCacheBlocks(cachesize)
	cache.cachemap = make(map[string]uint64)
	cache.cachesize = cachesize
	cache.writethrough = writethrough
	cache.chunksize = chunksize

	cache.chwrite = make(chan File)
	cache.chread = make(chan File)
	cache.chquit = make(chan int)
	cache.chstats = make(chan *CacheStats)
	cache.chstatsreq = make(chan int)
	cache.chdbsend = make(chan IoCacheRequest, 32)

	cache.server()
	cache.ioserver()
	//cache.ioget()
	cache.wg.Add(2)

	buf = make([]byte, chunksize)

	switch dbtype {
	case "leveldb":
		cache.db = kvdb.NewKVLevelDB("cache.ioleveldb")
	case "rocksdb":
		cache.db = kvdb.NewKVRocksDB("cache.iorocksdb")
	case "boltdb":
		cache.db = kvdb.NewKVBoltDB("cache.ioboltdb")
	case "iodb":
		cache.db = kvdb.NewKVIoDB("cache.iodb", cachesize, chunksize)
	default:
		godbc.Check(false, "Unknown cache db type")
	}

	godbc.Check(cache.db != nil)
	godbc.Ensure(cache.cachesize > 0)

	return cache
}

func (c *IoCacheKvDB) Close() {
	close(c.chquit)
	c.wg.Wait()
	c.db.Close()
}

func (c *IoCacheKvDB) server() {
	go func() {
		defer c.wg.Done()
		for {
			select {
			case f := <-c.chwrite:
				c.write(f.obj, f.chunk)
			case f := <-c.chread:
				c.read(f.obj, f.chunk)
			case <-c.chstatsreq:
				c.chstats <- c.stats.Copy()
			case <-c.chquit:
				close(c.chdbsend)
				return
			}
		}
	}()

}

func (c *IoCacheKvDB) ioserver() {
	go func() {

		defer c.wg.Done()
		for req := range c.chdbsend {
			if !req.put {
				val, err := c.db.Get([]byte(req.key), req.index)
				godbc.Check(err == nil)

				// Check Data returned.
				var indexcheck uint64
				keycheck := make([]byte, len(req.key))
				b := bufferio.NewBufferIO(val)
				b.Read(keycheck)
				b.ReadDataLE(&indexcheck)
				godbc.Check(indexcheck == req.index, fmt.Sprintf("index[%v] != %v", req.index, indexcheck))
				godbc.Check(req.key == string(keycheck), fmt.Sprintf("key[%s] != %s", req.key, keycheck))
			} else {
				b := bufferio.NewBufferIOMake(int(c.chunksize))
				b.Write([]byte(req.key))
				b.WriteDataLE(req.index)

				c.db.Put([]byte(req.key), b.Bytes(), req.index)
			}

		}
	}()

}

func (c *IoCacheKvDB) Invalidate(key string) {
	if index, ok := c.cachemap[key]; ok {
		c.stats.writehits++
		c.stats.invalidations++
		delete(c.cachemap, key)
		c.cacheblocks.Free(index)
		c.db.Delete([]byte(key), index)
	}
}

func (c *IoCacheKvDB) Insert(key string) {
	c.stats.insertions++

	evictkey, index, _ := c.cacheblocks.Insert(key)

	// Check for evictions
	if evictkey != "" {
		c.stats.evictions++
		delete(c.cachemap, evictkey)
		c.db.Delete([]byte(evictkey), index)
	}

	// Insert new key in cache map
	c.cachemap[key] = index

	c.chdbsend <- IoCacheRequest{
		put:   true,
		key:   key,
		index: index,
	}
}

func (c *IoCacheKvDB) write(obj string, chunk string) {
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

func (c *IoCacheKvDB) Write(obj string, chunk string) {

	c.chwrite <- File{obj, chunk}

}

func (c *IoCacheKvDB) read(obj, chunk string) {
	c.stats.reads++

	key := obj + chunk

	if index, ok := c.cachemap[key]; ok {
		// Read Hit
		c.stats.readhits++

		// Clock Algorithm: Set that we looked
		// at it
		c.cacheblocks.Using(index)

		c.chdbsend <- IoCacheRequest{
			put:   false,
			key:   key,
			index: index,
		}

	} else {
		// Read miss
		// We would do IO here
		c.Insert(key)
	}
}

func (c *IoCacheKvDB) Read(obj, chunk string) {

	c.chread <- File{obj, chunk}
}

func (c *IoCacheKvDB) Delete(obj string) {
	// Not supported
}

func (c *IoCacheKvDB) String() string {
	return fmt.Sprintf(
		"== Cache Information ==\n"+
			"Cache Utilization: %.2f %%\n",
		float64(len(c.cachemap))/float64(c.cachesize)*100.0) +
		c.stats.String() +
		c.db.String()
}

func (c *IoCacheKvDB) Stats() *CacheStats {
	c.chstatsreq <- 1
	return <-c.chstats
}
