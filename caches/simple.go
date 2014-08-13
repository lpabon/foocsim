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
	"sync"
)

type File struct {
	obj, chunk string
}

type SimpleCache struct {
	cacheobjids  map[string]string
	cachemap     map[string]int
	chwrite      chan File
	chread       chan File
	chdelete     chan File
	chstats      chan *CacheStats
	chstatsreq   chan int
	chquit       chan int
	cachesize    uint64
	wg           sync.WaitGroup
	writethrough bool
	stats        CacheStats
}

func cacheCreateObjKey(obj string) func() string {
	counter := 0
	return func() string {
		godbc.Require(counter >= 0)

		counter += 1
		return strconv.Itoa(counter)
	}
}

func NewSimpleCache(cachesize uint64, writethrough bool) *SimpleCache {

	godbc.Require(cachesize > 0)

	cache := &SimpleCache{}
	cache.cachesize = cachesize
	cache.writethrough = writethrough
	cache.cacheobjids = make(map[string]string)
	cache.cachemap = make(map[string]int)

	cache.chwrite = make(chan File)
	cache.chread = make(chan File)
	cache.chdelete = make(chan File)
	cache.chquit = make(chan int)
	cache.chstats = make(chan *CacheStats)
	cache.chstatsreq = make(chan int)

	cache.server()
	cache.wg.Add(1)

	godbc.Ensure(cache.cacheobjids != nil)
	godbc.Ensure(cache.cachemap != nil)
	godbc.Ensure(cache.cachesize > 0)

	return cache
}

func (c *SimpleCache) server() {
	go func() {
		defer c.wg.Done()
		for {
			select {
			case f := <-c.chwrite:
				c.write(f.obj, f.chunk)
			case f := <-c.chread:
				c.read(f.obj, f.chunk)
			case f := <-c.chdelete:
				c.delete(f.obj)
			case <-c.chstatsreq:
				c.chstats <- c.stats.Copy()
			case <-c.chquit:
				return
			}
		}
	}()

}

func (c *SimpleCache) getObjKey(obj string) string {
	if val, ok := c.cacheobjids[obj]; ok {
		return val
	} else {
		newid := cacheCreateObjKey(obj)
		c.cacheobjids[obj] = newid()
		return c.cacheobjids[obj]
	}
}

func (c *SimpleCache) Close() {
	close(c.chquit)
	c.wg.Wait()
}

func (c *SimpleCache) Invalidate(chunkkey string) {
	if _, ok := c.cachemap[chunkkey]; ok {
		c.stats.writehits++
		c.stats.invalidations++
		delete(c.cachemap, chunkkey)
	}
}

func (c *SimpleCache) Evict() {
	c.stats.evictions++

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

func (c *SimpleCache) Insert(chunkkey string) {
	c.stats.insertions++

	if uint64(len(c.cachemap)) >= c.cachesize {
		c.Evict()
	}

	c.cachemap[chunkkey] = 1
}

func (c *SimpleCache) write(obj, chunk string) {
	c.stats.writes++

	key := c.getObjKey(obj) + chunk

	// Invalidate
	c.Invalidate(key)

	// We would do back end IO here

	// Insert
	if c.writethrough {
		c.Insert(key)
	}
}

func (c *SimpleCache) Write(obj string, chunk string) {

	c.chwrite <- File{obj, chunk}

}

func (c *SimpleCache) read(obj, chunk string) {
	c.stats.reads++
	key := c.getObjKey(obj) + chunk

	if _, ok := c.cachemap[key]; ok {
		// Read Hit
		c.stats.readhits++

		// Clock Algorithm: Set that we looked
		// at it
		c.cachemap[key] = 1
	} else {
		// Read miss
		// We would do IO here
		c.Insert(key)
	}
}

func (c *SimpleCache) Read(obj, chunk string) {

	c.chread <- File{obj, chunk}
}

func (c *SimpleCache) delete(obj string) {
	c.stats.deletions++
	if _, ok := c.cacheobjids[obj]; ok {
		c.stats.deletionhits++
		delete(c.cacheobjids, obj)
	}
}

func (c *SimpleCache) Delete(obj string) {

	c.chdelete <- File{obj: obj}
}

func (c *SimpleCache) String() string {
	return fmt.Sprintf(
		"== Cache Information ==\n"+
			"Cache Utilization: %.2f %%\n",
		float64(len(c.cachemap))/float64(c.cachesize)*100.0) +
		c.stats.String()
}

func (c *SimpleCache) Stats() *CacheStats {
	c.chstatsreq <- 1
	return <-c.chstats
}
