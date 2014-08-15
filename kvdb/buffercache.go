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

package kvdb

import (
	"errors"
	"github.com/lpabon/godbc"
	"sync"
)

var (
	ErrKeyNotFound = errors.New("Key not found")
)

type BufferCacheBlock struct {
	key  uint64
	mru  bool
	used bool
	data []byte
}

type BufferCache struct {
	cacheblocks []BufferCacheBlock
	keymap      map[uint64]uint64
	index       uint64
	lock        sync.Mutex
}

func NewBufferCache(cachesize, blocksize uint64) *BufferCache {

	b := &BufferCache{}
	numblocks := cachesize / blocksize
	b.cacheblocks = make([]BufferCacheBlock, numblocks)
	b.keymap = make(map[uint64]uint64)

	for i := 0; i < len(b.cacheblocks); i++ {
		b.cacheblocks[i].data = make([]byte, blocksize)
	}

	return b
}

func (c *BufferCache) remove(index uint64) {
	delete(c.keymap, c.cacheblocks[index].key)

	c.cacheblocks[index].mru = false
	c.cacheblocks[index].used = false
	c.cacheblocks[index].key = 0
}

func (c *BufferCache) Set(key uint64, buf []byte) (err error) {

	c.lock.Lock()
	defer c.lock.Unlock()

	// Yes i know its the same as Invalides.. I'll fix it later!
	// :-)
	if index, ok := c.keymap[key]; ok {
		c.remove(index)
	}

	for {
		for ; c.index < uint64(len(c.cacheblocks)); c.index++ {
			if c.cacheblocks[c.index].mru {
				c.cacheblocks[c.index].mru = false
			} else {
				if c.cacheblocks[c.index].used {
					c.remove(c.index)
				}

				err = nil

				c.cacheblocks[c.index].key = key
				c.cacheblocks[c.index].mru = true
				c.cacheblocks[c.index].used = true

				godbc.Check(len(buf) == len(c.cacheblocks[c.index].data))
				copy(c.cacheblocks[c.index].data, buf)

				c.keymap[key] = c.index
				c.index++

				return
			}
		}
		c.index = 0
	}
}

func (c *BufferCache) Get(key uint64, buf []byte) (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if index, ok := c.keymap[key]; ok {
		c.cacheblocks[index].mru = true
		copy(buf, c.cacheblocks[index].data)

		return nil
	} else {
		return ErrKeyNotFound
	}
}

func (c *BufferCache) Invalidate(key uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if index, ok := c.keymap[key]; ok {
		c.remove(index)
	}
}
