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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewIoCache(t *testing.T) {
	assert.Panics(t, func() {
		NewIoCache(0, false)
	})

	c := NewIoCache(uint64(100), true)
	assert.Equal(t, uint64(100), c.cachesize)
	assert.True(t, c.writethrough)

	c = NewIoCache(uint64(200), false)
	assert.Equal(t, uint64(200), c.cachesize)
	assert.False(t, c.writethrough)
}

func TestIoCacheInvalidate(t *testing.T) {
	c := NewIoCache(100, true)

	// Invalidate with nothing there
	_, ok := c.cachemap["test"]
	assert.False(t, ok)
	c.Invalidate("test")
	assert.Equal(t, 0, c.stats.writehits)
	assert.Equal(t, 0, c.stats.invalidations)

	// Now insert the key and invalidate
	c.cachemap["test"] = 1
	c.Invalidate("test")
	assert.Equal(t, 1, c.stats.writehits)
	assert.Equal(t, 1, c.stats.invalidations)
	_, ok = c.cachemap["test"]
	assert.False(t, ok)
}

func TestIoCacheEvictions(t *testing.T) {
	c := NewIoCache(2, true)

	c.Insert("key1")
	assert.Equal(t, 0, c.stats.evictions)
	_, ok := c.cachemap["key1"]
	assert.True(t, ok)
	assert.True(t, c.cacheblocks.cacheblocks[0].mru)
	assert.True(t, c.cacheblocks.cacheblocks[0].used)
	assert.Equal(t, "key1", c.cacheblocks.cacheblocks[0].key)

	c.Insert("key2")
	assert.Equal(t, 0, c.stats.evictions)
	_, ok = c.cachemap["key1"]
	assert.True(t, ok)
	assert.True(t, c.cacheblocks.cacheblocks[0].mru)
	assert.True(t, c.cacheblocks.cacheblocks[0].used)
	assert.Equal(t, "key1", c.cacheblocks.cacheblocks[0].key)
	_, ok = c.cachemap["key2"]
	assert.True(t, ok)
	assert.True(t, c.cacheblocks.cacheblocks[1].mru)
	assert.True(t, c.cacheblocks.cacheblocks[1].used)
	assert.Equal(t, "key2", c.cacheblocks.cacheblocks[1].key)

	c.Insert("key3")
	assert.Equal(t, 1, c.stats.evictions)
	_, ok = c.cachemap["key1"]
	assert.False(t, ok)
	assert.NotEqual(t, "key1", c.cacheblocks.cacheblocks[0].key)
	_, ok = c.cachemap["key2"]
	assert.True(t, ok)
	assert.False(t, c.cacheblocks.cacheblocks[1].mru)
	assert.True(t, c.cacheblocks.cacheblocks[1].used)
	assert.Equal(t, "key2", c.cacheblocks.cacheblocks[1].key)
	_, ok = c.cachemap["key3"]
	assert.True(t, ok)
	assert.True(t, c.cacheblocks.cacheblocks[0].mru)
	assert.True(t, c.cacheblocks.cacheblocks[0].used)
	assert.Equal(t, "key3", c.cacheblocks.cacheblocks[0].key)

	// key3 will be invalidated.
	// key2 is still available, but
	// it has an mru of 0
	c.Invalidate("key3")
	assert.Equal(t, 1, c.stats.evictions)
	_, ok = c.cachemap["key2"]
	assert.False(t, c.cacheblocks.cacheblocks[1].mru)
	assert.True(t, c.cacheblocks.cacheblocks[1].used)
	assert.Equal(t, "key2", c.cacheblocks.cacheblocks[1].key)
	assert.True(t, ok)
	_, ok = c.cachemap["key3"]
	assert.False(t, ok)
	assert.False(t, c.cacheblocks.cacheblocks[0].mru)
	assert.False(t, c.cacheblocks.cacheblocks[0].used)
	assert.Equal(t, "", c.cacheblocks.cacheblocks[0].key)

	// key2 will be evicted since the
	// index is pointing to it
	c.Insert("key4")
	assert.Equal(t, 2, c.stats.evictions)
	_, ok = c.cachemap["key2"]
	assert.NotEqual(t, "key2", c.cacheblocks.cacheblocks[1].key)
	_, ok = c.cachemap["key4"]
	assert.True(t, ok)
	assert.True(t, c.cacheblocks.cacheblocks[1].mru)
	assert.True(t, c.cacheblocks.cacheblocks[1].used)
	assert.Equal(t, "key4", c.cacheblocks.cacheblocks[1].key)
	assert.False(t, c.cacheblocks.cacheblocks[0].mru)
	assert.False(t, c.cacheblocks.cacheblocks[0].used)
	assert.Equal(t, "", c.cacheblocks.cacheblocks[0].key)

}

func TestIoCacheInsert(t *testing.T) {
	c := NewIoCache(2, true)

	assert.Equal(t, 0, c.stats.insertions)
	assert.Equal(t, 0, c.stats.evictions)

	c.Insert("a")
	assert.Equal(t, 1, c.stats.insertions)
	assert.Equal(t, 0, c.stats.evictions)
	_, ok := c.cachemap["a"]
	assert.True(t, ok)

	c.Insert("b")
	assert.Equal(t, 2, c.stats.insertions)
	assert.Equal(t, 0, c.stats.evictions)
	_, ok = c.cachemap["a"]
	assert.True(t, ok)
	_, ok = c.cachemap["b"]
	assert.True(t, ok)

	c.Insert("c")
	assert.Equal(t, 3, c.stats.insertions)
	assert.Equal(t, 1, c.stats.evictions)
	_, ok = c.cachemap["a"]
	assert.False(t, ok)
	_, ok = c.cachemap["b"]
	assert.True(t, ok)
	_, ok = c.cachemap["c"]
	assert.True(t, ok)
}
