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

func TestNewBoltDBCache(t *testing.T) {
	assert.Panics(t, func() {
		NewBoltDBCache(0, false)
	})

	c := NewBoltDBCache(uint64(100), true)
	assert.Equal(t, uint64(100), c.cachesize)
	assert.True(t, c.writethrough)

	c = NewBoltDBCache(uint64(200), false)
	assert.Equal(t, uint64(200), c.cachesize)
	assert.False(t, c.writethrough)
}

func TestBoltFncs(t *testing.T) {
	c := NewBoltDBCache(uint64(100), true)
	assert.Equal(t, uint64(100), c.cachesize)
	assert.True(t, c.writethrough)

	_, err := c.boltget("bucket", "key")
	assert.Error(t, err)

	err = c.boltput("bucket", "key", []byte("val"))
	assert.NoError(t, err)
	err = c.boltput("bucket", "set", valueset)
	assert.NoError(t, err)
	err = c.boltput("bucket", "unset", valueunset)
	assert.NoError(t, err)

	val, err := c.boltget("bucket", "key")
	assert.NoError(t, err)
	assert.Equal(t, "val", string(val))
	val, err = c.boltget("bucket", "set")
	assert.NoError(t, err)
	assert.Equal(t, valueset, val)
	val, err = c.boltget("bucket", "unset")
	assert.NoError(t, err)
	assert.Equal(t, valueunset, val)

	err = c.boltdelete("bucket", "key")
	assert.NoError(t, err)
	_, err = c.boltget("bucket", "key")
	assert.Error(t, err)
}

func TestBoltDBCacheInvalidate(t *testing.T) {
	c := NewBoltDBCache(100, true)

	// Invalidate with nothing there
	_, err := c.boltget("file", "test")
	assert.Error(t, err)
	c.Invalidate("file", "test")
	assert.Equal(t, 0, c.stats.writehits)
	assert.Equal(t, 0, c.stats.invalidations)

	// Now insert the key and invalidate
	c.boltput("file", "test", valueset)
	c.Invalidate("file", "test")
	assert.Equal(t, 1, c.stats.writehits)
	assert.Equal(t, 1, c.stats.invalidations)
	_, err = c.boltget("file", "test")
	assert.Error(t, err)
}

func TestBoltDBCacheEvict(t *testing.T) {
	c := NewBoltDBCache(10, true)

	c.boltput("file", "test", valueset)
	c.Evict()
	assert.Equal(t, 1, c.stats.evictions)
	_, err := c.boltget("file", "test")
	assert.Error(t, err)

	c.boltput("file", "thisonestays", valueset)
	c.boltput("file", "tobeevicted", valueunset)
	c.Evict()
	assert.Equal(t, 2, c.stats.evictions)
	_, err = c.boltget("file", "thisonestays")
	assert.NoError(t, err)
	_, err = c.boltget("file", "tobeevicted")
	assert.Error(t, err)

	c.Evict()
	assert.Equal(t, 3, c.stats.evictions)
	_, err = c.boltget("file", "thisonestays")
	assert.Error(t, err)
}

func TestBoltDBCacheInsert(t *testing.T) {
	c := NewBoltDBCache(2, true)

	assert.Equal(t, 0, c.stats.insertions)
	assert.Equal(t, 0, c.stats.evictions)

	c.Insert("file", "a")
	assert.Equal(t, 1, c.stats.insertions)
	assert.Equal(t, 0, c.stats.evictions)
	_, err := c.boltget("file", "a")
	assert.NoError(t, err)

	c.Insert("anotherfile", "b")
	assert.Equal(t, 2, c.stats.insertions)
	assert.Equal(t, 0, c.stats.evictions)
	_, err = c.boltget("file", "a")
	assert.NoError(t, err)
	_, err = c.boltget("anotherfile", "b")
	assert.NoError(t, err)

	c.Insert("file", "c")
	assert.Equal(t, 3, c.stats.insertions)
	assert.Equal(t, 1, c.stats.evictions)

}
