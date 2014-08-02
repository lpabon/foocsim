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

func TestNewLevelDBCache(t *testing.T) {
	assert.Panics(t, func() {
		NewLevelDBCache(0, false)
	})

	c := NewLevelDBCache(uint64(100), true)
	assert.Equal(t, uint64(100), c.cachesize)
	assert.True(t, c.writethrough)
	c.Close()

	c = NewLevelDBCache(uint64(200), false)
	assert.Equal(t, uint64(200), c.cachesize)
	assert.False(t, c.writethrough)
	c.Close()
}

func TestLevelDBCacheInvalidate(t *testing.T) {
	c := NewLevelDBCache(100, true)

	// Invalidate with nothing there
	c.Invalidate("test")
	assert.Equal(t, 0, c.stats.writehits)
	assert.Equal(t, 0, c.stats.invalidations)

	// Now insert the key and invalidate
	c.Insert("test")
	c.Invalidate("test")
	assert.Equal(t, 1, c.stats.writehits)
	assert.Equal(t, 1, c.stats.invalidations)
	c.Invalidate("test")
	assert.Equal(t, 1, c.stats.writehits)
	assert.Equal(t, 1, c.stats.invalidations)
	c.Close()
}

func TestLevelDBCacheEvict(t *testing.T) {
	c := NewLevelDBCache(10, true)

	c.Insert("test")
	c.Evict()
	assert.Equal(t, 1, c.stats.evictions)

	c.Insert("key1")
	c.Insert("key2")
	c.Evict()
	assert.Equal(t, 2, c.stats.evictions)

	c.Evict()
	assert.Equal(t, 3, c.stats.evictions)
	c.Close()
}

func TestLevelDBCacheInsert(t *testing.T) {
	c := NewLevelDBCache(2, true)

	assert.Equal(t, 0, c.stats.insertions)
	assert.Equal(t, 0, c.stats.evictions)

	c.Insert("a")
	assert.Equal(t, 1, c.stats.insertions)
	assert.Equal(t, 0, c.stats.evictions)

	c.Insert("b")
	assert.Equal(t, 2, c.stats.insertions)
	assert.Equal(t, 0, c.stats.evictions)

	c.Insert("c")
	assert.Equal(t, 3, c.stats.insertions)
	assert.Equal(t, 1, c.stats.evictions)
	c.Close()

}
