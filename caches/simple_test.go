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

func TestNewCache(t *testing.T) {
	assert.Panics(t, func() {
		NewCache(0, false)
	})

	c := NewCache(uint64(100), true)
	assert.Equal(t, uint64(100), c.cachesize)
	assert.True(t, c.writethrough)

	c = NewCache(uint64(200), false)
	assert.Equal(t, uint64(200), c.cachesize)
	assert.False(t, c.writethrough)
}

func TestCopy(t *testing.T) {
	c := NewCache(100, true)
	ccopy := c.Copy()

	assert.ObjectsAreEqual(c, ccopy)
}

func TestInvalidate(t *testing.T) {
	c := NewCache(100, true)

	// Invalidate with nothing there
	_, ok := c.cachemap["test"]
	assert.False(t, ok)
	c.Invalidate("test")
	assert.Equal(t, 0, c.writehits)
	assert.Equal(t, 0, c.invalidations)

	// Now insert the key and invalidate
	c.cachemap["test"] = 1
	c.Invalidate("test")
	assert.Equal(t, 1, c.writehits)
	assert.Equal(t, 1, c.invalidations)
	_, ok = c.cachemap["test"]
	assert.False(t, ok)
}

func TestEvict(t *testing.T) {
	c := NewCache(10, true)

	c.cachemap["test"] = 1
	c.Evict()
	assert.Equal(t, 1, c.evictions)
	_, ok := c.cachemap["test"]
	assert.False(t, ok)

	c.cachemap["thisonestays"] = 1
	c.cachemap["tobeevicted"] = 0
	c.Evict()
	assert.Equal(t, 2, c.evictions)
	_, ok = c.cachemap["thisonestays"]
	assert.True(t, ok)
	_, ok = c.cachemap["tobeevicted"]
	assert.False(t, ok)

	c.Evict()
	assert.Equal(t, 3, c.evictions)
	_, ok = c.cachemap["thisonestays"]
	assert.False(t, ok)
}

func TestInsert(t *testing.T) {
	c := NewCache(2, true)

	assert.Equal(t, 0, c.insertions)
	assert.Equal(t, 0, c.evictions)

	c.Insert("a")
	assert.Equal(t, 1, c.insertions)
	assert.Equal(t, 0, c.evictions)
	_, ok := c.cachemap["a"]
	assert.True(t, ok)

	c.Insert("b")
	assert.Equal(t, 2, c.insertions)
	assert.Equal(t, 0, c.evictions)
	_, ok = c.cachemap["a"]
	assert.True(t, ok)
	_, ok = c.cachemap["b"]
	assert.True(t, ok)

	c.Insert("c")
	assert.Equal(t, 3, c.insertions)
	assert.Equal(t, 1, c.evictions)
	_, ok = c.cachemap["a"]
	assert.False(t, ok)
	_, ok = c.cachemap["b"]
	assert.True(t, ok)
	_, ok = c.cachemap["c"]
	assert.True(t, ok)

}
