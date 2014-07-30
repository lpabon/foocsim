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
)

type NullCache struct {
	stats CacheStats
}

func NewNullCache() *NullCache {
	return &NullCache{}
}

func (n *NullCache) Close() {

}

func (c *NullCache) Invalidate(chunkkey string) {
}

func (c *NullCache) Evict() {
	c.stats.evictions++
}

func (c *NullCache) Insert(chunkkey string) {
}

func (c *NullCache) Write(obj string, chunk string) {
	c.stats.writes++
}

func (c *NullCache) Read(obj, chunk string) {
	c.stats.reads++
}

func (c *NullCache) Delete(obj string) {
	c.stats.deletions++
}

func (c *NullCache) String() string {
	return fmt.Sprintf(
		"== Cache Information ==\n"+
			"Cache Utilization: 0\n") +
		c.stats.String()
}

func (c *NullCache) Stats() *CacheStats {
	return c.stats.Copy()
}
