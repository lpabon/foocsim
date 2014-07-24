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

type CacheStats struct {
	readhits, writehits      int
	reads, writes            int
	deletions, deletionhits  int
	evictions, invalidations int
	insertions               int
}

func (c *CacheStats) ReadHitRateDelta(prev *CacheStats) float64 {
	reads := c.reads - prev.reads
	readhits := c.readhits - prev.readhits
	if reads == 0 {
		return 0.0
	} else {
		return float64(readhits) / float64(reads)
	}
}

func (c *CacheStats) WriteHitRateDelta(prev *CacheStats) float64 {
	writes := c.writes - prev.writes
	writehits := c.writehits - prev.writehits
	if writes == 0 {
		return 0.0
	} else {
		return float64(writehits) / float64(writes)
	}

}

func (c *CacheStats) ReadHitRate() float64 {
	if c.reads == 0 {
		return 0.0
	} else {
		return float64(c.readhits) / float64(c.reads)
	}
}

func (c *CacheStats) WriteHitRate() float64 {
	if c.writes == 0 {
		return 0.0
	} else {
		return float64(c.writehits) / float64(c.writes)
	}

}

func (c *CacheStats) Copy() *CacheStats {
	statscopy := &CacheStats{}
	*statscopy = *c

	return statscopy
}

func (c *CacheStats) String() string {
	return fmt.Sprintf(
		"Read Hit Rate: %v\n"+
			"Write Hit Rate: %v\n"+
			"Read hits: %d\n"+
			"Write hits: %d\n"+
			"Delete hits: %d\n"+
			"Reads: %d\n"+
			"Writes: %d\n"+
			"Deletions: %d\n"+
			"Insertions: %d\n"+
			"Evictions: %d\n"+
			"Invalidations: %d\n",
		c.ReadHitRate(),
		c.WriteHitRate(),
		c.readhits,
		c.writehits,
		c.deletionhits,
		c.reads,
		c.writes,
		c.deletions,
		c.insertions,
		c.evictions,
		c.invalidations)
}

func (c *CacheStats) Dump() string {
	return fmt.Sprintf(
		"%v,"+ // Read Hit Rate 1
			"%v,"+ // Write Hit Rate 2
			"%d,"+ // Read Hits 3
			"%d,"+ // Write Hits 4
			"%d,"+ // Deletion Hits 5
			"%d,"+ // Reads 6
			"%d,"+ // Writes 7
			"%d,"+ // Deletions 8
			"%d,"+ // Insertions 9
			"%d,"+ // Evictions 10
			"%d\n", // Invalidations 11
		c.ReadHitRate(),
		c.WriteHitRate(),
		c.readhits,
		c.writehits,
		c.deletionhits,
		c.reads,
		c.writes,
		c.deletions,
		c.insertions,
		c.evictions,
		c.invalidations)

}

func (c *CacheStats) DumpDelta(prev *CacheStats) string {
	return fmt.Sprintf(
		"%v,"+ // Read Hit Rate 1
			"%v,"+ // Write Hit Rate 2
			"%d,"+ // Read Hits 3
			"%d,"+ // Write Hits 4
			"%d,"+ // Deletion Hits 5
			"%d,"+ // Reads 6
			"%d,"+ // Writes 7
			"%d,"+ // Deletions 8
			"%d,"+ // Insertions 9
			"%d,"+ // Evictions 10
			"%d\n", // Invalidations 11
		c.ReadHitRateDelta(prev),
		c.WriteHitRateDelta(prev),
		c.readhits-prev.readhits,
		c.writehits-prev.writehits,
		c.deletionhits-prev.deletionhits,
		c.reads-prev.reads,
		c.writes-prev.writes,
		c.deletions-prev.deletions,
		c.insertions-prev.insertions,
		c.evictions-prev.evictions,
		c.invalidations-prev.invalidations)

}
