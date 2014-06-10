package main

import (
	"bitbucket.org/lpabon/filecc/zipfworkload"
	"bufio"
	//"crypto/md5"
	"fmt"
	"github.com/lpabon/godbc"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type LoadInfo struct {
	numaccess, reads, writes int
}

func demo(z *zipfworkload.ZipfWorkload) {
	h := make(map[uint64]*LoadInfo)
	for i := 0; i < 20000000; i++ {
		obj, isread := z.ZipfGenerate()
		if nil == h[obj] {
			h[obj] = &LoadInfo{}
		}
		h[obj].numaccess += 1
		if isread {
			h[obj].reads += 1
		} else {
			h[obj].writes += 1
		}
	}

	fp, err := os.Create("filecc.data")
	godbc.Check(err == nil)
	defer fp.Close()
	w := bufio.NewWriter(fp)

	for k, v := range h {
		_, err := w.WriteString(fmt.Sprintf("%v %d %d %d\n", k, v.numaccess, v.reads, v.writes))
		godbc.Check(err == nil)
	}
}

type Cache struct {
	cacheobjids              map[string]string
	cachemap                 map[string]int
	cachesize                uint64
	readhits, writehits      int
	reads, writes            int
	deletions, deletionhits  int
	evictions, invalidations int
	insertions               int
	writethrough             bool
}

func CacheCreateObjKey(obj string) func() string {
	counter := 0
	return func() string {
		counter += 1
		return strconv.Itoa(counter)
	}
}

func (c *Cache) GetObjKey(obj string) string {
	if val, ok := c.cacheobjids[obj]; ok {
		return val
	} else {
		newid := CacheCreateObjKey(obj)
		c.cacheobjids[obj] = newid()
		return c.cacheobjids[obj]
	}
}

func (c *Cache) Invalidate(chunkkey string) {
	if _, ok := c.cachemap[chunkkey]; ok {
		c.writehits++
		c.invalidations++
		delete(c.cachemap, chunkkey)
	}
}

func (c *Cache) Evict() {
	c.evictions++

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

func (c *Cache) Insert(chunkkey string) {
	c.insertions++

	if uint64(len(c.cachemap)) > c.cachesize {
		c.Evict()
	}

	c.cachemap[chunkkey] = 1
}

func (c *Cache) Write(obj string, chunk string) {
	c.writes++

	key := c.GetObjKey(obj) + chunk

	// Invalidate
	c.Invalidate(key)

	// We would do back end IO here

	// Insert
	if c.writethrough {
		c.Insert(key)
	}
}

func (c *Cache) Read(obj, chunk string) {
	c.reads++

	key := c.GetObjKey(obj) + chunk

	if _, ok := c.cachemap[key]; ok {
		// Read Hit
		c.readhits++

		// Clock Algorithm: Set that we looked
		// at it
		c.cachemap[key] = 1
	} else {
		// Read miss
		// We would do IO here
		c.Insert(key)
	}
}

func (c *Cache) Delete(obj string) {
	c.deletions++

	if _, ok := c.cacheobjids[obj]; ok {
		c.deletionhits++
		delete(c.cacheobjids, obj)
	}
}

func (c *Cache) ReadHitRate() float64 {
	if c.reads == 0 {
		return 0.0
	} else {
		return float64(c.readhits) / float64(c.reads)
	}
}

func (c *Cache) WriteHitRate() float64 {
	if c.writes == 0 {
		return 0.0
	} else {
		return float64(c.writehits) / float64(c.writes)
	}

}

func (c *Cache) ReadHitRateDelta(prev *Cache) float64 {
	reads := c.reads - prev.reads
	readhits := c.readhits - prev.readhits
	if reads == 0 {
		return 0.0
	} else {
		return float64(readhits) / float64(reads)
	}
}

func (c *Cache) WriteHitRateDelta(prev *Cache) float64 {
	writes := c.writes - prev.writes
	writehits := c.writehits - prev.writehits
	if writes == 0 {
		return 0.0
	} else {
		return float64(writehits) / float64(writes)
	}

}

func (c *Cache) String() string {
	return fmt.Sprintf(
		"Cache Information:\n"+
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

func (c *Cache) Dump() string {
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

func (c *Cache) DumpDelta(prev *Cache) string {
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

type SimFile struct {
	iogen              *zipfworkload.ZipfWorkload
	blockinfo          map[uint64]*LoadInfo
	size               uint64
	name               string
	reads, writes, ios uint64
}

func main() {

	// Setup seed for random numbers
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Config
	chunksize := 256 * 1024
	maxfilesize := int64(10*1024*1024*1024) / int64(chunksize) // Up to 10 GB in 256k chunks
	cachesize := uint64(1*1024*1024*1024) / uint64(chunksize)  // 1 GB divided into 256k chunks
	numfiles := 10000
	numios := 100000000
	deletion_chance := 10 // percent
	read_chance := 65     // percent
	writethrough := true

	// Create environment
	filezipf := zipfworkload.NewZipfWorkload(uint64(numfiles), 0)
	files := make([]*SimFile, numfiles)
	for file := 0; file < numfiles; file++ {
		files[file] = &SimFile{}
		files[file].size = uint64(r.Int63n(maxfilesize))
		files[file].iogen = zipfworkload.NewZipfWorkload(files[file].size, read_chance)
		files[file].name = strconv.Itoa(file)
		files[file].blockinfo = make(map[uint64]*LoadInfo)
	}

	// Setup file to write cache metrics
	fp, err := os.Create("cache.data")
	godbc.Check(err == nil)
	metrics := bufio.NewWriter(fp)

	// XXX Don't know if this works.. probably does not
	defer metrics.Flush()
	defer fp.Close()

	// Create the cache
	cache_prev := &Cache{}
	cache := &Cache{}
	cache.cachesize = cachesize
	cache.writethrough = writethrough
	cache.cacheobjids = make(map[string]string)
	cache.cachemap = make(map[string]int)

	for io := 0; io < numios; io++ {

		// Save metrics
		if (io % 10000) == 0 {
			_, err := metrics.WriteString(fmt.Sprintf("%d,", io) + cache.DumpDelta(cache_prev))
			godbc.Check(err == nil)

			// Now copy the data
			*cache_prev = *cache
		}

		// Get the file
		file, _ := filezipf.ZipfGenerate()
		godbc.Check(int(file) <= numfiles, fmt.Sprintf("file = %v", file))

		// Check if we need to delete this file
		if rand.Intn(100) < deletion_chance {
			cache.Delete(strconv.FormatUint(file, 10))
			continue
		}

		// Which block on the file
		chunk, isread := files[file].iogen.ZipfGenerate()

		// Track the number of IOs to this file
		files[file].ios++

		//if nil == files[file].blockinfo[obj] {
		//	files[file].blockinfo[obj] = &LoadInfo{}
		//}

		//files[file].blockinfo[obj].numaccess += 1
		if isread {
			files[file].reads++
			cache.Read(strconv.FormatUint(file, 10), strconv.FormatUint(chunk, 10))
			//files[file].blockinfo[obj].reads += 1
		} else {
			files[file].writes++
			cache.Write(strconv.FormatUint(file, 10), strconv.FormatUint(chunk, 10))
			//files[file].blockinfo[obj].writes += 1
		}
	}

	fmt.Print(cache)

	//c = &Cache{}
	/*
		h := md5.New()
		val := h.Sum([]byte(string(1)))
		fmt.Printf("%s::%x map[%v] = %v",
			string(val),
			val,
			0,
			0)
	*/
}
