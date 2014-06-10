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

func (c *Cache) Insert(chunkkey string) {
	c.insertions++
	c.cachemap[chunkkey] = 1
}

func (c *Cache) Write(obj string, chunk string) {
	c.writes++

	key := c.GetObjKey(obj) + chunk

	// Invalidate
	c.Invalidate(key)

	// We would do back end IO here

	// Insert
	c.Insert(key)
}

func (c *Cache) Read(obj, chunk string) {
	c.reads++

	key := c.GetObjKey(obj) + chunk

	if _, ok := c.cachemap[key]; ok {
		// Read Hit
		c.readhits++
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
			"%d\n", // Invalidations 10
		c.ReadHitRate(),
		c.WriteHitRate(),
		c.readhits,
		c.writehits,
		c.deletionhits,
		c.reads,
		c.writes,
		c.deletions,
		c.insertions,
		c.invalidations)

}

type SimFile struct {
	iogen              *zipfworkload.ZipfWorkload
	blockinfo          map[uint64]*LoadInfo
	size               uint64
	name               string
	reads, writes, ios uint64
}

func main() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Config
	maxfilesize := int64(1 * 1024 * 1024 * 1024)
	numfiles := 1000
	numios := 10000000

	// Create environment
	filezipf := zipfworkload.NewZipfWorkload(uint64(numfiles), 0)
	files := make([]*SimFile, numfiles)
	for file := 0; file < numfiles; file++ {
		files[file] = &SimFile{}
		files[file].size = uint64(r.Int63n(maxfilesize))
		files[file].iogen = zipfworkload.NewZipfWorkload(files[file].size, 75)
		files[file].name = strconv.Itoa(file)
		files[file].blockinfo = make(map[uint64]*LoadInfo)
	}

	// Setup file to write cache metrics
	fp, err := os.Create("cache.data")
	godbc.Check(err == nil)
	metrics := bufio.NewWriter(fp)
	defer metrics.Flush()
	defer fp.Close()

	// Create the cache
	cache := &Cache{}
	cache.cacheobjids = make(map[string]string)
	cache.cachemap = make(map[string]int)

	for io := 0; io < numios; io++ {
		if (io % 10000) == 0 {
			_, err := metrics.WriteString(fmt.Sprintf("%d,", io) + cache.Dump())
			godbc.Check(err == nil)
		}
		// Get the file
		file, _ := filezipf.ZipfGenerate()
		godbc.Check(int(file) <= numfiles, fmt.Sprintf("file = %v", file))

		// Which block on the file
		chunk, isread := files[file].iogen.ZipfGenerate()
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
