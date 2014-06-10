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

type CachePerformance struct {
	reads, writes uint64
}

type Cache struct {
	cachemap  map[string]int
	cachesize uint64
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
	numios := 100000000

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

	for io := 0; io < numios; io++ {
		// Get the file
		file, _ := filezipf.ZipfGenerate()
		godbc.Check(int(file) <= numfiles, fmt.Sprintf("file = %v", file))

		// Which block on the file
		_, isread := files[file].iogen.ZipfGenerate()
		files[file].ios++

		//if nil == files[file].blockinfo[obj] {
		//	files[file].blockinfo[obj] = &LoadInfo{}
		//}

		//files[file].blockinfo[obj].numaccess += 1
		if isread {
			files[file].reads++
			//files[file].blockinfo[obj].reads += 1
		} else {
			files[file].writes++
			//files[file].blockinfo[obj].writes += 1
		}
	}

	fp, err := os.Create("files.data")
	godbc.Check(err == nil)
	defer fp.Close()
	w := bufio.NewWriter(fp)

	for file := range files {
		_, err := w.WriteString(fmt.Sprintf("%s %v %v %v %v\n",
			files[file].name,
			files[file].size,
			files[file].ios,
			files[file].reads,
			files[file].writes))
		godbc.Check(err == nil)
	}

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
