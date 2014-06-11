package main

import (
	"bitbucket.org/lpabon/filecc/zipfworkload"
	"bufio"
	"fmt"
	"github.com/lpabon/godbc"
	"os"
)

type LoadInfo struct {
	numaccess, reads, writes int
}

func main() {
	z := zipfworkload.NewZipfWorkload(1*1024*1024*1024, 90)
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
