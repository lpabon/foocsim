package main

import (
	"bufio"
	"fmt"
	"github.com/lpabon/godbc"
	"math/rand"
	"os"
	"time"
)

type ZipfWorkload struct {
	objs  uint64
	readp int
	zipf  *rand.Zipf
	rv    *rand.Rand
}

type LoadInfo struct {
	numaccess, reads, writes int
}

func NewZipfWorkload(imax uint64, readp int) *ZipfWorkload {
	godbc.Require(0 <= readp && readp <= 100)

	s := float64(1.1)
	v := float64(10)
	z := ZipfWorkload{}
	z.rv = rand.New(rand.NewSource(time.Now().UnixNano()))
	z.zipf = rand.NewZipf(z.rv, s, v, imax)
	z.readp = readp
	z.objs = imax

	godbc.Ensure(z.objs != 0)
	godbc.Ensure(z.readp == readp)
	godbc.Ensure(z.zipf != nil)
	godbc.Ensure(z.rv != nil)

	return &z
}

func (z *ZipfWorkload) ZipfGenerate() (uint64, bool) {
	return z.zipf.Uint64(), z.rv.Intn(100) < z.readp
}

func main() {
	/*
		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		imax := uint64(1 * 1024 * 1024 * 1024) // Max number of ints
		s := float64(1.1)
		v := float64(10)
		z := rand.NewZipf(r, s, v, imax)
	*/
	z := NewZipfWorkload(1*1024*1024*1024, 90)
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
