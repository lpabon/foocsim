package main

import (
	"bufio"
	"fmt"
	"github.com/lpabon/godbc"
	"math/rand"
	"os"
	"time"
)

func main() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	imax := uint64(1 * 1024 * 1024 * 1024) // Max number of ints
	s := float64(1.1)
	v := float64(10)
	z := rand.NewZipf(r, s, v, imax)
	h := make(map[uint64]int)

	for i := 0; i < 20000000; i++ {
		h[z.Uint64()] += 1
	}

	fp, err := os.Create("filecc.data")
	godbc.Check(err == nil)
	defer fp.Close()
	w := bufio.NewWriter(fp)

	for k, v := range h {
		_, err := w.WriteString(fmt.Sprintf("%v %d\n", k, v))
		godbc.Check(err == nil)
	}

}
