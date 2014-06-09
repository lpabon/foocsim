package main

import (
	"fmt"
	"math/rand"
	"time"
)

func main() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	imax := uint64(1 * 1024 * 1024 * 1024) // Max number of ints
	s := float64(2)
	v := float64(10)
	z := rand.NewZipf(r, s, v, imax)
	h := make(map[uint64]int)

	for i := 0; i < 10000000; i++ {
		h[z.Uint64()] += 1
	}

	for k, v := range h {
		if v >= 10 {
			fmt.Printf("%v %d\n", k, v)
		}
	}

}
