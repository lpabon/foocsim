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

package zipfworkload

import (
	"fmt"
	"github.com/lpabon/godbc"
	"math/rand"
	"time"
)

type ZipfWorkload struct {
	objs  uint64
	readp int
	zipf  *rand.Zipf
	rv    *rand.Rand
}

func NewZipfWorkloadsv(imax uint64, readp int, s float64, v float64, seed int64) *ZipfWorkload {
	godbc.Require(0 <= readp && readp <= 100)
	godbc.Require(imax > 0)

	z := ZipfWorkload{}
	z.rv = rand.New(rand.NewSource(seed))
	z.zipf = rand.NewZipf(z.rv, s, v, imax-1)
	z.readp = readp
	z.objs = imax

	godbc.Ensure(z.objs != 0)
	godbc.Ensure(z.readp == readp)
	godbc.Ensure(z.zipf != nil)
	godbc.Ensure(z.rv != nil)

	return &z
}

func NewZipfWorkloadSeed(imax uint64, readp int, seed int64) *ZipfWorkload {
	return NewZipfWorkloadsv(imax, readp, 1.1, 10, seed)
}

func NewZipfWorkload(imax uint64, readp int) *ZipfWorkload {
	return NewZipfWorkloadsv(imax, readp, 1.1, 10, time.Now().UnixNano())
}

func (z *ZipfWorkload) ZipfGenerate() (uint64, bool) {
	return z.zipf.Uint64(), z.rv.Intn(100) < z.readp
}

func (z *ZipfWorkload) Invariant() bool {
	if (z.rv != nil) &&
		(z.zipf != nil) &&
		(0 <= z.readp && z.readp <= 100) &&
		(z.objs > 0) {
		return true
	}
	return false

}

func (z *ZipfWorkload) String() string {
	return fmt.Sprintf("objs:%v readp:%v zipf:%v rv:%v",
		z.objs, z.readp, z.zipf, z.rv)
}
