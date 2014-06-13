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
