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

package iogenerator

import (
	"github.com/lpabon/goioworkload/spc1"
)

type File struct {
	iogen *spc1.Spc1Io
	size  uint64
}

func NewFile(size uint64, readp int) *File {
	f := &File{}
	spc1.Spc1Init(
		100, //bsus: Doesn't matter since we do not use timing
		1,   //contexts
		uint32(size/(2*4096)),           // asu1 in 4k blocks
		uint32(size/(2*4096)),           // asu2 in 4k blocks
		uint32((float64(size)/0.9)*0.1), // asu3, unsused
	)
	f.iogen = spc1.NewSpc1Io(1)
	f.size = size

	return f
}

func (f *File) Gen() (uint64, bool) {
	if f.iogen.Blocks <= 0 {
		f.iogen.Generate()
		for f.iogen.Asu == 3 {
			f.iogen.Generate()
		}
	}
	offset := uint64((uint32(f.size/2)*(f.iogen.Asu-1))+f.iogen.Offset) * 4096
	f.iogen.Offset++
	f.iogen.Blocks--
	return offset, f.iogen.Isread
}
