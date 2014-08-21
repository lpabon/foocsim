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
	"github.com/lpabon/foocsim/zipfworkload"
)

type File struct {
	iogen *zipfworkload.ZipfWorkload
	size  uint64
}

func NewFile(size uint64, readp int) *File {
	f := &File{}
	f.iogen = zipfworkload.NewZipfWorkload(size, readp)
	f.size = size

	return f
}

func (f *File) Gen() (uint64, bool) {
	return f.iogen.ZipfGenerate()
}
