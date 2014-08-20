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
	"math/rand"
)

type App struct {
	files []*File
	r     *rand.Rand
}

func NewApp(numfiles int, maxblocks uint64, randomfilesize bool, readp int, seed int64) {
	app := &App{}
	app.files = make([]*File, numfiles)

	app.r = rand.New(rand.NewSource(seed))

	for file := 0; file < len(app.files); file++ {
		var size uint64
		if randomfilesize {
			size = uint64(app.r.Int63n(maxblocks)) + uint64(1) // in case we get 0
		} else {
			size = uint64(maxfilesize)
		}
		app.files[file] = NewFile(size, readp)
	}
}

func (a *App) Gen() (file, block uint64, read bool) {
	file = a.r.Intn(len(a.files))
	block, read = a.files[file].Gen()
	return
}
