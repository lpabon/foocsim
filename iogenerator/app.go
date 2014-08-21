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
	"fmt"
	"github.com/lpabon/foocsim/caches"
	"math/rand"
	"strconv"
)

type App struct {
	files            []*File
	r                *rand.Rand
	cache            caches.Caches
	pc               caches.Caches
	deletion_percent int
}

func NewApp(numfiles int,
	maxblocks uint64,
	randomfilesize bool,
	readp int,
	seed int64,
	deletion_percent int,
	pagecacheblocks uint64,
	cache caches.Caches) *App {

	app := &App{}
	app.files = make([]*File, numfiles)
	app.cache = cache
	app.deletion_percent = deletion_percent

	// Create random number for accessing files
	app.r = rand.New(rand.NewSource(seed))

	// Create page cache
	if pagecacheblocks != 0 {
		app.pc = caches.NewIoCache(pagecacheblocks, true /* writethrough */)
	} else {
		app.pc = caches.NewNullCache()
	}

	// Create files
	for file := 0; file < len(app.files); file++ {
		var size uint64
		if randomfilesize {
			size = uint64(app.r.Int63n(int64(maxblocks))) + uint64(1) // in case we get 0
		} else {
			size = maxblocks
		}
		app.files[file] = NewFile(size, readp)
	}

	return app
}

func (a *App) Gen() {
	file := a.r.Intn(len(a.files))
	block, isread := a.files[file].Gen()

	str_file := strconv.FormatInt(int64(file), 10)
	str_block := strconv.FormatUint(block, 10)

	// Check if we need to delete this file
	if rand.Intn(100) < (a.deletion_percent) {
		a.cache.Delete(str_file)
		return
	}

	// Which block on the file
	if isread {
		if !a.pc.Read(str_file, str_block) {
			a.cache.Read(str_file, str_block)
		}
	} else {
		a.pc.Write(str_file, str_block)
		a.cache.Write(str_file, str_block)
	}
}

func (a *App) String() string {

	return fmt.Sprint("== Page Cache ==\n") +
		fmt.Sprint(a.pc)
}
