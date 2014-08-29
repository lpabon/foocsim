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

package args

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestArgs(t *testing.T) {
	a := NewArgs()
	a.initialize()

	assert.Equal(t, a.maxfilesize, uint64(8*MB))
	assert.Equal(t, a.blocksize, a.blocksizekb*KB)
	assert.Equal(t, a.cacheblocks, uint64(a.cachesize*GB/a.blocksize))
	assert.Equal(t, a.maxfileblocks, a.maxfilesize*uint64(MB)/uint64(a.blocksize))
	assert.Equal(t, a.pagecacheblocks, uint64(a.pagecachesize*MB)/uint64(a.blocksize))
}
