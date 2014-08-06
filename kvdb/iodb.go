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

package kvdb

import (
	"fmt"
	"github.com/lpabon/bufferio"
	"github.com/lpabon/godbc"
	"os"
	"syscall"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
	TB = 1024 * GB
)

type IoSegment struct {
	max          uint64
	size         uint64
	metadatasize uint64
	datasize     uint64
}

type IoEntryKey struct {
	key string
}

type KVIoDB struct {
	size       uint64
	blocksize  uint64
	segment    IoSegment
	current    uint64
	entry      uint64
	maxentries uint64
	segmentbuf []byte
	data       *bufferio.BufferIO
	meta       *bufferio.BufferIO
	fp         *os.File
}

func NewKVIoDB(dbpath string, blocks uint64, blocksize uint32) *KVIoDB {

	var err error

	db := &KVIoDB{}
	db.size = blocks * uint64(blocksize)
	db.blocksize = uint64(blocksize)
	db.segment.metadatasize = 4 * KB
	db.segment.datasize = 4 * MB
	db.maxentries = db.segment.datasize / db.blocksize
	db.segment.size = db.segment.metadatasize + db.segment.datasize
	db.segment.max = db.size / uint64(db.segment.size)

	db.segmentbuf = make([]byte, db.segment.size)
	db.data = bufferio.NewBufferIO(db.segmentbuf[:db.segment.datasize])
	db.meta = bufferio.NewBufferIO(db.segmentbuf[db.segment.datasize:])

	db.fp, err = os.OpenFile(dbpath, syscall.O_DIRECT|os.O_CREATE|os.O_RDWR, os.ModePerm)
	godbc.Check(err == nil)

	return db
}

func (c *KVIoDB) sync() {
	c.fp.WriteAt(c.segmentbuf, int64(c.current))
	c.current += c.segment.size
	c.current = c.current % c.size
	c.data.Reset()
	c.meta.Reset()
}

func (c *KVIoDB) Close() {
	c.sync()
	c.fp.Close()
}

func (c *KVIoDB) Put(key, val []byte, index uint64) error {

	var keyentry IoEntryKey

	c.data.Write(val)
	keyentry.key = string(key)
	c.meta.WriteDataLE(keyentry)
	c.entry++

	if c.entry > c.maxentries {
		c.sync()
	}

	return nil
}

func (c *KVIoDB) Get(key []byte, index uint64) ([]byte, error) {

	buf := make([]byte, c.blocksize)
	offset := (index*c.blocksize + (index/c.maxentries)*c.segment.metadatasize)
	n, err := c.fp.ReadAt(buf, int64(offset))
	godbc.Check(uint64(n) == c.blocksize,
		fmt.Sprintf("Read %v expected %v from location %v",
			n, c.blocksize, offset))
	godbc.Check(err == nil)

	return buf, nil
}

func (c *KVIoDB) Delete(key []byte, index uint64) error {
	// nothing to do
	return nil
}
