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

type IoSegmentInfo struct {
	size         uint64
	metadatasize uint64
	datasize     uint64
}

type IoSegment struct {
	segmentbuf []byte
	data       *bufferio.BufferIO
	meta       *bufferio.BufferIO
	offset     uint64
}

type IoEntryKey struct {
	key string
}

type KVIoDB struct {
	size           uint64
	blocksize      uint64
	segmentinfo    IoSegmentInfo
	segments       []IoSegment
	segment        int
	chwriting      chan int
	chavailable    chan int
	chquit         chan int
	segmentbuffers int
	current        uint64
	entry          uint64
	numsegments    uint64
	maxentries     uint64
	fp             *os.File
}

func NewKVIoDB(dbpath string, blocks uint64, blocksize uint32) *KVIoDB {

	var err error

	db := &KVIoDB{}
	db.blocksize = uint64(blocksize)
	db.segmentinfo.metadatasize = 4 * KB
	db.segmentinfo.datasize = 1 * MB
	db.segmentbuffers = 32
	db.maxentries = db.segmentinfo.datasize / db.blocksize
	db.segmentinfo.size = db.segmentinfo.metadatasize + db.segmentinfo.datasize
	db.numsegments = blocks / db.maxentries
	db.size = db.numsegments * db.segmentinfo.size

	db.segments = make([]IoSegment, db.segmentbuffers)
	db.chwriting = make(chan int, db.segmentbuffers)
	db.chavailable = make(chan int, db.segmentbuffers)
	db.chquit = make(chan int)
	db.segment = 0
	for i := 0; i < db.segmentbuffers; i++ {
		db.segments[i].segmentbuf = make([]byte, db.segmentinfo.size)
		db.segments[i].data = bufferio.NewBufferIO(db.segments[i].segmentbuf[:db.segmentinfo.datasize])
		db.segments[i].meta = bufferio.NewBufferIO(db.segments[i].segmentbuf[db.segmentinfo.datasize:])

		// Fill ch available with all the available buffers
		db.chavailable <- i
	}

	os.Remove(dbpath)
	db.fp, err = os.OpenFile(dbpath, syscall.O_DIRECT|os.O_CREATE|os.O_RDWR, os.ModePerm)
	godbc.Check(err == nil)

	// Start writer thread
	db.writer()

	return db
}

func (c *KVIoDB) writer() {

	go func() {
		for i := range c.chwriting {
			n, err := c.fp.WriteAt(c.segments[i].segmentbuf, int64(c.segments[i].offset))
			godbc.Check(n == len(c.segments[i].segmentbuf))
			godbc.Check(err == nil)
			c.chavailable <- i
		}
		close(c.chquit)
	}()

}

func (c *KVIoDB) sync() {
	// Send to writer
	c.chwriting <- c.segment

	// Get a new available buffer
	c.segment = <-c.chavailable

	// Reset the bufferIO managers
	c.segments[c.segment].data.Reset()
	c.segments[c.segment].meta.Reset()

	// Move to the next offset
	c.current += c.segmentinfo.size
	c.current = c.current % c.size
	c.segments[c.segment].offset = c.current

	// Reset the number of entries in the segment
	c.entry = 0

}

func (c *KVIoDB) Close() {
	c.sync()
	close(c.chwriting)
	<-c.chquit
	c.fp.Close()
}

func (c *KVIoDB) Put(key, val []byte, index uint64) error {

	var keyentry IoEntryKey

	if c.entry >= c.maxentries {
		c.sync()
	}

	n, err := c.segments[c.segment].data.Write(val)
	godbc.Check(n == len(val))
	godbc.Check(err == nil)

	keyentry.key = string(key)
	c.segments[c.segment].meta.WriteDataLE(keyentry)
	c.entry++

	fmt.Printf("%v_", index)
	return nil
}

func (c *KVIoDB) Get(key []byte, index uint64) ([]byte, error) {

	var n int
	var err error

	buf := make([]byte, c.blocksize)
	offset := (index*c.blocksize + (index/c.maxentries)*c.segmentinfo.metadatasize)

	// Check if the data is in RAM.  Go through each buffered segment
	for i := 0; i < c.segmentbuffers; i++ {

		if (offset >= c.segments[i].offset) &&
			(offset < (c.segments[i].offset + c.segmentinfo.datasize)) {

			n, err = c.segments[i].data.ReadAt(buf, int64(offset-c.segments[i].offset))

			fmt.Printf("+RAM\n")
			godbc.Check(uint64(n) == c.blocksize,
				fmt.Sprintf("Read %v expected:%v from location:%v index:%v current:%v",
					n, c.blocksize, offset, index, c.current))
			godbc.Check(err == nil)

			return buf, nil
		}
	}

	// Read from storage
	n, err = c.fp.ReadAt(buf, int64(offset))
	godbc.Check(uint64(n) == c.blocksize,
		fmt.Sprintf("Read %v expected %v from location %v index %v current:%v",
			n, c.blocksize, offset, index, c.current))
	godbc.Check(err == nil)

	return buf, nil
}

func (c *KVIoDB) Delete(key []byte, index uint64) error {
	// nothing to do
	return nil
}
