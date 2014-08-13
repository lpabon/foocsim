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
	"time"
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
	written    bool
}

type IoStatDuration struct {
	duration int64
	counter  int64
}

func (d *IoStatDuration) Add(delta time.Duration) {
	d.duration += delta.Nanoseconds()
	d.counter++
}

func (d *IoStatDuration) MeanTimeUsecs() float64 {
	if d.counter == 0 {
		return 0.0
	}
	return (float64(d.duration) / float64(d.counter)) / 1000.0
}

func (d *IoStatDuration) String() string {
	return fmt.Sprintf("duration = %v\n"+
		"counter = %v\n",
		d.duration,
		d.counter)
}

type IoStats struct {
	ramhits         uint64
	storagehits     uint64
	wraps           uint64
	seg_skipped     uint64
	readtime        *IoStatDuration
	segmentreadtime *IoStatDuration
	writetime       *IoStatDuration
}

func NewIoStats() *IoStats {

	stats := &IoStats{}
	stats.readtime = &IoStatDuration{}
	stats.segmentreadtime = &IoStatDuration{}
	stats.writetime = &IoStatDuration{}

	return stats

}

func (s *IoStats) Close() {

}

func (s *IoStats) SegmentSkipped() {
	s.seg_skipped++
}

func (s *IoStats) RamHit() {
	s.ramhits++
}

func (s *IoStats) StorageHit() {
	s.storagehits++
}

func (s *IoStats) Wrapped() {
	s.wraps++
}

func (s *IoStats) ReadTimeRecord(d time.Duration) {
	s.readtime.Add(d)
}

func (s *IoStats) WriteTimeRecord(d time.Duration) {
	s.writetime.Add(d)
}

func (s *IoStats) SegmentReadTimeRecord(d time.Duration) {
	s.segmentreadtime.Add(d)
}

func (s *IoStats) RamHitRate() float64 {
	hits := s.ramhits + s.storagehits
	if 0 == hits {
		return 0.0
	} else {
		return float64(s.ramhits) / float64(hits)
	}
}

func (s *IoStats) String() string {
	return fmt.Sprintf("Ram Hit Rate: %.4f\n"+
		"Ram Hits: %v\n"+
		"Storage Hits: %v\n"+
		"Wraps: %v\n"+
		"Segments Skipped: %v\n"+
		"Mean Read Latency: %.2f usec\n"+
		"Mean Segment Read Latency: %.2f usec\n"+
		"Mean Write Latency: %.2f usec\n",
		s.RamHitRate(),
		s.ramhits,
		s.storagehits,
		s.wraps,
		s.seg_skipped,
		s.readtime.MeanTimeUsecs(),
		s.segmentreadtime.MeanTimeUsecs(),
		s.writetime.MeanTimeUsecs()) // + s.readtime.String() + s.writetime.String()
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
	numsegments    uint64
	maxentries     uint64
	fp             *os.File
	wrapped        bool
	stats          *IoStats
}

func NewKVIoDB(dbpath string, blocks uint64, blocksize uint32) *KVIoDB {

	var err error

	db := &KVIoDB{}
	db.stats = NewIoStats()
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
	for i := 0; i < db.segmentbuffers; i++ {
		db.segments[i].segmentbuf = make([]byte, db.segmentinfo.size)
		db.segments[i].data = bufferio.NewBufferIO(db.segments[i].segmentbuf[:db.segmentinfo.datasize])
		db.segments[i].meta = bufferio.NewBufferIO(db.segments[i].segmentbuf[db.segmentinfo.datasize:])

		// Fill ch available with all the available buffers
		db.chavailable <- i
	}
	db.segment = <-db.chavailable

	os.Remove(dbpath)
	db.fp, err = os.OpenFile(dbpath, syscall.O_DIRECT|os.O_CREATE|os.O_RDWR, os.ModePerm)
	godbc.Check(err == nil)

	// Start writer thread
	db.writer()

	godbc.Ensure(db.blocksize == uint64(blocksize))
	godbc.Ensure(db.chwriting != nil)
	godbc.Ensure(db.chavailable != nil)
	godbc.Ensure(db.chquit != nil)
	godbc.Ensure(db.segmentbuffers == len(db.segments))
	godbc.Ensure((db.segmentbuffers - 1) == len(db.chavailable))
	godbc.Ensure(0 == len(db.chquit))
	godbc.Ensure(0 == len(db.chwriting))
	godbc.Ensure(0 == db.segment)

	return db
}

func (c *KVIoDB) writer() {

	go func() {
		for i := range c.chwriting {
			if c.segments[i].written {
				start := time.Now()
				n, err := c.fp.WriteAt(c.segments[i].segmentbuf, int64(c.segments[i].offset))
				end := time.Now()
				c.stats.WriteTimeRecord(end.Sub(start))
				godbc.Check(n == len(c.segments[i].segmentbuf))
				godbc.Check(err == nil)
				c.segments[i].written = false
			} else {
				c.stats.SegmentSkipped()
			}
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
	godbc.Check(c.segment < c.segmentbuffers)

	// Reset the bufferIO managers
	c.segments[c.segment].data.Reset()
	c.segments[c.segment].meta.Reset()

	// Move to the next offset
	c.current += c.segmentinfo.size
	c.current = c.current % c.size
	if 0 == c.current {
		c.stats.Wrapped()
		c.wrapped = true
	}
	c.segments[c.segment].offset = c.current

	// Ok, we have wrapped around.  We need to read the data
	// on the storage device back into the segment.  That way
	// we will only write the indexes that have been evicted.
	// Also, if non are written, we have atleast preloaded it into
	// memory for read hits.
	if c.wrapped {
		start := time.Now()
		n, err := c.fp.ReadAt(c.segments[c.segment].segmentbuf, int64(c.segments[c.segment].offset))
		end := time.Now()
		c.stats.SegmentReadTimeRecord(end.Sub(start))
		godbc.Check(n == len(c.segments[c.segment].segmentbuf))
		godbc.Check(err == nil)
	}

}

func (c *KVIoDB) Close() {
	c.sync()
	close(c.chwriting)
	<-c.chquit
	c.fp.Close()
	fmt.Print("== IoDB Stats ==\n" + c.stats.String())
}

func (c *KVIoDB) offset(index uint64) uint64 {
	return (index*c.blocksize + (index/c.maxentries)*c.segmentinfo.metadatasize)
}

func (c *KVIoDB) inRange(index uint64, segment int) bool {
	offset := c.offset(index)

	return ((offset >= c.segments[segment].offset) &&
		(offset < (c.segments[segment].offset + c.segmentinfo.datasize)))
}

func (c *KVIoDB) Put(key, val []byte, index uint64) error {

	for !c.inRange(index, c.segment) {
		c.sync()
	}

	offset := c.offset(index)

	godbc.Require(c.inRange(index, c.segment),
		fmt.Sprintf("[%v - %v - %v]",
			c.segments[c.segment].offset,
			offset,
			c.segments[c.segment].offset+c.segmentinfo.datasize))

	n, err := c.segments[c.segment].data.WriteAt(val, int64(offset-c.segments[c.segment].offset))
	godbc.Check(n == len(val))
	godbc.Check(err == nil)

	c.segments[c.segment].written = true
	c.segments[c.segment].meta.Write([]byte(key))

	return nil
}

func (c *KVIoDB) Get(key []byte, index uint64) ([]byte, error) {

	var n int
	var err error

	buf := make([]byte, c.blocksize)
	offset := c.offset(index)

	// Check if the data is in RAM.  Go through each buffered segment
	for i := 0; i < c.segmentbuffers; i++ {

		if (offset >= c.segments[i].offset) &&
			(offset < (c.segments[i].offset + c.segmentinfo.datasize)) {

			n, err = c.segments[i].data.ReadAt(buf, int64(offset-c.segments[i].offset))

			godbc.Check(uint64(n) == c.blocksize,
				fmt.Sprintf("Read %v expected:%v from location:%v index:%v current:%v",
					n, c.blocksize, offset, index, c.current))
			godbc.Check(err == nil)
			c.stats.RamHit()

			return buf, nil
		}
	}

	// Read from storage
	start := time.Now()
	n, err = c.fp.ReadAt(buf, int64(offset))
	end := time.Now()
	c.stats.ReadTimeRecord(end.Sub(start))

	godbc.Check(uint64(n) == c.blocksize,
		fmt.Sprintf("Read %v expected %v from location %v index %v current:%v",
			n, c.blocksize, offset, index, c.current))
	godbc.Check(err == nil)
	c.stats.StorageHit()

	return buf, nil
}

func (c *KVIoDB) Delete(key []byte, index uint64) error {
	// nothing to do
	return nil
}

func (c *KVIoDB) String() string {
	return ""
}
