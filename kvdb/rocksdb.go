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
	"github.com/DanielMorsing/rocksdb"
	"github.com/lpabon/godbc"
	"os"
)

type KVRocksDB struct {
	db *rocksdb.DB
	ro *rocksdb.ReadOptions
	wo *rocksdb.WriteOptions
}

func NewKVRocksDB(dbpath string, blocks, bcsize uint64, blocksize uint32) *KVRocksDB {

	var err error

	db := &KVRocksDB{}

	os.RemoveAll(dbpath)

	// Set Options
	opts := rocksdb.NewOptions()
	opts.SetCache(rocksdb.NewLRUCache(int(bcsize)))
	opts.SetCreateIfMissing(true)

	db.db, err = rocksdb.Open(dbpath, opts)
	godbc.Check(err == nil)

	// Set read and write options
	db.ro = rocksdb.NewReadOptions()
	db.wo = rocksdb.NewWriteOptions()

	godbc.Ensure(db.ro != nil)
	godbc.Ensure(db.wo != nil)

	return db
}

func (c *KVRocksDB) Close() {
	c.wo.Close()
	c.ro.Close()
	c.db.Close()
}

func (c *KVRocksDB) Put(key, val []byte, index uint64) error {
	return c.db.Put(c.wo, key, val)
}

func (c *KVRocksDB) Get(key, val []byte, index uint64) error {
	buf, err := c.db.Get(c.ro, key)
	if err == nil {
		copy(val, buf)
	}
	return err
}

func (c *KVRocksDB) Delete(key []byte, index uint64) error {
	return c.db.Delete(c.wo, key)
}

func (c *KVRocksDB) String() string {
	return ""
}
