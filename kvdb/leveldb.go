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
	"github.com/jmhodges/levigo"
	"github.com/lpabon/godbc"
	"os"
)

type KVLevelDB struct {
	db *levigo.DB
	ro *levigo.ReadOptions
	wo *levigo.WriteOptions
}

func NewKVLevelDB(dbpath string) *KVLevelDB {

	var err error

	db := &KVLevelDB{}

	os.RemoveAll(dbpath)

	// Set Options
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)

	db.db, err = levigo.Open(dbpath, opts)
	godbc.Check(err == nil)

	// Set read and write options
	db.ro = levigo.NewReadOptions()
	db.wo = levigo.NewWriteOptions()

	godbc.Ensure(db.ro != nil)
	godbc.Ensure(db.wo != nil)

	return db
}

func (c *KVLevelDB) Close() {
	c.wo.Close()
	c.ro.Close()
	c.db.Close()
}

func (c *KVLevelDB) Put(key, val []byte, index uint64) error {
	return c.db.Put(c.wo, key, val)
}

func (c *KVLevelDB) Get(key []byte, index uint64) ([]byte, error) {
	return c.db.Get(c.ro, key)
}

func (c *KVLevelDB) Delete(key []byte, index uint64) error {
	return c.db.Delete(c.wo, key)
}

func (c *KVLevelDB) String() string {
	return ""
}
