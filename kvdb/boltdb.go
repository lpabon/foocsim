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
	"github.com/boltdb/bolt"
	"github.com/lpabon/godbc"
	"os"
)

type KVBoltDB struct {
	db *bolt.DB
}

func NewKVBoltDB(dbpath string) *KVBoltDB {

	var err error

	db := &KVBoltDB{}

	os.Remove(dbpath)
	db.db, err = bolt.Open(dbpath, 0600, nil)
	godbc.Check(err == nil)

	err = db.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("cache"))
		godbc.Check(err == nil)
		return nil
	})
	godbc.Check(err == nil)

	return db
}

func (c *KVBoltDB) Close() {
	c.db.Close()
}

func (c *KVBoltDB) Put(key, val []byte, index uint64) (err error) {
	err = c.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("cache")).Put(key, val)
	})
	return
}

func (c *KVBoltDB) Get(key []byte, index uint64) (val []byte, err error) {
	err = c.db.View(func(tx *bolt.Tx) error {
		val = tx.Bucket([]byte("cache")).Get(key)
		return nil
	})
	return
}

func (c *KVBoltDB) Delete(key []byte, index uint64) (err error) {
	err = c.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("cache")).Delete(key)
	})
	return
}

func (c *KVBoltDB) String() string {
	return ""
}
