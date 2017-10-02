package boltdbcache

import (
	"errors"
	"log"

	bolt "github.com/coreos/bbolt"
)

const bktName = "httpcache"

// Cache is an implementation of httpcache.Cache that uses a bolt database.
type Cache struct {
	db *bolt.DB
}

// New returns a new Cache that uses a bolt database at the given path.
func New(path string) (*Cache, error) {
	cache := &Cache{}

	var err error
	cache.db, err = bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	init := func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bktName))
		return err
	}

	if err := cache.db.Update(init); err != nil {
		log.Printf("boltdbcache.New(): init error: %v", err)

		if err := cache.db.Close(); err != nil {
			log.Printf("boltdbcache.New(): close error: %v", err)
		}

		return nil, err
	}

	return cache, nil
}

// NewWithDB returns a new Cache using the provided (and opened) bolt database.
func NewWithDB(db *bolt.DB) *Cache {
	return &Cache{db: db}
}

// Close closes the underlying boltdb database.
func (c *Cache) Close() error {
	return c.db.Close()
}

// Get retrieves the response corresponding to the given key if present.
func (c *Cache) Get(key string) (resp []byte, ok bool) {
	get := func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bktName))
		if bkt == nil {
			return errors.New("bucket is nil")
		}

		resp = bkt.Get([]byte(key))

		return nil
	}

	if err := c.db.View(get); err != nil {
		log.Printf("boltdbcache.Get(): view error: %v", err)
		return resp, false
	}

	return resp, resp != nil
}

// Set stores a response to the cache at the given key.
func (c *Cache) Set(key string, resp []byte) {
	set := func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bktName))
		if bkt == nil {
			return errors.New("bucket is nil")
		}

		return bkt.Put([]byte(key), resp)
	}

	if err := c.db.Update(set); err != nil {
		log.Printf("boltdbcache.Set(): update error: %v", err)
	}
}

// Delete removes the response with the given key from the cache.
func (c *Cache) Delete(key string) {
	del := func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bktName))
		if bkt == nil {
			return errors.New("bucket is nil")
		}

		return bkt.Delete([]byte(key))
	}

	if err := c.db.Update(del); err != nil {
		log.Printf("boltdbcache.Delete(): update error: %v", err)
	}
}
