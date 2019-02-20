package boltdbcache

import (
	bolt "go.etcd.io/bbolt"
)

const defaultBucketName = "httpcache"

// Cache is an implementation of httpcache.Cache that uses a bolt database.
type Cache struct {
	db  *bolt.DB
	bkt string
}

// An Option is a function that applies an option to a Cache.
type Option func(*Cache)

// WithBucketName is a functional option that sets the bucket name to use for
// this cache.
func WithBucketName(name string) Option {
	return func(c *Cache) {
		c.bkt = name
	}
}

// New returns a new Cache that uses a bolt database at the given path.
func New(path string, opts ...Option) (*Cache, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	return NewWithDB(db, opts...)
}

// NewWithDB returns a new Cache using the provided (and opened) bolt database.
func NewWithDB(db *bolt.DB, opts ...Option) (*Cache, error) {
	cache := &Cache{
		db:  db,
		bkt: defaultBucketName,
	}

	for _, opt := range opts {
		opt(cache)
	}

	init := func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(cache.bkt))
		return err
	}

	if err := cache.db.Update(init); err != nil {
		if err := cache.db.Close(); err != nil {
			panic(err)
		}

		return nil, err
	}

	return cache, nil
}

// Close closes the underlying boltdb database.
func (c *Cache) Close() error {
	if c != nil {
		return c.db.Close()
	}

	return nil
}

// Get retrieves the response corresponding to the given key if present.
func (c *Cache) Get(key string) (resp []byte, ok bool) {
	get := func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(c.bkt))
		if bkt == nil {
			panic("nil bucket")
		}

		resp = bkt.Get([]byte(key))

		return nil
	}

	if err := c.db.View(get); err != nil {
		return resp, false
	}

	return resp, resp != nil
}

// Set stores a response to the cache at the given key.
func (c *Cache) Set(key string, resp []byte) {
	set := func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(c.bkt))
		if bkt == nil {
			panic("nil bucket")
		}

		return bkt.Put([]byte(key), resp)
	}

	if err := c.db.Update(set); err != nil {
		panic(err)
	}
}

// Delete removes the response with the given key from the cache.
func (c *Cache) Delete(key string) {
	del := func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(c.bkt))
		if bkt == nil {
			panic("nil bucket")
		}

		return bkt.Delete([]byte(key))
	}

	if err := c.db.Update(del); err != nil {
		panic(err)
	}
}
