package boltdbcache

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func setup(t *testing.T) (string, func()) {
	tempDir, err := ioutil.TempDir("", "httpcache")
	if err != nil {
		t.Fatal(err)
	}

	return tempDir, func() {
		os.RemoveAll(tempDir)
	}
}

func TestGetNoKey(t *testing.T) {
	tempDir, teardown := setup(t)
	defer teardown()

	cache, err := New(filepath.Join(tempDir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	key := "test"
	v, ok := cache.Get(key)
	if ok || v != nil {
		t.Fatal("retrieved key before adding it")
	}
}

func TestSet(t *testing.T) {
	tempDir, teardown := setup(t)
	defer teardown()

	cache, err := New(filepath.Join(tempDir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	k := "foo"
	v := []byte("bar")
	cache.Set(k, v)

	v2, ok := cache.Get(k)
	if !ok {
		t.Fatalf("could not retrieve value for key %q", k)
	}

	if !bytes.Equal(v, v2) {
		t.Fatalf("expected %q; got %q", v, v2)
	}
}

func TestDelete(t *testing.T) {
	tempDir, teardown := setup(t)
	defer teardown()

	cache, err := New(filepath.Join(tempDir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	k := "foo"
	v := []byte("bar")
	cache.Set(k, v)

	v2, ok := cache.Get(k)
	if !ok {
		t.Fatalf("could not retrieve value for key %q", k)
	}

	if !bytes.Equal(v, v2) {
		t.Fatalf("expected %q; got %q", v, v2)
	}

	cache.Delete(k)

	v3, ok := cache.Get(k)
	if ok || v3 != nil {
		t.Fatalf("key still present")
	}
}

func TestNilBucketName(t *testing.T) {
	tempDir, teardown := setup(t)
	defer teardown()

	_, err := New(filepath.Join(tempDir, "db"), WithBucketName(""))
	if err != bolt.ErrBucketNameRequired {
		t.Fatalf("expected bolt.ErrBucketNameRequired; got %v", err)
	}
}
