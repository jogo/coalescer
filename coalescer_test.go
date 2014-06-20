package coalescer_test

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	. "github.com/boltdb/coalescer"
)

// Ensure that the coalescer groups together multiple updates.
func TestCoalescer_Update(t *testing.T) {
	db := open()
	defer closedb(db)
	c, err := New(db, 10, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("new: ", err)
	}

	// Create a bucket.
	go func() {
		err := c.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket([]byte("foo"))
			return err
		})
		if err != nil {
			t.Fatalf("coalesce update(1) failed: %s", err)
		}
	}()

	// Create a key/value in our bucket.
	go func() {
		time.Sleep(10 * time.Millisecond)
		err := c.Update(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte("foo")).Put([]byte("bar"), []byte("baz"))
		})
		if err != nil {
			t.Fatalf("coalesce update(2) failed: %s", err)
		}
	}()

	// Verify that our bucket was created.
	time.Sleep(100 * time.Millisecond)
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("foo"))
		if b == nil {
			t.Error("bucket not created")
		} else if v := b.Get([]byte("bar")); string(v) != "baz" {
			t.Errorf("invalid value: %#v", v)
		}
		return nil
	})
}

// open creates a new temporary Bolt database.
func open() *bolt.DB {
	db, err := bolt.Open(tempfile(), 0600)
	if err != nil {
		panic("open: " + err.Error())
	}
	return db
}

// closedb closes and deletes a ReportifyDB database.
func closedb(db *bolt.DB) {
	if db == nil {
		return
	}
	path := db.Path()
	db.Close()
	if path != "" {
		os.Remove(path)
	}
}

// tempfile returns the path to a non-existent temporary file.
func tempfile() string {
	f, _ := ioutil.TempFile("", "coalescer-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}
