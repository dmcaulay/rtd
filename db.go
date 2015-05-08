package main

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/boltdb/bolt"
	"github.com/ugorji/go/codec"
)

var jh codec.Handle = new(codec.JsonHandle)

func dbFileName(name string) string {
	return fmt.Sprintf("%s/%s.db", rootDir, name)
}

func getDb(name string) (*bolt.DB, error) {
	if db, ok := dbs[name]; ok {
		return db, nil
	}

	db, err := bolt.Open(dbFileName(name), 0600, nil)
	if err != nil {
		return nil, err
	}
	dbs[name] = db
	return db, nil
}

func deleteDb(name string) error {
	db, err := getDb(name)
	if err != nil {
		return err
	}
	db.Close()
	return os.Remove(dbFileName(name))
}

func insertDoc(dbName string, collection string, docReader io.Reader) (*bytes.Buffer, error) {
	db, err := getDb(dbName)
	if err != nil {
		return nil, err
	}

	doc, err := decodeJson(docReader)
	if err != nil {
		return nil, err
	}

	id, lookupId, err := newId()
	if err != nil {
		return nil, err
	}
	doc["_id"] = id

	encDoc, err := encodeDoc(doc)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(collection))
		if err != nil {
			return err
		}
		return bucket.Put(lookupId.Bytes(), encDoc.Bytes())
	})
	return encDoc, err
}

func query(dbName string, collection string) ([]byte, error) {
	db, err := getDb(dbName)
	if err != nil {
		return nil, err
	}

	var docs []byte
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(collection))
		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			docs = append(docs, v...)
		}
		return nil
	})
	return docs, err
}

func findDoc(dbName string, collection string, id string) ([]byte, error) {
	db, err := getDb(dbName)
	if err != nil {
		return nil, err
	}

	lookupId, err := parseId(id)
	if err != nil {
		return nil, err
	}

	var doc []byte
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(collection))
		doc = bucket.Get(lookupId.Bytes())
		return nil
	})
	return doc, err
}

func decodeJson(reader io.Reader) (map[string]interface{}, error) {
	var doc map[string]interface{}
	err := codec.NewDecoder(reader, jh).Decode(&doc)
	if err != nil {
		return nil, err
	}
	return doc, nil
}

func encodeDoc(doc map[string]interface{}) (*bytes.Buffer, error) {
	encDoc := new(bytes.Buffer)
	err := codec.NewEncoder(encDoc, jh).Encode(doc)
	return encDoc, err
}
