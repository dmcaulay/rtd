package main

import (
	"bytes"
	"errors"
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

func query(dbName string, collection string, queryReader io.Reader) ([]byte, error) {
	db, err := getDb(dbName)
	if err != nil {
		return nil, err
	}

	queryMap, err := decodeJson(queryReader)
	if err != nil {
		return nil, err
	}

	id, ok := queryMap["_id"]
	if ok {
		return findDoc(dbName, collection, id.(string))
	}

	var docs []byte
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(collection))
		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			doc, err := decodeJson(v)
			if err != nil {
				return err
			}
			if queryMatch(doc, queryMap) {
				docs = append(docs, v...)
			}
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

func queryMatch(doc map[interface{}]interface{}, query map[interface{}]interface{}) bool {
	for k, queryV := range query {
		docV, ok := doc[k]
		if !ok {
			return false
		}
		if !valueMatch(docV, queryV) {
			return false
		}
	}
	return true
}

func valueMatch(docV interface{}, queryV interface{}) bool {
	// bool
	vBool, vOk := queryV.(bool)
	docBool, docOk := docV.(bool)
	if vOk && docOk {
		return vBool == docBool
	}

	// uint64
	vInt, vOk := queryV.(uint64)
	docInt, docOk := docV.(uint64)
	if vOk && docOk {
		return vInt == docInt
	}

	// float64
	vFloat, vOk := queryV.(float64)
	docFloat, docOk := docV.(float64)
	if vOk && docOk {
		return vFloat == docFloat
	}

	// string
	vStr, vOk := queryV.(string)
	docStr, docOk := docV.(string)
	if vOk && docOk {
		return vStr == docStr
	}

	// object
	vObj, vOk := queryV.(map[interface{}]interface{})
	docObj, docOk := docV.(map[interface{}]interface{})
	if vOk && docOk {
		return queryMatch(docObj, vObj)
	}

	// slice
	vSlice, vOk := queryV.([]interface{})
	docSlice, docOk := docV.([]interface{})
	if vOk && docOk {
		return sliceMatch(docSlice, vSlice)
	}
	if vOk {
		return sliceMatchValue(vSlice, docV, true)
	}
	if docOk {
		return sliceMatchValue(docSlice, queryV, false)
	}

	// not comparable
	return false
}

func sliceMatch(docSlice []interface{}, vSlice []interface{}) bool {
	if len(vSlice) != len(docSlice) {
		return false
	}
	for i, v := range vSlice {
		if !valueMatch(docSlice[i], v) {
			return false
		}
	}
	return true
}

func sliceMatchValue(slice []interface{}, value interface{}, sliceQuery bool) bool {
	for _, v := range slice {
		if sliceQuery {
			if valueMatch(value, v) {
				return true
			}
		} else {
			if valueMatch(v, value) {
				return true
			}
		}
	}
	return false
}

func decodeJson(data interface{}) (map[interface{}]interface{}, error) {
	var decoder *codec.Decoder
	switch data := data.(type) {
	default:
		return nil, errors.New("Unexpected type in decodeJson")
	case io.Reader:
		decoder = codec.NewDecoder(data, jh)
	case []byte:
		decoder = codec.NewDecoderBytes(data, jh)
	}

	var doc map[interface{}]interface{}
	err := decoder.Decode(&doc)
	return doc, err
}

func encodeDoc(doc map[interface{}]interface{}) (*bytes.Buffer, error) {
	encDoc := new(bytes.Buffer)
	err := codec.NewEncoder(encDoc, jh).Encode(doc)
	return encDoc, err
}
