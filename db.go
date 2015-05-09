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

	query, err := decodeJson(queryReader)
	if err != nil {
		return nil, err
	}

	id, ok := query["_id"]
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
			if queryMatch(doc, query) {
				docs = append(docs, v...)
			} else if err != nil {
				return err
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

func queryMatch(doc map[string]interface{}, query map[string]interface{}) bool {
	for k, v := range query {
		docV, ok := doc[k]
		if !ok {
			return false
		}

		// bool
		vBool, vOk := v.(bool)
		docBool, docOk := docV.(bool)
		if vOk && docOk {
			if vBool == docBool {
				continue
			}
			return false
		}

		// uint64
		vInt, vOk := v.(uint64)
		docInt, docOk := docV.(uint64)
		if vOk && docOk {
			if vInt == docInt {
				continue
			}
			return false
		}

		// float64
		vFloat, vOk := v.(float64)
		docFloat, docOk := docV.(float64)
		if vOk && docOk {
			if vFloat == docFloat {
				continue
			}
			return false
		}

		// string
		vStr, vOk := v.(string)
		docStr, docOk := docV.(string)
		if vOk && docOk {
			if vStr == docStr {
				continue
			}
			return false
		}

		// nested
		vObj, vOk := v.(map[string]interface{})
		docObj, docOk := docV.(map[string]interface{})
		if vOk && docOk {
			fmt.Println("nested!")
			if queryMatch(docObj, vObj) {
				continue
			}
			return false
		}

		return false
	}
	return true
	// switch values := [2]interface{}{v, docV}.(type) {
	// default:
	// 	return false
	// case [2]map[string]interface{}:
	// 	eq, err := queryMatch(values[0], values[1])
	// 	if err != nil || !eq {
	// 		return eq, err
	// 	}
	// case [2]bool:
	// 	if values[0] != values[1] {
	// 		return false
	// 	}
	// case [2]float64:
	// 	if values[0] != values[1] {
	// 		return false
	// 	}
	// case [2]string:
	// 	if values[0] != values[1] {
	// 		return false
	// 	}
	// }
}

func decodeJson(data interface{}) (map[string]interface{}, error) {
	var decoder *codec.Decoder
	switch data := data.(type) {
	default:
		return nil, errors.New("Unexpected type in decodeJson")
	case io.Reader:
		decoder = codec.NewDecoder(data, jh)
	case []byte:
		decoder = codec.NewDecoderBytes(data, jh)
	}

	var doc map[string]interface{}
	err := decoder.Decode(&doc)
	return doc, err
}

func encodeDoc(doc map[string]interface{}) (*bytes.Buffer, error) {
	encDoc := new(bytes.Buffer)
	err := codec.NewEncoder(encDoc, jh).Encode(doc)
	return encDoc, err
}
