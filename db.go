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

type (
	BucketHandler   func(*bolt.Bucket) error
	QueryHandler    func(*bolt.Bucket, []byte, []byte, map[interface{}]interface{}) error
	TransactionFunc func(string, string, BucketHandler) error
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

func insertDoc(db string, collection string, docReader io.Reader) (*bytes.Buffer, error) {
	doc, err := decodeJson(docReader)
	if err != nil {
		return nil, err
	}

	id, ok := doc["_id"]
	var lookupId *bytes.Buffer
	if !ok {
		id, lookupId, err = newId()
		if err != nil {
			return nil, err
		}
		doc["_id"] = id
	} else {
		id, ok := id.(string)
		if !ok {
			return nil, errors.New("ID must be a string UUID")
		}
		lookupId, err = parseId(id)
		if err != nil {
			return nil, err
		}
	}

	encDoc, err := encodeDoc(doc)
	if err != nil {
		return nil, err
	}

	err = updateCollection(db, collection, func(bucket *bolt.Bucket) error {
		return bucket.Put(lookupId.Bytes(), encDoc.Bytes())
	})
	return encDoc, err
}

func updateDoc(db string, collection string, id string, updateReader interface{}) ([]byte, error) {
	update, err := decodeJson(updateReader)
	if err != nil {
		return nil, err
	}

	lookupId, err := parseId(id)
	if err != nil {
		return nil, err
	}

	var encDoc *bytes.Buffer
	err = updateCollection(db, collection, func(bucket *bolt.Bucket) error {
		originalDoc := bucket.Get(lookupId.Bytes())
		encDoc, err = updateDocValue(originalDoc, update, bucket)
		if err != nil {
			return err
		}
		return bucket.Put(lookupId.Bytes(), encDoc.Bytes())
	})
	return encDoc.Bytes(), err
}

func query(db string, collection string, queryReader io.Reader) ([]byte, error) {
	queryMap, err := decodeJson(queryReader)
	if err != nil {
		return nil, err
	}

	id, ok := queryMap["_id"]
	if ok {
		return findDoc(db, collection, id.(string))
	}

	var docs []byte
	err = iterateQuery(db, collection, queryMap, readCollection, func(bucket *bolt.Bucket, key []byte, value []byte, doc map[interface{}]interface{}) error {
		docs = append(docs, value...)
		return nil
	})
	return docs, err
}

func updateQuery(db string, collection string, queryReader io.Reader) ([]byte, error) {
	updateMap, err := decodeJson(queryReader)
	if err != nil {
		return nil, err
	}

	queryMap, ok := updateMap["query"].(map[interface{}]interface{})
	if !ok {
		return nil, errors.New("Cannot update without a query")
	}

	update, ok := updateMap["update"].(map[interface{}]interface{})
	if !ok {
		return nil, errors.New("Cannot update without an update object")
	}

	id, ok := queryMap["_id"]
	if ok {
		return updateDoc(db, collection, id.(string), update)
	}

	var docs []byte
	err = iterateQuery(db, collection, queryMap, updateCollection, func(bucket *bolt.Bucket, key []byte, value []byte, doc map[interface{}]interface{}) error {
		updated, err := updateDocValue(value, update, bucket)
		if err != nil {
			return err
		}

		err = bucket.Put(key, updated.Bytes())
		if err != nil {
			return err
		}
		docs = append(docs, updated.Bytes()...)
		return nil
	})
	return docs, err
}

func findDoc(db string, collection string, id string) ([]byte, error) {
	lookupId, err := parseId(id)
	if err != nil {
		return nil, err
	}

	var doc []byte
	err = readCollection(db, collection, func(bucket *bolt.Bucket) error {
		doc = bucket.Get(lookupId.Bytes())
		return nil
	})
	return doc, err
}

func updateDocValue(originalDoc []byte, update map[interface{}]interface{}, bucket *bolt.Bucket) (*bytes.Buffer, error) {
	doc, err := decodeJson(originalDoc)
	if err != nil {
		return nil, err
	}

	for k, v := range update {
		if k == "_id" {
			return nil, errors.New("Can't update ID on update")
		}
		doc[k] = v
	}

	encDoc, err := encodeDoc(doc)
	if err != nil {
		return nil, err
	}

	return encDoc, err
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

func updateCollection(dbName string, collection string, handler BucketHandler) error {
	db, err := getDb(dbName)
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(collection))
		if err != nil {
			return err
		}
		return handler(bucket)
	})
}

func readCollection(dbName string, collection string, handler BucketHandler) error {
	db, err := getDb(dbName)
	if err != nil {
		return err
	}

	return db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(collection))
		return handler(bucket)
	})
}

func iterateQuery(db string, collection string, query map[interface{}]interface{}, tx TransactionFunc, handler QueryHandler) error {
	return tx(db, collection, func(bucket *bolt.Bucket) error {
		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			doc, err := decodeJson(v)
			if err != nil {
				return err
			}
			if queryMatch(doc, query) {
				err = handler(bucket, k, v, doc)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
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
	case map[interface{}]interface{}:
		return data, nil
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
