package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"code.google.com/p/go-uuid/uuid"
	"github.com/boltdb/bolt"
	"github.com/labstack/echo"
	"github.com/ugorji/go/codec"
)

var dbs map[string]*bolt.DB = make(map[string]*bolt.DB)
var rootDir string
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
		return db, err
	}
	dbs[name] = db
	return db, nil
}

func badRequest(c *echo.Context, description string, err error) {
	c.String(http.StatusBadRequest, fmt.Sprintf("%s: %s", description, err))
}

func Index(c *echo.Context) {
	c.String(http.StatusOK, "Welcome to RTD v0.1")
}

func Create(c *echo.Context) {
	_, err := getDb(c.Param("db"))
	if err != nil {
		badRequest(c, "Error creating your database", err)
	} else {
		c.String(http.StatusOK, fmt.Sprintf("Created %s", c.Param("db")))
	}
}

func Delete(c *echo.Context) {
	dbName := c.Param("db")
	db, err := getDb(dbName)
	if err != nil {
		badRequest(c, "Error deleting your database", err)
		return
	}
	db.Close()
	err = os.Remove(dbFileName(dbName))
	if err != nil {
		badRequest(c, "Error deleting your database", err)
		return
	}
	c.String(http.StatusOK, fmt.Sprintf("Deleted %s", dbName))
}

func Query(c *echo.Context) {
	body, _ := ioutil.ReadAll(c.Request.Body)
	c.String(http.StatusOK, fmt.Sprintf("Query %s:%s:%s", c.Param("db"), c.Param("collection"), body))
}

func InsertDoc(c *echo.Context) {
	db, err := getDb(c.Param("db"))
	if err != nil {
		badRequest(c, "Error retrieving db:", err)
		return
	}

	// decode the document
	var doc map[string]interface{}
	err = codec.NewDecoder(c.Request.Body, jh).Decode(&doc)
	if err != nil {
		badRequest(c, "Error decoding your document", err)
		return
	}

	// add the id
	id := uuid.NewUUID()
	doc["_id"] = id.String()
	lookupId := new(bytes.Buffer)
	time, ok := id.Time()
	if ok != true {
		badRequest(c, "Error building time from uuid", err)
		return
	}
	binary.Write(lookupId, binary.BigEndian, time)
	binary.Write(lookupId, binary.BigEndian, id)

	// encode the document
	encDoc := new(bytes.Buffer)
	err = codec.NewEncoder(encDoc, jh).Encode(doc)
	if err != nil {
		badRequest(c, "Error encoding your document", err)
		return
	}

	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(c.Param("collection")))
		if err != nil {
			return err
		}
		return bucket.Put(lookupId.Bytes(), encDoc.Bytes())
	})
	if err != nil {
		badRequest(c, "Error inserting your document", err)
		return
	}

	c.String(http.StatusOK, fmt.Sprintf("InsertDoc %s:%s:%s", c.Param("db"), c.Param("collection"), encDoc))
}

func FindDoc(c *echo.Context) {
	db, err := getDb(c.Param("db"))
	if err != nil {
		badRequest(c, "Error retrieving db:", err)
		return
	}

	id := uuid.Parse(c.Param("id"))
	lookupId := new(bytes.Buffer)
	time, ok := id.Time()
	if ok != true {
		badRequest(c, "Error building time from uuid", err)
		return
	}
	binary.Write(lookupId, binary.BigEndian, time)
	binary.Write(lookupId, binary.BigEndian, id)

	var doc []byte
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(c.Param("collection")))
		doc = bucket.Get(lookupId.Bytes())
		return nil
	})

	c.String(http.StatusOK, fmt.Sprintf("FindDoc %s:%s:%s:%s", c.Param("db"), c.Param("collection"), id, doc))
}

func DeleteDoc(c *echo.Context) {
	c.String(http.StatusOK, fmt.Sprintf("DeleteDoc %s:%s:%s", c.Param("db"), c.Param("collection"), c.Param("id")))
}

func start(bind string) {
	e := echo.New()

	// root
	e.Get("/", Index)

	// DB
	e.Post("/:db", Create)
	e.Delete("/:db", Delete)

	// Documents
	e.Get("/:db/:collection", Query)
	e.Post("/:db/:collection", InsertDoc)
	e.Get("/:db/:collection/:id", FindDoc)
	e.Delete("/:db/:collection/:id", DeleteDoc)

	e.Run(bind)
}

func main() {
	var dir string
	var bind string
	flag.StringVar(&dir, "dir", "", "(HTTP server) database directory")
	flag.StringVar(&bind, "bind", ":8888", "(HTTP server) listening address")
	flag.Parse()

	if dir == "" {
		log.Fatal("Please specify database directory, for example -dir=/tmp/db")
	}
	rootDir = dir

	start(bind)
}
