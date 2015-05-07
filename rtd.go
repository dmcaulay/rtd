package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"code.google.com/p/go-uuid/uuid"
	"github.com/boltdb/bolt"
	"github.com/labstack/echo"
)

var dbs map[string]*bolt.DB = make(map[string]*bolt.DB)
var rootDir string

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
	body, _ := ioutil.ReadAll(c.Request.Body)
	uuid := uuid.NewUUID()
	c.String(http.StatusOK, fmt.Sprintf("InsertDoc %s:%s:%s:%s", c.Param("db"), c.Param("collection"), uuid, body))
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
