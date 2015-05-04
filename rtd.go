package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"code.google.com/p/go-uuid/uuid"
	"github.com/labstack/echo"
)

func Index(c *echo.Context) {
	c.String(http.StatusOK, "Welcome to RTD")
}

func Create(c *echo.Context) {
	c.String(http.StatusOK, fmt.Sprintf("Create %s", c.Param("db")))
}

func Delete(c *echo.Context) {
	c.String(http.StatusOK, fmt.Sprintf("Delete %s", c.Param("db")))
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

func start(dir string, bind string) {
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
	start(dir, bind)
}
