package main

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/labstack/echo"
)

func badRequest(c *echo.Context, description string, err error) {
	c.String(http.StatusBadRequest, fmt.Sprintf("%s: %s\n", description, err))
}

func ok(c *echo.Context) {
	c.String(http.StatusOK, "Success\n")
}

func okWithBody(c *echo.Context, body []byte) error {
	c.Response.Header().Set(echo.HeaderContentType, echo.MIMEJSON+"; charset=utf-8")
	c.Response.WriteHeader(http.StatusOK)
	_, err := c.Response.Write(body)
	return err
}

func Welcome(c *echo.Context) {
	c.String(http.StatusOK, "Welcome to RTD v0.1")
}

func Create(c *echo.Context) {
	if _, err := getDb(c.Param("db")); err != nil {
		badRequest(c, "Error creating your database", err)
	} else {
		ok(c)
	}
}

func Delete(c *echo.Context) {
	if err := deleteDb(c.Param("db")); err != nil {
		badRequest(c, "Error deleting your database", err)
	} else {
		ok(c)
	}
}

func Query(c *echo.Context) {
	body, _ := ioutil.ReadAll(c.Request.Body)
	c.String(http.StatusOK, fmt.Sprintf("Query %s:%s:%s", c.Param("db"), c.Param("collection"), body))
}

func InsertDoc(c *echo.Context) {
	insertedDoc, err := insertDoc(c.Param("db"), c.Param("collection"), c.Request.Body)
	if err != nil {
		badRequest(c, "Error inserting document", err)
	} else {
		okWithBody(c, insertedDoc.Bytes())
	}
}

func FindDoc(c *echo.Context) {
	doc, err := findDoc(c.Param("db"), c.Param("collection"), c.Param("id"))
	if err != nil {
		badRequest(c, "Error finding document", err)
	} else {
		okWithBody(c, doc)
	}
}

func DeleteDoc(c *echo.Context) {
	c.String(http.StatusOK, fmt.Sprintf("DeleteDoc %s:%s:%s", c.Param("db"), c.Param("collection"), c.Param("id")))
}

func StartHttp(bind string) {
	e := echo.New()

	// root
	e.Get("/", Welcome)

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
