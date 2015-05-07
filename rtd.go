package main

import (
	"flag"
	"log"

	"github.com/boltdb/bolt"
)

// Database map
var dbs map[string]*bolt.DB = make(map[string]*bolt.DB)

// Root data directory
var rootDir string

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

	StartHttp(bind)
}
