# rtd

rtd is a document database built on top of [boltdb/bolt](https://github.com/boltdb/bolt) and accessible through a REST API.

# Project Status

rtd is a side project and has not been used in production.

# Getting Started

## Install

Install the database.

```
$ go get github.com/dmcaulay/rtd
```

Install the command line tool

```
$ npm install -g rtdctl
```

## Running

Start rtd with a data directory.

```
$ rtd -dir=/data/dir
```

## Accesssing

Start rtdctl or use curl.

```
$ rtdctl
```

### Selecting the DB

```
rtdctl> use('blog')
```

### Inserting

```
rtdctl> db.posts.insert({hello: 'world'})
```
