# rtd

rtd is a document database built on top of [boltdb/bolt](https://github.com/boltdb/bolt) and accessible through a REST API.

# Project Status

rtd is a side project and has not been used in production.

# Getting Started

## Install

The database.

```
$ go get github.com/dmcaulay/rtd
```

The command line tool

```
$ npm install -g rtdctl
```

## Run

Start rtd with a data directory.

```
$ rtd -dir=/data/dir
```

## Usage

rtdctl

``` js
$ rtdctl
rtdctl> use('blog')
rtdctl> db.posts.insert({hello: 'world'})
=> { hello: 'world', _id: 'dd4f6107-f77b-11e4-befe-406c8f1dca7a' }
rtdctl> db.posts.find({hello: 'world'})
=> { hello: 'world', _id: 'dd4f6107-f77b-11e4-befe-406c8f1dca7a' }
rtdctl> db.posts.findById('dd4f6107-f77b-11e4-befe-406c8f1dca7a')
=> { hello: 'world', _id: 'dd4f6107-f77b-11e4-befe-406c8f1dca7a' }
rtdctl> db.posts.insert({title: 'golang is awesome', author: 'dmcaulay'})
=> { title: 'golang is awesome', author: 'dmcaulay', _id: '0fae3410-f788-11e4-befe-406c8f1dca7a' }
rtdctl> db.posts.find()
=> [ { hello: 'world', _id: 'dd4f6107-f77b-11e4-befe-406c8f1dca7a'}, { title: 'golang is awesome', author: 'dmcaulay', _id: '0fae3410-f788-11e4-befe-406c8f1dca7a' } ]
rtdctl> db.posts.updateById('0fae3410-f788-11e4-befe-406c8f1dca7a', {title: 'golang is fun'})
=> { title: 'golang is fun', author: 'dmcaulay', _id: '0fae3410-f788-11e4-befe-406c8f1dca7a' }
rtdctl> db.posts.update({author: 'dmcaulay'}, {title: 'update by query'})
=> { title: 'golang is fun', author: 'dmcaulay', _id: '0fae3410-f788-11e4-befe-406c8f1dca7a' }
```
