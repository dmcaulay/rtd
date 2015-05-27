package main

import (
	"bytes"
	"encoding/binary"
	"testing"

	"code.google.com/p/go-uuid/uuid"
	"github.com/hooklift/assert"
)

func TestNewId(t *testing.T) {
	id, lookupId, err := NewId()
	assert.Ok(t, err)

	parsedId := uuid.Parse(id)
	assert.Cond(t, parsedId != nil, "id should be a valid uuid")
	v, ok := parsedId.Version()
	assert.Cond(t, ok, "id should have a valid version")
	assert.Cond(t, v == 1, "id should be a version 1 uuid")
	idTime, ok := parsedId.Time()
	assert.Cond(t, ok, "id should have a valid time")
	assert.Cond(t, idTime > 0, "id time should be greater than zero")

	reader := bytes.NewBuffer(lookupId)
	var lookupTime uuid.Time
	binary.Read(reader, binary.BigEndian, &lookupTime)
	assert.Cond(t, lookupTime == idTime, "the lookup time should equal the id time")
	rawId := reader.Next(128)
	assert.Cond(t, bytes.Equal(rawId, parsedId), "the lookup id should end with the full uuid")
}

func TestParseId(t *testing.T) {
	_, err := ParseId("invalid")
	assert.Cond(t, err != nil, "ParseId should return an error if the id is invalid")

	id, lookupId, err := NewId()
	assert.Ok(t, err)

	parsedLookup, err := ParseId(id)
	assert.Ok(t, err)
	assert.Cond(t, bytes.Equal(parsedLookup, lookupId), "ParseId should return the same lookup id that NewId returns")
}
