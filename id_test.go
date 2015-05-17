package main

import (
	"bytes"
	"encoding/binary"
	"testing"

	"code.google.com/p/go-uuid/uuid"
	"github.com/hooklift/assert"
)

func TestId(t *testing.T) {
	id, lookupId, err := newId()
	assert.Ok(t, err)

	parsedId := uuid.Parse(id)
	assert.Cond(t, parsedId != nil, "id should be a valid uuid")
	v, ok := parsedId.Version()
	assert.Cond(t, ok, "id should have a valid version")
	assert.Cond(t, v == 1, "id should be a version 1 uuid")
	idTime, ok := parsedId.Time()
	assert.Cond(t, ok, "id should have a valid time")
	assert.Cond(t, idTime > 0, "id time should be greater than zero")

	var lookupTime uuid.Time
	binary.Read(lookupId, binary.BigEndian, &lookupTime)
	assert.Cond(t, lookupTime == idTime, "the lookip time should equal the id time")
	rawId := lookupId.Next(128)
	assert.Cond(t, bytes.Equal(rawId, parsedId), "the lookup id should end with the full uuid")
}
