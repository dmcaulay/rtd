package main

import (
	"bytes"
	"encoding/binary"
	"errors"

	"code.google.com/p/go-uuid/uuid"
)

func newId() (string, *bytes.Buffer, error) {
	id := uuid.NewUUID()
	lookupId, err := buildLookupId(id)
	if err != nil {
		return "", nil, err
	}
	return id.String(), lookupId, nil
}

func parseId(strId string) (*bytes.Buffer, error) {
	id := uuid.Parse(strId)
	return buildLookupId(id)
}

func buildLookupId(id uuid.UUID) (*bytes.Buffer, error) {
	time, ok := id.Time()
	if !ok {
		return nil, errors.New("Error retrieving time from UUID")
	}
	lookupId := new(bytes.Buffer)
	binary.Write(lookupId, binary.BigEndian, time)
	binary.Write(lookupId, binary.BigEndian, id)
	return lookupId, nil
}
