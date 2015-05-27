package main

import (
	"bytes"
	"encoding/binary"
	"errors"

	"code.google.com/p/go-uuid/uuid"
)

func NewId() (string, []byte, error) {
	id := uuid.NewUUID()
	lookupId, err := buildLookupId(id)
	if err != nil {
		return "", nil, err
	}
	return id.String(), lookupId, nil
}

func ParseId(strId string) ([]byte, error) {
	id := uuid.Parse(strId)
	return buildLookupId(id)
}

func buildLookupId(id uuid.UUID) ([]byte, error) {
	time, ok := id.Time()
	if !ok {
		return nil, errors.New("Error retrieving time from UUID")
	}
	writer := new(bytes.Buffer)
	binary.Write(writer, binary.BigEndian, time)
	binary.Write(writer, binary.BigEndian, id)
	return writer.Bytes(), nil
}
