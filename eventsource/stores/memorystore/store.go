package memorystore

import (
	"errors"

	"github.com/skf/go-eventsource/eventsource"
)

type store struct {
	Data map[string][]eventsource.Record
}

func New() eventsource.Store {
	return &store{
		Data: map[string][]eventsource.Record{},
	}
}

// Save ...
func (mem *store) Save(record eventsource.Record) error {
	id := record.AggregateID
	if rows, ok := mem.Data[id]; ok {
		mem.Data[id] = append(rows, record)
	} else {
		mem.Data[id] = []eventsource.Record{record}
	}

	return nil
}

// Load ...
func (mem *store) Load(aggregateID string) (evt []eventsource.Record, err error) {
	if rows, ok := mem.Data[aggregateID]; ok {
		return rows, nil
	}
	return evt, errors.New("Not found")
}
