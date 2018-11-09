package memorystore

import (
	"context"
	"errors"

	"github.com/SKF/go-eventsource/eventsource"
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
func (store *store) Save(records ...eventsource.Record) (err error) {
	return store.SaveWithContext(context.Background(), records...)
}

// SaveWithContext ...
func (mem *store) SaveWithContext(_ context.Context, records ...eventsource.Record) error {
	for _, record := range records {
		id := record.AggregateID
		if rows, ok := mem.Data[id]; ok {
			mem.Data[id] = append(rows, record)
		} else {
			mem.Data[id] = []eventsource.Record{record}
		}
	}

	return nil
}

//Load ...
func (store *store) Load(id string) (records []eventsource.Record, err error) {
	return store.LoadWithContext(context.Background(), id)
}

// LoadWithContext ...
func (mem *store) LoadWithContext(_ context.Context, id string) (evt []eventsource.Record, err error) {
	if rows, ok := mem.Data[id]; ok {
		return rows, nil
	}
	return evt, errors.New("Not found")
}
