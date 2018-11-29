package memorystore

import (
	"context"

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

// Load ...
func (store *store) LoadAggregate(_ context.Context, aggregateID string) (records []eventsource.Record, err error) {
	if rows, ok := store.Data[aggregateID]; ok {
		return rows, nil
	}
	return records, nil
}

func (store *store) LoadNewerThan(ctx context.Context, sequenceID string) (records []eventsource.Record, hasMore bool, err error) {
	for _, aggregate := range store.Data {
		for _, row := range aggregate {
			if row.SequenceID >= sequenceID {
				records = append(records, row)
			}
		}
	}
	// TODO: sort records by SequenceID
	return
}
