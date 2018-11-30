package memorystore

import (
	"context"
	"sort"

	"github.com/SKF/go-eventsource/eventsource"
)

type store struct {
	Data map[string][]eventsource.Record
}

// New creates a new event store
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



func (store *store) LoadNewerThan(ctx context.Context, sequenceID string) (records []eventsource.Record, err error) {
	for _, aggregate := range store.Data {
		for _, row := range aggregate {
			if row.SequenceID > sequenceID {
				records = append(records, row)
			}
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].SequenceID < records[j].SequenceID
	})
	return
}
