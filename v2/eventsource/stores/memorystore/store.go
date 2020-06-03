package memorystore

import (
	"context"

	"github.com/SKF/go-eventsource/v2/eventsource"
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
func (mem *store) Load(_ context.Context, opts ...eventsource.QueryOption) ([]eventsource.Record, error) {
	return mem.loadRecords(opts)
}

func (mem *store) LoadByAggregate(_ context.Context, aggregateID string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	if rows, ok := mem.Data[aggregateID]; ok {
		return rows, nil
	}
	return records, nil
}

func (mem *store) loadRecords(opts []eventsource.QueryOption) (records []eventsource.Record, err error) {
	queryOpts := evaluateQueryOptions(opts)

	var recordSlice []eventsource.Record
	for _, aggregate := range mem.Data {
		recordSlice = append(recordSlice, aggregate...)
	}
	queryOpts.sorter(recordSlice)
	for _, record := range recordSlice {
		filterResult := true
		for _, filter := range queryOpts.filters {
			filterResult = filterResult && filter(record)
		}

		if filterResult {
			records = append(records, record)
		}

		if queryOpts.limit != nil && len(records) >= *queryOpts.limit {
			return
		}
	}
	return
}
