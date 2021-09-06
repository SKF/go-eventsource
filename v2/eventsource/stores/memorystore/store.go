package memorystore

import (
	"context"
	"sync"

	"github.com/SKF/go-eventsource/v2/eventsource"
)

type store struct {
	Data map[string][]eventsource.Record
	mutex  sync.RWMutex
}

// New creates a new event store
func New() eventsource.Store {
	return &store{
		Data: map[string][]eventsource.Record{},
	}
}

// Load will load records based on specified query options
func (mem *store) Load(_ context.Context, opts ...eventsource.QueryOption) ([]eventsource.Record, error) {
	return mem.loadRecords(opts)
}

func (mem *store) LoadByAggregate(_ context.Context, aggregateID string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	mem.mutex.RLock()
	defer mem.mutex.RUnlock()

	if rows, ok := mem.Data[aggregateID]; ok {
		return rows, nil
	}
	return records, nil
}

func (mem *store) loadRecords(opts []eventsource.QueryOption) (records []eventsource.Record, err error) {
	mem.mutex.RLock()
	defer mem.mutex.RUnlock()

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

// Deprecated
func (mem *store) LoadBySequenceID(ctx context.Context, sequenceID string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return mem.Load(ctx, append(opts, BySequenceID(sequenceID))...)
}

// Deprecated
func (mem *store) LoadBySequenceIDAndType(ctx context.Context, sequenceID string, eventType string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return mem.Load(ctx, append(opts, BySequenceID(sequenceID), ByType(eventType))...)
}

// Deprecated
func (mem *store) LoadByTimestamp(ctx context.Context, timestamp int64, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return mem.Load(ctx, append(opts, ByTimestamp(timestamp))...)
}
