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
func (mem *store) LoadByAggregate(_ context.Context, aggregateID string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	if rows, ok := mem.Data[aggregateID]; ok {
		return rows, nil
	}
	return records, nil
}

func (mem *store) loadRecords(opts []eventsource.QueryOption, includeRecord func(eventsource.Record) bool) (records []eventsource.Record, err error) {
	queryOpts := evaluateQueryOptions(opts)

	var recordSlice []eventsource.Record
	for _, aggregate := range mem.Data {
		recordSlice = append(recordSlice, aggregate...)
	}
	queryOpts.sorter(recordSlice)
	for _, record := range recordSlice {
		if includeRecord(record) {
			records = append(records, record)
		}
		if queryOpts.limit != nil && len(records) >= *queryOpts.limit {
			return
		}
	}
	return
}

func (mem *store) LoadBySequenceID(_ context.Context, sequenceID string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return mem.loadRecords(opts, func(record eventsource.Record) (include bool) {
		return record.SequenceID > sequenceID
	})
}

func (mem *store) LoadBySequenceIDAndType(_ context.Context, sequenceID string, eventType string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return mem.loadRecords(opts, func(record eventsource.Record) (include bool) {
		return record.SequenceID > sequenceID && record.Type == eventType
	})
}

func (mem *store) LoadByTimestamp(_ context.Context, timestamp int64, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return mem.loadRecords(opts, func(record eventsource.Record) (include bool) {
		return record.Timestamp > timestamp
	})
}
