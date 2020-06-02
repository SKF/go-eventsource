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
func (mem *store) LoadByAggregate(_ context.Context, aggregateID string) (records []eventsource.Record, err error) {
	if rows, ok := mem.Data[aggregateID]; ok {
		return rows, nil
	}
	return records, nil
}

func sortRecords(records []eventsource.Record) {
	sort.Slice(records, func(i, j int) bool {
		return records[i].SequenceID < records[j].SequenceID
	})
}

func (mem *store) loadRecords(includeRecord func(eventsource.Record) bool, limit int) (records []eventsource.Record, err error) {
	var recordSlice []eventsource.Record
	for _, aggregate := range mem.Data {
		recordSlice = append(recordSlice, aggregate...)
	}
	sortRecords(recordSlice)
	for _, record := range recordSlice {
		if includeRecord(record) {
			records = append(records, record)
		}
		if limit != 0 && len(records) >= limit {
			return
		}
	}
	return
}
func (mem *store) GetRecordsForAggregate(ctx context.Context, aggregateID string, sequenceID string) (records []eventsource.Record, err error) {
	return mem.loadRecords(func(record eventsource.Record) (include bool) {
		return record.AggregateID == aggregateID && record.SequenceID > sequenceID
	}, 0)
}
func (mem *store) LoadBySequenceID(_ context.Context, sequenceID string, limit int) (records []eventsource.Record, err error) {
	return mem.loadRecords(func(record eventsource.Record) (include bool) {
		return record.SequenceID > sequenceID
	}, limit)
}

func (mem *store) LoadBySequenceIDAndType(_ context.Context, sequenceID string, eventType string, limit int) (records []eventsource.Record, err error) {
	return mem.loadRecords(func(record eventsource.Record) (include bool) {
		return record.SequenceID > sequenceID && record.Type == eventType
	}, limit)
}

func (mem *store) LoadByTimestamp(_ context.Context, timestamp int64, limit int) (records []eventsource.Record, err error) {
	return mem.loadRecords(func(record eventsource.Record) (include bool) {
		return record.Timestamp > timestamp
	}, limit)
}
