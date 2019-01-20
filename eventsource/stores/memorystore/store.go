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

func (mem *store) LoadBySequenceID(_ context.Context, sequenceID string) (records []eventsource.Record, err error) {
	for _, aggregate := range mem.Data {
		for _, row := range aggregate {
			if row.SequenceID > sequenceID {
				records = append(records, row)
			}
			if len(records) >= 1000 {
				break
			}
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].SequenceID < records[j].SequenceID
	})
	return
}

func (mem *store) LoadBySequenceIDAndType(_ context.Context, sequenceID string, eventType string) (records []eventsource.Record, err error) {
	for _, aggregate := range mem.Data {
		for _, row := range aggregate {
			if row.SequenceID > sequenceID && row.Type == eventType {
				records = append(records, row)
			}
			if len(records) >= 1000 {
				break
			}
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].SequenceID < records[j].SequenceID
	})
	return
}

func (mem *store) LoadByTimestamp(_ context.Context, timestamp int64) (records []eventsource.Record, err error) {
	for _, aggregate := range mem.Data {
		for _, row := range aggregate {
			if row.Timestamp > timestamp {
				records = append(records, row)
			}
			if len(records) >= 1000 {
				break
			}
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].Timestamp < records[j].Timestamp
	})
	return
}
