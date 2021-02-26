package memorystore

import (
	"sort"

	"github.com/SKF/go-eventsource/v2/eventsource"
)

type FilterFunc func(record eventsource.Record) bool

var defaultOptions = &options{
	sorter: func(records []eventsource.Record) {
		sort.Slice(records, func(i, j int) bool {
			return records[i].SequenceID < records[j].SequenceID
		})
	},
	filters: []FilterFunc{},
}

type options struct {
	sorter  func(records []eventsource.Record)
	filters []FilterFunc
	limit   *int
}

// WithLimit will limit the result
func WithLimit(limit int) eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			o.limit = &limit
		}
	}
}

func WithFilter(filter FilterFunc) eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			o.filters = append(o.filters, filter)
		}
	}
}

func BySequenceID(sequenceID string) eventsource.QueryOption {
	return WithFilter(func(record eventsource.Record) bool {
		return record.SequenceID > sequenceID
	})
}

func ByType(eventType string) eventsource.QueryOption {
	return WithFilter(func(record eventsource.Record) bool {
		return record.Type == eventType
	})
}

func ByTimestamp(timestamp int64) eventsource.QueryOption {
	return WithFilter(func(record eventsource.Record) bool {
		return record.Timestamp > timestamp
	})
}

// evaluate a list of options by extending the default options
func evaluateQueryOptions(opts []eventsource.QueryOption) *options {
	copy := &options{}
	*copy = *defaultOptions

	for _, opt := range opts {
		opt(copy)
	}

	return copy
}
