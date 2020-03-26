package memorystore

import (
	"sort"

	"github.com/SKF/go-eventsource/eventsource"
)

var (
	defaultOptions = &options{
		sorter: func(records []eventsource.Record) {
			sort.Slice(records, func(i, j int) bool {
				return records[i].SequenceID < records[j].SequenceID
			})
		},
	}
)

type options struct {
	sorter func(records []eventsource.Record)
	limit  *int
}

func WithLimit(limit int) eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			o.limit = &limit
		}
	}
}

func evaluateQueryOptions(opts []eventsource.QueryOption) *options {
	copy := &options{}
	*copy = *defaultOptions
	for _, opt := range opts {
		opt(copy)
	}
	return copy
}
