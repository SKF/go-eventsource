package memorystore

import (
	"sort"

	"github.com/SKF/go-eventsource/v2/eventsource"
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

// WithLimit will limit the result
func WithLimit(limit int) eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			o.limit = &limit
		}
	}
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
