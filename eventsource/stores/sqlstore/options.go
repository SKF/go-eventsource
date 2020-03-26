package sqlstore

import (
	"github.com/SKF/go-eventsource/eventsource"
)

var (
	defaultOptions = &options{
		descending: false,
	}
)

type options struct {
	limit      *int
	offset     *int
	descending bool
}

func WithLimit(limit int) eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			o.limit = &limit
		}
	}
}

func WithOffset(offset int) eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			o.offset = &offset
		}
	}
}

func WithDescending() eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			o.descending = true
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
