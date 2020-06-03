package sqlstore

import (
	"github.com/SKF/go-eventsource/v2/eventsource"
)

var (
	defaultOptions = &options{
		descending: false,
	}
)

type column string

const (
	columnAggregateID column = "aggregate_id"
	columnSequenceID  column = "sequence_id"
	columnCreatedAt   column = "created_at"
	columnUserID      column = "user_id"
	columnType        column = "type"
	columnData        column = "data"
)

type whereOperator string

const (
	whereOperatorEquals      = "="
	whereOperatorLessThan    = "<"
	whereOperatorGreaterThan = ">"
)

type whereOpt struct {
	value    interface{}
	operator whereOperator
}

type options struct {
	limit      *int
	offset     *int
	descending bool
	where      map[column]whereOpt
}

// WithLimit will limit the result
func WithLimit(limit int) eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			o.limit = &limit
		}
	}
}

// WithOffset will offset the result
func WithOffset(offset int) eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			o.offset = &offset
		}
	}
}

// WithDescending will set the sorting order to descending
func WithDescending() eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			o.descending = true
		}
	}
}

// WithAscending will set the sorting order to ascending
func WithAscending() eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			o.descending = false
		}
	}
}

func where(operator whereOperator, key column, value interface{}) eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			o.where[key] = whereOpt{
				value:    value,
				operator: operator,
			}
		}
	}
}

func equals(key column, value interface{}) eventsource.QueryOption {
	return where(whereOperatorEquals, key, value)
}

func lessThan(key column, value interface{}) eventsource.QueryOption {
	return where(whereOperatorLessThan, key, value)
}

func greaterThan(key column, value interface{}) eventsource.QueryOption {
	return where(whereOperatorGreaterThan, key, value)
}

func BySequenceID(value string) eventsource.QueryOption {
	return greaterThan(columnSequenceID, value)
}

func ByTimestamp(value int64) eventsource.QueryOption {
	return greaterThan(columnCreatedAt, value)
}

func ByType(value string) eventsource.QueryOption {
	return equals(columnType, value)
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
