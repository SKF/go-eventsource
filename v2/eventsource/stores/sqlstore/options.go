package sqlstore

import (
	"github.com/SKF/go-eventsource/v2/eventsource"
)

var (
	defaultOptions = &options{
		descending: false,
	}
)

type Column string

const (
	ColumnAggregateID Column = "aggregate_id"
	ColumnSequenceID  Column = "sequence_id"
	ColumnCreatedAt   Column = "created_at"
	ColumnUserID      Column = "user_id"
	ColumnType        Column = "type"
	ColumnData        Column = "data"
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
	where      map[Column]whereOpt
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

func where(operator whereOperator, key Column, value interface{}) eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			o.where[key] = whereOpt{
				value:    value,
				operator: operator,
			}
		}
	}
}

func Equals(key Column, value interface{}) eventsource.QueryOption {
	return where(whereOperatorEquals, key, value)
}

func LessThan(key Column, value interface{}) eventsource.QueryOption {
	return where(whereOperatorLessThan, key, value)
}

func GreaterThan(key Column, value interface{}) eventsource.QueryOption {
	return where(whereOperatorGreaterThan, key, value)
}

func BySequenceID(value interface{}) eventsource.QueryOption {
	return GreaterThan(ColumnSequenceID, value)
}

func ByType(value interface{}) eventsource.QueryOption {
	return Equals(ColumnType, value)
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
