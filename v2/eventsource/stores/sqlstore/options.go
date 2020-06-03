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
	Column_AggregateID Column = "aggregate_id"
	Column_SequenceID  Column = "sequence_id"
	Column_CreatedAt   Column = "created_at"
	Column_UserID      Column = "user_id"
	Column_Type        Column = "type"
	Column_Data        Column = "data"
)

type whereOperator string

const (
	whereOperator_Equals      = "="
	whereOperator_LessThan    = "<"
	whereOperator_GreaterThan = ">"
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
	return where(whereOperator_Equals, key, value)
}

func LessThan(key Column, value interface{}) eventsource.QueryOption {
	return where(whereOperator_LessThan, key, value)
}

func GreaterThan(key Column, value interface{}) eventsource.QueryOption {
	return where(whereOperator_GreaterThan, key, value)
}

func BySequenceID(value interface{}) eventsource.QueryOption {
	return GreaterThan(Column_SequenceID, value)
}

func ByType(value interface{}) eventsource.QueryOption {
	return Equals(Column_Type, value)
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
