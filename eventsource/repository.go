package eventsource

import (
	"errors"
	"reflect"
	"time"
)

// Store ...
type Store interface {
	Save(record Record) error
	Load(aggregateID string) (record []Record, err error)
}

// Aggregate ...
type Aggregate interface {
	On(event Event) error
	SetAggregateID(id string)
}

type Serializer interface {
	Unmarshal(data []byte, eventType string) (event Event, err error)
	Marshal(event Event) (data []byte, err error)
}

// Repository ...
type Repository interface {
	Save(events ...Event) (err error)
	Load(aggregateID string) (aggr Aggregate, err error)
}

// NewRepository ...
func NewRepository(store Store, aggregate Aggregate, serializer Serializer) Repository {
	return &repository{
		store:         store,
		aggregateType: getTypeOfValue(aggregate),
		serializer:    serializer,
	}
}

// Record ...
type Record struct {
	AggregateID string `json:"aggregateId"`
	Timestamp   int64  `json:"timestamp"`
	Type        string `json:"type"`
	Data        []byte `json:"data"`
	UserID      string `json:"userId"`
}

// repository ...
type repository struct {
	store         Store
	aggregateType reflect.Type
	serializer    Serializer
}

func getTypeOfValue(input interface{}) reflect.Type {
	value := reflect.TypeOf(input)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	return value
}

// SaveEvent ...
func (repo *repository) Save(events ...Event) (err error) {
	for _, event := range events {
		var data []byte
		if data, err = repo.serializer.Marshal(event); err != nil {
			return
		}

		record := Record{
			AggregateID: event.GetAggregateID(),
			Timestamp:   time.Now().UnixNano(),
			Type:        reflect.TypeOf(event).Name(),
			Data:        data,
			UserID:      event.GetUserID(),
		}

		if err = repo.store.Save(record); err != nil {
			return
		}
	}
	return nil
}

var ErrDeleted = errors.New("Not found (was deleted)")

// Load ...
func (repo repository) Load(aggregateID string) (aggr Aggregate, err error) {
	history, err := repo.store.Load(aggregateID)
	if err != nil {
		return
	}

	if len(history) == 0 {
		return aggr, errors.New("No history found")
	}

	aggr = reflect.New(repo.aggregateType).Interface().(Aggregate)
	aggr.SetAggregateID(aggregateID)

	for _, record := range history {
		var event Event
		if event, err = repo.serializer.Unmarshal(record.Data, record.Type); err != nil {
			return
		}

		if err = aggr.On(event); err.Error() == ErrDeleted.Error() {
			return nil, nil
		} else if err != nil {
			return
		}
	}
	return
}
