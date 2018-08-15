package eventsource

import (
	"errors"
	"reflect"
	"time"
)

// Store is a interface
type Store interface {
	Save(record Record) error
	Load(id string) (record []Record, err error)
}

// Aggregate is a interface
type Aggregate interface {
	On(event Event) error
	SetAggregateID(id string)
}

// Serializer is a interface
type Serializer interface {
	Unmarshal(data []byte, eventType string) (event Event, err error)
	Marshal(event Event) (data []byte, err error)
}

// Repository is a interface
type Repository interface {
	Save(events ...Event) (err error)
	Load(id string, aggr Aggregate) (deleted bool, err error)
}

// NewRepository returns a new repository
func NewRepository(store Store, serializer Serializer) Repository {
	return &repository{
		store:      store,
		serializer: serializer,
	}
}

// Record is a store row
type Record struct {
	AggregateID string `json:"aggregateId"`
	Timestamp   int64  `json:"timestamp"`
	Type        string `json:"type"`
	Data        []byte `json:"data"`
	UserID      string `json:"userId"`
}

type repository struct {
	store      Store
	serializer Serializer
}

// Save persists the event to the repo
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

		if eventOnSave, ok := event.(EventOnSave); ok {
			err = eventOnSave.OnSave(record)
			if err != nil {
				return
			}
		}
	}
	return nil
}

var (
	ErrDeleted   = errors.New("Not found (was deleted)")
	ErrNoHistory = errors.New("No history found")
)

// Load rehydrates the repo
func (repo repository) Load(id string, aggr Aggregate) (_ bool, err error) {
	history, err := repo.store.Load(id)
	if err != nil {
		return
	}

	if len(history) == 0 {
		return false, ErrNoHistory
	}

	aggr.SetAggregateID(id)

	for _, record := range history {
		var event Event
		if event, err = repo.serializer.Unmarshal(record.Data, record.Type); err != nil {
			return
		}

		if err = aggr.On(event); err == ErrDeleted {
			return true, nil
		} else if err != nil {
			return
		}
	}
	return
}
