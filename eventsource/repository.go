package eventsource

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"github.com/oklog/ulid"
)

// Store is a interface
type Store interface {
	Save(record Record) error
	SaveWithContext(ctx context.Context, record Record) error
	Load(id string) (record []Record, err error)
	LoadWithContext(ctx context.Context, id string) (record []Record, err error)
}

// Aggregate is a interface
type Aggregate interface {
	On(ctx context.Context, event Event) error
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
	SaveWithContext(ctx context.Context, events ...Event) (err error)
	Load(id string, aggr Aggregate) (deleted bool, err error)
	LoadWithContext(ctx context.Context, id string, aggr Aggregate) (deleted bool, err error)
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
	SequenceID  string `json:"sequenceId"`
	Timestamp   int64  `json:"timestamp"`
	Type        string `json:"type"`
	Data        []byte `json:"data"`
	UserID      string `json:"userId"`
}

type repository struct {
	store      Store
	serializer Serializer
}

// See https://godoc.org/github.com/oklog/ulid#example-ULID
var (
	entropy      = rand.New(rand.NewSource(time.Now().UnixNano()))
	entropyMutex sync.Mutex
)

// NewULID returns a Universally Unique Lexicographically Sortable Identifier
func NewULID() string {
	entropyMutex.Lock()
	defer entropyMutex.Unlock()
	return ulid.MustNew(ulid.Now(), entropy).String()
}

// Save persists the event to the repo
func (repo *repository) Save(events ...Event) (err error) {
	return repo.SaveWithContext(context.Background(), events...)
}

// SaveWithContext persists the event to the repo
func (repo *repository) SaveWithContext(ctx context.Context, events ...Event) (err error) {
	for _, event := range events {
		var data []byte
		if data, err = repo.serializer.Marshal(event); err != nil {
			return
		}

		record := Record{
			AggregateID: event.GetAggregateID(),
			SequenceID:  NewULID(),
			Timestamp:   time.Now().UnixNano(),
			Type:        reflect.TypeOf(event).Name(),
			Data:        data,
			UserID:      event.GetUserID(),
		}

		if err = repo.store.SaveWithContext(ctx, record); err != nil {
			return
		}

		if eventOnSave, ok := event.(EventOnSave); ok {
			err = eventOnSave.OnSave(record)
			if err != nil {
				return
			}
		}

		if eventOnSave, ok := event.(EventOnSaveWithContext); ok {
			err = eventOnSave.OnSave(ctx, record)
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
	return repo.LoadWithContext(context.Background(), id, aggr)
}

// LoadWithContext rehydrates the repo
func (repo repository) LoadWithContext(ctx context.Context, id string, aggr Aggregate) (_ bool, err error) {
	history, err := repo.store.LoadWithContext(ctx, id)
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

		if err = aggr.On(ctx, event); err == ErrDeleted {
			return true, nil
		} else if err != nil {
			return
		}
	}
	return
}
