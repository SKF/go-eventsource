package eventsource

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"github.com/oklog/ulid"
)

var (
	ErrDeleted   = errors.New("Not found (was deleted)")
	ErrNoHistory = errors.New("No history found")
)

// Store is a interface
type Store interface {
	NewTransaction(ctx context.Context, records ...Record) (StoreTransaction, error)
	Load(ctx context.Context, id string) (record []Record, err error)
}

type StoreTransaction interface {
	Commit() error
	Rollback() error
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
	Save(ctx context.Context, events ...Event) error
	SaveTransaction(ctx context.Context, events ...Event) (StoreTransaction, error)
	Load(ctx context.Context, id string, aggr Aggregate) (deleted bool, err error)
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
	entropy      = ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
	entropyMutex sync.Mutex
)

// NewULID returns a Universally Unique Lexicographically Sortable Identifier
func NewULID() string {
	entropyMutex.Lock()
	defer entropyMutex.Unlock()
	return ulid.MustNew(ulid.Now(), entropy).String()
}

// Save persists the event to the repo
func (repo *repository) Save(ctx context.Context, events ...Event) error {
	tx, err := repo.SaveTransaction(ctx, events...)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		rollbackErr := tx.Rollback()
		return fmt.Errorf("Rollback error: %+v, Save error: %+v", rollbackErr, err)
	}

	return nil
}

func (repo *repository) SaveTransaction(ctx context.Context, events ...Event) (StoreTransaction, error) {
	records := []Record{}
	for _, event := range events {
		data, err := repo.serializer.Marshal(event)
		if err != nil {
			return nil, err
		}

		records = append(records, Record{
			AggregateID: event.GetAggregateID(),
			SequenceID:  NewULID(),
			Timestamp:   time.Now().UnixNano(),
			Type:        reflect.TypeOf(event).Name(),
			Data:        data,
			UserID:      event.GetUserID(),
		})
	}

	return repo.store.NewTransaction(ctx, records...)
}

// Load rehydrates the repo
func (repo repository) Load(ctx context.Context, id string, aggr Aggregate) (_ bool, err error) {
	history, err := repo.store.Load(ctx, id)
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
