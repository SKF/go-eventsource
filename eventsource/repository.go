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
	// ErrDeleted is returned by Aggregate.On() method to signal that the object has been deleted
	ErrDeleted = errors.New("Not found (was deleted)")
	// ErrNoHistory is returned by Repository.Load() when no history exist for the given aggregate ID
	ErrNoHistory = errors.New("No history found")
)

// Store is the interface implemented by the data stores that can be used as back end for
// the event source.
type Store interface {
	NewTransaction(ctx context.Context, records ...Record) (StoreTransaction, error)
	LoadByAggregate(ctx context.Context, aggregateID string) (record []Record, err error)
	LoadBySequenceID(ctx context.Context, sequenceID string) (record []Record, err error)
	LoadBySequenceIDAndType(ctx context.Context, sequenceID string, eventType string) (records []Record, err error)
	LoadByTimestamp(ctx context.Context, timestamp int64) (record []Record, err error)
}

// StoreTransaction encapsulates a write operation to a Store, allowing the caller
// to roll back the operation.
type StoreTransaction interface {
	Commit() error
	Rollback() error
}

// Aggregate is an interface representing an object whose state changes can be
// recorded to and replayed from an event source.
type Aggregate interface {
	On(ctx context.Context, event Event) error
	SetAggregateID(id string)
}

// Serializer is an interface that should be implemented if events need to be saved
// using a new storage format.
type Serializer interface {
	Unmarshal(data []byte, eventType string) (event Event, err error)
	Marshal(event Event) (data []byte, err error)
}

// Repository is an interface representing the actual event source.
type Repository interface {
	// Save one or more events to the repository
	Save(ctx context.Context, events ...Event) error

	// Save one or more events to the repository, within a transaction
	SaveTransaction(ctx context.Context, events ...Event) (StoreTransaction, error)

	// Load events from repository for the given aggregate ID. For each event e,
	// call aggr.On(e) to update the state of aggr. When done, aggr has been
	// "fast forwarded" to the current state.
	Load(ctx context.Context, id string, aggr Aggregate) (deleted bool, err error)

	// Get all events with sequence ID newer than the given ID (see https://github.com/oklog/ulid)
	// There is a limit to how many events can be returned, so this method
	// should be called repeatedly until no more events are returned.
	GetEventsBySequenceID(ctx context.Context, sequenceID string) (events []Event, err error)

	// Same as GetEventsBySequenceID, but only returns events of the same type
	// as the one provided in the eventType parameter.
	GetEventsBySequenceIDAndType(ctx context.Context, sequenceID string, eventType Event) (events []Event, err error)

	// Get all events newer than the given timestamp
	// There is a limit to how many events can be returned, so this method
	// should be called repeatedly until no more events are returned.
	GetEventsByTimestamp(ctx context.Context, timestamp int64) (events []Event, err error)
}

// NewRepository returns a new repository
func NewRepository(store Store, serializer Serializer) Repository {
	return &repository{
		store:      store,
		serializer: serializer,
	}
}

// Record is a store row. The Data field contains the marshalled Event, and
// Type is the type of event retrieved by reflect.TypeOf(event).
type Record struct {
	AggregateID string `json:"aggregateId"`
	SequenceID  string `json:"sequenceId"`
	Timestamp   int64  `json:"timestamp"`
	Type        string `json:"type"`
	Data        []byte `json:"data"`
	UserID      string `json:"userId"`
}

// EventRecord is returned by GetRecords and contains
// both the raw Record and the unmarshalled Event
type EventRecord struct {
	Record Record
	Event  Event
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
		event.SetSequenceID(NewULID())
		event.SetTimestamp(time.Now().UnixNano())

		data, err := repo.serializer.Marshal(event)
		if err != nil {
			return nil, err
		}

		records = append(records, Record{
			AggregateID: event.GetAggregateID(),
			SequenceID:  event.GetSequenceID(),
			Timestamp:   event.GetTimestamp(),
			Type:        reflect.TypeOf(event).Name(),
			Data:        data,
			UserID:      event.GetUserID(),
		})
	}

	return repo.store.NewTransaction(ctx, records...)
}

// Load rehydrates the repo
func (repo repository) Load(ctx context.Context, aggregateID string, aggr Aggregate) (_ bool, err error) {
	history, err := repo.store.LoadByAggregate(ctx, aggregateID)
	if err != nil {
		return
	}

	if len(history) == 0 {
		return false, ErrNoHistory
	}

	aggr.SetAggregateID(aggregateID)

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

func unmarshalRecords(serializer Serializer, records []Record) (events []Event, err error) {
	for _, record := range records {
		var event Event
		if event, err = serializer.Unmarshal(record.Data, record.Type); err != nil {
			return
		}
		events = append(events, event)
	}
	return
}

func (repo repository) GetEventsBySequenceID(ctx context.Context, sequenceID string) (events []Event, err error) {
	var records []Record
	if records, err = repo.store.LoadBySequenceID(ctx, sequenceID); err != nil {
		return
	}
	return unmarshalRecords(repo.serializer, records)
}

func (repo repository) GetEventsBySequenceIDAndType(ctx context.Context, sequenceID string, eventType Event) (events []Event, err error) {
	var records []Record
	if records, err = repo.store.LoadBySequenceIDAndType(ctx, sequenceID, reflect.TypeOf(eventType).Name()); err != nil {
		return
	}
	return unmarshalRecords(repo.serializer, records)
}

func (repo repository) GetEventsByTimestamp(ctx context.Context, timestamp int64) (events []Event, err error) {
	var records []Record
	if records, err = repo.store.LoadByTimestamp(ctx, timestamp); err != nil {
		return
	}
	return unmarshalRecords(repo.serializer, records)
}
