package eventsource

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

var (
	// ErrDeleted is returned by Aggregate.On() method to signal that the object has been deleted
	ErrDeleted = errors.New("not found (was deleted)")
	// ErrNoHistory is returned by Repository.Load() when no history exist for the given aggregate ID
	ErrNoHistory = errors.New("no history found")
	// ErrNotificationFailed is returned by Commit() if notification service fails
	ErrNotificationFailed = errors.New("Failed to send notification")
)

// QueryOption is used for setting store specific options like limit or sorting
// Can be found in any of the stores
type QueryOption func(opt interface{})

// Store is the interface implemented by the data stores that can be used as back end for
// the event source.
type Store interface {
	NewTransaction(ctx context.Context, records ...Record) (StoreTransaction, error)
	LoadByAggregate(ctx context.Context, aggregateID string, opts ...QueryOption) ([]Record, error)
	Load(ctx context.Context, opts ...QueryOption) ([]Record, error)

	// Deprecated: Use Load(ctx, store.BySequenceID(...))
	LoadBySequenceID(ctx context.Context, sequenceID string, opts ...QueryOption) (record []Record, err error)

	// Deprecated: Use Load(ctx, store.BySequenceID(...), store.ByType(...))
	LoadBySequenceIDAndType(ctx context.Context, sequenceID string, eventType string, opts ...QueryOption) (records []Record, err error)

	// Deprecated: Use Load(ctx, store.ByTimestamp(...))
	LoadByTimestamp(ctx context.Context, timestamp int64, opts ...QueryOption) (record []Record, err error)
}

// StoreTransaction encapsulates a write operation to a Store, allowing the caller
// to roll back the operation.
type StoreTransaction interface {
	Commit() error
	Rollback() error
	GetRecords() []Record
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

// NotificationService represents a service which can emit notifications
// when records are saved to the event source
type NotificationService interface {
	Send(record Record) error // deprecated use SendWithContext instead
	SendWithContext(ctx context.Context, record Record) error
}

// Repository is an interface representing the actual event source.
type Repository interface {
	// Return store
	Store() Store

	// Save one or more events to the repository
	Save(ctx context.Context, events ...Event) error

	// Save one or more events to the repository, within a transaction
	SaveTransaction(ctx context.Context, events ...Event) (StoreTransaction, error)

	// Load events from repository for the given aggregate ID. For each event e,
	// call aggr.On(e) to update the state of aggr. When done, aggr has been
	// "fast forwarded" to the current state.
	Load(ctx context.Context, id string, aggr Aggregate) (deleted bool, err error)

	// Get all events with query options (definied in the store)
	// Query options can be used for filter by sequence ID (see https://github.com/oklog/ulid)
	// or options like limit, offset
	LoadEvents(ctx context.Context, opts ...QueryOption) (events []Event, err error)

	// Deprecated: Use LoadEvents(ctx, store.BySequenceId(...))
	// Get all events with sequence ID newer than the given ID (see https://github.com/oklog/ulid)
	// Return at most limit records. If limit is 0, don't limit the number of records returned.
	GetEventsBySequenceID(ctx context.Context, sequenceID string, opts ...QueryOption) (events []Event, err error)

	// Deprecated: Use LoadEvents(ctx, store.BySequenceId(...), store.ByType(...))
	// Same as GetEventsBySequenceID, but only returns events of the same type
	// as the one provided in the eventType parameter.
	GetEventsBySequenceIDAndType(ctx context.Context, sequenceID string, eventType Event, opts ...QueryOption) (events []Event, err error)

	// Deprecated: Use LoadEvents(ctx, store.ByTimestamp(...))
	// Get all events newer than the given timestamp
	// Return at most limit records. If limit is 0, don't limit the number of records returned.
	GetEventsByTimestamp(ctx context.Context, timestamp int64, opts ...QueryOption) (events []Event, err error)

	// Add notification service
	AddNotificationService(service NotificationService)

	// Unmarshal records to events using repository
	UnmarshalRecords(records []Record) (events []Event, err error)
}

// NewRepository returns a new repository
func NewRepository(store Store, serializer Serializer) Repository {
	return &repository{
		store:                store,
		serializer:           serializer,
		notificationServices: []NotificationService{},
	}
}

func (repo *repository) AddNotificationService(service NotificationService) {
	repo.notificationServices = append(repo.notificationServices, service)
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

type repository struct {
	store                Store
	serializer           Serializer
	notificationServices []NotificationService
}

type transactionWrapper struct {
	ctx                  context.Context
	transaction          StoreTransaction
	notificationServices []NotificationService
}

func newTransactionWrapper(ctx context.Context, store Store, records []Record, ns []NotificationService) (StoreTransaction, error) {
	transaction, err := store.NewTransaction(ctx, records...)
	if err != nil {
		return nil, err
	}

	return &transactionWrapper{ctx, transaction, ns}, nil
}

// Commit transaction to underlying store and, if configured, publish the records to a
// notification service. If ErrNotificationFailed is returned, the data has been successfully
// committed to the store, but the notification service failed.
func (transWrap *transactionWrapper) Commit() error {
	err := transWrap.transaction.Commit()
	if err != nil {
		return err
	}

	for _, service := range transWrap.notificationServices {
		for _, r := range transWrap.transaction.GetRecords() {
			if err = service.SendWithContext(transWrap.ctx, r); err != nil {
				return fmt.Errorf("%w: %s", ErrNotificationFailed, err)
			}
		}
	}

	return nil
}

func (transWrap *transactionWrapper) Rollback() error {
	return transWrap.transaction.Rollback()
}

func (transWrap *transactionWrapper) GetRecords() []Record {
	return transWrap.transaction.GetRecords()
}

// See https://godoc.org/github.com/oklog/ulid#example-ULID
var (
	entropy      = ulid.Monotonic(rand.Reader, 0)
	entropyMutex sync.Mutex
)

// NewULID returns a Universally Unique Lexicographically Sortable Identifier
func NewULID() string {
	entropyMutex.Lock()
	defer entropyMutex.Unlock()

	return ulid.MustNew(ulid.Now(), entropy).String()
}

// Return store
func (repo *repository) Store() Store {
	return repo.store
}

// Save persists the event to the repo
func (repo *repository) Save(ctx context.Context, events ...Event) error {
	tx, err := repo.SaveTransaction(ctx, events...)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return errors.Wrapf(err, "rollback error: %+v", rollbackErr)
		}
		return errors.Wrap(err, "failed to commit transaction")
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
			Type:        GetTypeName(event),
			Data:        data,
			UserID:      event.GetUserID(),
		})
	}

	return newTransactionWrapper(ctx, repo.store, records, repo.notificationServices)
}

// Load rehydrates the repo
func (repo repository) Load(ctx context.Context, aggregateID string, aggr Aggregate) (deleted bool, err error) {
	history, err := repo.store.LoadByAggregate(ctx, aggregateID)
	if err != nil {
		return false, err
	}

	if len(history) == 0 {
		return false, ErrNoHistory
	}

	aggr.SetAggregateID(aggregateID)

	for _, record := range history {
		var event Event
		event, err = repo.serializer.Unmarshal(record.Data, record.Type)

		if err != nil {
			return false, err
		}

		// Some older events created with earlier releases did not have timestamp in
		// record.Data so in those cases we pick up timestamp from event
		if event.GetTimestamp() == int64(0) {
			event.SetTimestamp(record.Timestamp)
		}

		err = aggr.On(ctx, event)

		if err == ErrDeleted {
			return true, nil
		}

		if err != nil {
			return false, err
		}
	}

	return false, nil
}

func (repo repository) UnmarshalRecords(records []Record) ([]Event, error) {
	return unmarshalRecords(repo.serializer, records)
}

func unmarshalRecords(serializer Serializer, records []Record) (events []Event, err error) {
	for _, record := range records {
		var event Event

		if event, err = serializer.Unmarshal(record.Data, record.Type); err != nil {
			err = errors.Wrap(err, "failed to unmarshal record")
			return
		}

		events = append(events, event)
	}

	return
}

func (repo repository) LoadEvents(ctx context.Context, opts ...QueryOption) (events []Event, err error) {
	var records []Record

	if records, err = repo.store.Load(ctx, opts...); err != nil {
		return
	}

	return unmarshalRecords(repo.serializer, records)
}

// Deprecated
func (repo repository) GetEventsBySequenceID(ctx context.Context, sequenceID string, opts ...QueryOption) (events []Event, err error) {
	var records []Record

	if records, err = repo.store.LoadBySequenceID(ctx, sequenceID, opts...); err != nil {
		return
	}

	return unmarshalRecords(repo.serializer, records)
}

// Deprecated
func (repo repository) GetEventsBySequenceIDAndType(ctx context.Context, sequenceID string, eventType Event, opts ...QueryOption) (events []Event, err error) {
	var records []Record

	if records, err = repo.store.LoadBySequenceIDAndType(ctx, sequenceID, GetTypeName(eventType), opts...); err != nil {
		return
	}

	return unmarshalRecords(repo.serializer, records)
}

// Deprecated
func (repo repository) GetEventsByTimestamp(ctx context.Context, timestamp int64, opts ...QueryOption) (events []Event, err error) {
	var records []Record

	if records, err = repo.store.LoadByTimestamp(ctx, timestamp, opts...); err != nil {
		return
	}

	return unmarshalRecords(repo.serializer, records)
}
