package eventsource

import (
	"context"

	"github.com/stretchr/testify/mock"
)

// SerializerMock is a mock
type SerializerMock struct {
	*mock.Mock
}

// CreateSerializerMock returns a serializerMock
func CreateSerializerMock() *SerializerMock {
	return &SerializerMock{
		Mock: &mock.Mock{},
	}
}

// StoreMock is a mock
type StoreMock struct {
	*mock.Mock
}

// CreateStoreMock returns a storeMock
func CreateStoreMock() *StoreMock {
	return &StoreMock{
		Mock: &mock.Mock{},
	}
}

// AggregatorMock is a mock
type AggregatorMock struct {
	Mock *mock.Mock
}

// CreateAggregatorMock returns an aggregatorMock
func CreateAggregatorMock() *AggregatorMock {
	return &AggregatorMock{
		Mock: &mock.Mock{},
	}
}

// Unmarshal parses the JSON-encoded data and returns an event
func (o SerializerMock) Unmarshal(data []byte, eventType string) (event Event, err error) {
	args := o.Called(data, eventType)
	return args.Get(0).(Event), args.Error(1)
}

// Marshal returns the JSON encoding of event.
func (o SerializerMock) Marshal(event Event) (data []byte, err error) {
	args := o.Called(event)
	return args.Get(0).([]byte), args.Error(1)
}

// StoreTransactionMock is a mock
type StoreTransactionMock struct {
	*mock.Mock
}

// CreateStoreTransactionMock returns a store transaction mock
func CreateStoreTransactionMock() *StoreTransactionMock {
	return &StoreTransactionMock{
		Mock: &mock.Mock{},
	}
}

// Commit is a mock
func (o StoreTransactionMock) Commit() error {
	args := o.Called()
	return args.Error(0)
}

// Rollback is a mock
func (o StoreTransactionMock) Rollback() error {
	args := o.Called()
	return args.Error(0)
}

// GetRecords is a mock
func (o StoreTransactionMock) GetRecords() []Record {
	args := o.Called()
	return args.Get(0).([]Record)
}

// NewTransaction is a mock
func (o StoreMock) NewTransaction(ctx context.Context, records ...Record) (StoreTransaction, error) {
	args := o.Called(ctx, records)
	return args.Get(0).(StoreTransaction), args.Error(1)
}

// LoadByAggregate is a mock
func (o StoreMock) LoadByAggregate(ctx context.Context, aggregateID string, opts ...QueryOption) (record []Record, err error) {
	args := o.Called(ctx, aggregateID, opts)
	return args.Get(0).([]Record), args.Error(1)
}

// LoadBySequenceID is a mock
func (o StoreMock) Load(ctx context.Context, opts ...QueryOption) (record []Record, err error) {
	args := o.Called(ctx, opts)
	return args.Get(0).([]Record), args.Error(1)
}

// LoadBySequenceID is a mock
func (o StoreMock) LoadBySequenceID(ctx context.Context, sequenceID string, opts ...QueryOption) (record []Record, err error) {
	args := o.Called(ctx, sequenceID, opts)
	return args.Get(0).([]Record), args.Error(1)
}

// LoadBySequenceIDAndType is a mock
func (o StoreMock) LoadBySequenceIDAndType(ctx context.Context, sequenceID string, eventType string, opts ...QueryOption) (record []Record, err error) {
	args := o.Called(ctx, sequenceID, eventType, opts)
	return args.Get(0).([]Record), args.Error(1)
}

// LoadByTimestamp is a mock
func (o StoreMock) LoadByTimestamp(ctx context.Context, timestamp int64, opts ...QueryOption) (record []Record, err error) {
	args := o.Called(ctx, timestamp, opts)
	return args.Get(0).([]Record), args.Error(1)
}

// On is a mock
func (o AggregatorMock) On(ctx context.Context, event Event) error {
	args := o.Mock.Called(ctx, event)
	return args.Error(0)
}

// SetAggregateID is not implemented
func (o AggregatorMock) SetAggregateID(id string) {}

// RepositoryMock is a mock
type RepositoryMock struct {
	*mock.Mock
}

// CreateRepositoryMock returns a repositoryMock
func CreateRepositoryMock() *RepositoryMock {
	return &RepositoryMock{
		Mock: &mock.Mock{},
	}
}

// Store is a mock
func (r RepositoryMock) Store() Store {
	args := r.Called()
	return args.Get(0).(Store)
}

// Save is a mock
func (r RepositoryMock) Save(ctx context.Context, events ...Event) error {
	args := r.Called(ctx, events)
	return args.Error(0)
}

// SaveTransaction is a mock
func (r RepositoryMock) SaveTransaction(ctx context.Context, events ...Event) (StoreTransaction, error) {
	args := r.Called(ctx, events)
	return args.Get(0).(StoreTransaction), args.Error(1)
}

// Load is a mock
func (r RepositoryMock) Load(ctx context.Context, id string, aggr Aggregate) (deleted bool, err error) {
	args := r.Called(ctx, id, aggr)
	return args.Bool(0), args.Error(1)
}

// UnmarshalRecords is a mock
func (r RepositoryMock) UnmarshalRecords(records []Record) ([]Event, error) {
	args := r.Called(records)
	return args.Get(0).([]Event), args.Error(1)
}

// LoadEvents is a mock
func (r RepositoryMock) LoadEvents(ctx context.Context, opts ...QueryOption) ([]Event, error) {
	args := r.Called(ctx, opts)
	return args.Get(0).([]Event), args.Error(1)
}

// GetEventsBySequenceID is a mock
func (r RepositoryMock) GetEventsBySequenceID(ctx context.Context, sequenceID string, opts ...QueryOption) ([]Event, error) {
	args := r.Called(ctx, sequenceID, opts)
	return args.Get(0).([]Event), args.Error(1)
}

// GetEventsBySequenceIDAndType is a mock
func (r RepositoryMock) GetEventsBySequenceIDAndType(ctx context.Context, sequenceID string, eventType Event, opts ...QueryOption) ([]Event, error) {
	args := r.Called(ctx, sequenceID, eventType, opts)
	return args.Get(0).([]Event), args.Error(1)
}

// GetEventsByTimestamp is a mock
func (r RepositoryMock) GetEventsByTimestamp(ctx context.Context, timestamp int64, opts ...QueryOption) ([]Event, error) {
	args := r.Called(ctx, timestamp, opts)
	return args.Get(0).([]Event), args.Error(1)
}

// AddNotificationService is a mock
func (r RepositoryMock) AddNotificationService(service NotificationService) {
	r.Called(service)
}

type NotificationServiceMock struct {
	*mock.Mock
}

// CreateNotificationServiceMock creates a notification service mock
func CreateNotificationServiceMock() *NotificationServiceMock {
	return &NotificationServiceMock{
		Mock: &mock.Mock{},
	}
}

func (ns NotificationServiceMock) Send(record Record) error {
	args := ns.Called(record)
	return args.Error(0)
}

func (ns NotificationServiceMock) SendWithContext(ctx context.Context, record Record) error {
	args := ns.Called(ctx, record)
	return args.Error(0)
}

var (
	_ Store               = &StoreMock{}
	_ Repository          = &RepositoryMock{}
	_ NotificationService = &NotificationServiceMock{}
)
