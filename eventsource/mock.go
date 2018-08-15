package eventsource

import (
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

// CreateAggregatorMock returns a aggregatorMock
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

// Save is a mock
func (o StoreMock) Save(record Record) error {
	args := o.Called(record)
	return args.Error(0)
}

// Load is a mock
func (o StoreMock) Load(id string) (record []Record, err error) {
	args := o.Called(id)
	return args.Get(0).([]Record), args.Error(1)
}

// On is a mock
func (o AggregatorMock) On(event Event) error {
	args := o.Mock.Called(event)
	return args.Error(0)
}

// SetAggregateID is not implemented
func (o AggregatorMock) SetAggregateID(id string) {

}

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

// Save is a mock
func (r RepositoryMock) Save(events ...Event) (err error) {
	args := r.Called(events)
	return args.Error(0)
}

// Load is a mock
func (r RepositoryMock) Load(id string, aggr Aggregate) (deleted bool, err error) {
	args := r.Called(id, aggr)
	return args.Bool(0), args.Error(1)
}

var _ Store = &StoreMock{}
