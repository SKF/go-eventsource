package eventsource

import (
	"github.com/stretchr/testify/mock"
)

type serializerMock struct {
	*mock.Mock
}

//CreateSerializerMock create object
func CreateSerializerMock() *serializerMock {
	return &serializerMock{
		Mock: &mock.Mock{},
	}
}

type storeMock struct {
	*mock.Mock
}

//CreateStoreMock create object
func CreateStoreMock() *storeMock {
	return &storeMock{
		Mock: &mock.Mock{},
	}
}

type aggregatorMock struct {
	Mock *mock.Mock
}

//CreateAggregatorMock create object
func CreateAggregatorMock() *aggregatorMock {
	return &aggregatorMock{
		Mock: &mock.Mock{},
	}
}

func (o serializerMock) Unmarshal(data []byte, eventType string) (event Event, err error) {
	args := o.Called(data, eventType)
	return args.Get(0).(Event), args.Error(1)
}

func (o serializerMock) Marshal(event Event) (data []byte, err error) {
	args := o.Called(event)
	return args.Get(0).([]byte), args.Error(1)
}

func (o storeMock) Save(record Record) error {
	args := o.Called(record)
	return args.Error(0)
}
func (o storeMock) Load(id string) (record []Record, err error) {
	args := o.Called(id)
	return args.Get(0).([]Record), args.Error(1)
}

func (o aggregatorMock) On(event Event) error {
	args := o.Mock.Called(event)
	return args.Error(0)
}

func (o aggregatorMock) SetAggregateID(id string) {

}

// Repository ...
type repositoryMock struct {
	*mock.Mock
}

func CreateRepositoryMock() *repositoryMock {
	return &repositoryMock{
		Mock: &mock.Mock{},
	}
}

func (r repositoryMock) Save(events ...Event) (err error) {
	args := r.Called(events)
	return args.Error(0)
}

func (r repositoryMock) Load(id string, aggr Aggregate) (err error) {
	args := r.Called(id, aggr)
	return args.Error(0)
}
