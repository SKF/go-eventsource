package mock

import (
	"github.com/SKF/go-eventsource/eventsource"
	"github.com/stretchr/testify/mock"
)

type serializerMock struct {
	*mock.Mock
}

//CreateSerializerMock create object
func Create() *serializerMock {
	return &serializerMock{
		Mock: &mock.Mock{},
	}
}

func (o serializerMock) Unmarshal(data []byte, eventType string) (event eventsource.Event, err error) {
	args := o.Called(data, eventType)
	return args.Get(0).(eventsource.Event), args.Error(1)
}

func (o serializerMock) Marshal(event eventsource.Event) (data []byte, err error) {
	args := o.Called(event)
	return args.Get(0).([]byte), args.Error(1)
}
