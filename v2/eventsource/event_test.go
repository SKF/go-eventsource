package eventsource

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Getters(t *testing.T) {
	var (
		aggID           = "123"
		userID          = "testUser"
		ulid            = "ULID"
		timestamp int64 = 123
		testEvent       = BaseEvent{aggID, userID, ulid, timestamp}
	)

	assert.Equal(t, aggID, testEvent.GetAggregateID())
	assert.Equal(t, testEvent.AggregateID, testEvent.GetAggregateID())
	assert.Equal(t, ulid, testEvent.GetSequenceID())
	assert.Equal(t, testEvent.SequenceID, testEvent.GetSequenceID())
	assert.Equal(t, timestamp, testEvent.GetTimestamp())
	assert.Equal(t, testEvent.Timestamp, testEvent.GetTimestamp())
	assert.Equal(t, userID, testEvent.GetUserID())
	assert.Equal(t, testEvent.UserID, testEvent.GetUserID())
}

func Test_Setters(t *testing.T) {
	var (
		aggID            = "123"
		userID           = "testUser"
		ulid             = "ULID"
		ulid2            = "CHANGCED_ULID"
		timestamp  int64 = 123
		timestamp2 int64 = 456
		testEvent        = BaseEvent{aggID, userID, ulid, timestamp}
	)

	testEvent.SetSequenceID(ulid2)
	testEvent.SetTimestamp(timestamp2)
	assert.Equal(t, ulid2, testEvent.GetSequenceID())
	assert.Equal(t, timestamp2, testEvent.GetTimestamp())
}

func Test_GetTypeName(t *testing.T) {
	type TestEvent struct {
		*BaseEvent
	}

	tests := []struct {
		event Event
		name  string
	}{
		{TestEvent{}, "TestEvent"},
		{&TestEvent{}, "TestEvent"},
	}

	for _, test := range tests {
		typeName := GetTypeName(test.event)
		if strings.Compare(typeName, test.name) != 0 {
			t.Errorf("Expected string representation %v to equal %s", typeName, test.name)
		}
	}
}
