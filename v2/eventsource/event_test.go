package eventsource

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Getters(t *testing.T) {
	var aggID = "123"
	var userID = "testUser"
	var ulid = "ULID"
	var timestamp int64 = 123
	var testEvent = BaseEvent{aggID, userID, ulid, timestamp}

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
	var aggID = "123"
	var userID = "testUser"
	var ulid = "ULID"
	var ulid2 = "CHANGCED_ULID"
	var timestamp int64 = 123
	var timestamp2 int64 = 456
	var testEvent = BaseEvent{aggID, userID, ulid, timestamp}

	testEvent.SetSequenceID(ulid2)
	testEvent.SetTimestamp(timestamp2)
	assert.Equal(t, ulid2, testEvent.GetSequenceID())
	assert.Equal(t, timestamp2, testEvent.GetTimestamp())
}

func Test_GetTypeName(t *testing.T) {
	type TestEvent struct {
		*BaseEvent
	}
	var tests = []struct {
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
