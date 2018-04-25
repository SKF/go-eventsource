package eventsource

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var e Event = &BaseEvent{}

func Test_GetAggregatedID(t *testing.T) {
	var aggID = "123"
	var testEvent = BaseEvent{aggID, "dontcare"}
	assert.Equal(t, aggID, testEvent.GetAggregateID())
	assert.Equal(t, testEvent.AggregateID, testEvent.GetAggregateID())
}

func Test_GetUserID(t *testing.T) {
	var userID = "testUser"
	var testEvent = BaseEvent{"dontcare", userID}
	assert.Equal(t, userID, testEvent.GetUserID())
	assert.Equal(t, testEvent.UserID, testEvent.GetUserID())
}
