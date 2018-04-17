package eventsource
import(
	"testing"

	"github.com/stretchr/testify/assert"
)

var e Event = &BaseEvent{}

func Test_GetAggregatedID(t *testing.T){
	var aggId = "123"
	var testEvent = BaseEvent{aggId, "dontcare"}
	assert.Equal(t, aggId, testEvent.GetAggregateID())
	assert.Equal(t, testEvent.AggregateID, testEvent.GetAggregateID())
}

func Test_GetUserID(t *testing.T){
	var userId = "testUser"
	var testEvent = BaseEvent{"dontcare", userId}
	assert.Equal(t, userId, testEvent.GetUserID())
	assert.Equal(t, testEvent.UserID, testEvent.GetUserID())
}
