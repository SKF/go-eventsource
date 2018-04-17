package eventsource
import(
	"testing"

	"github.com/stretchr/testify/assert"
)

var e Event = &BaseEvent{}

func Test_GetAggregatedID(t *testing.T){
	var aggId = "123"
	var testEvent = BaseEvent{"", userId}
	assert.Equal(t, aggId, testEvent.GetAggregateID())
}

func Test_GetUserID(t *testing.T){
	var userId = "testUser"
	var testEvent = BaseEvent{"", userId}
	assert.Equal(t, userId, event.GetUserID())
}
