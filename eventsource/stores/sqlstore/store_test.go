package sqlstore

import (
	"context"
	"database/sql"
	"math/rand"
	"os"

	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/SKF/go-eventsource/eventsource"
	"github.com/SKF/go-eventsource/eventsource/serializers/json"
	"github.com/SKF/go-utility/v2/uuid"

	_ "github.com/lib/pq"
	"github.com/oklog/ulid"
)

var ctx = context.TODO()

func TestLoadBySequenceID(t *testing.T) {
	if testing.Short() || os.Getenv("POSTGRES_CONN_STRING") == "" {
		t.Log("Skipping postgres e2e test")
		t.Skip()
	}

	db, err := sql.Open("postgres", os.Getenv("POSTGRES_CONN_STRING"))
	require.NoError(t, err, "Could not connect to db")
	defer db.Close()

	tableName, err := createTable(db)
	require.NoError(t, err, "Could not create table")

	eventTypes := []string{"EventTypeA", "EventTypeB", "EventTypeA", "EventTypeC", "EventTypeA"}
	events, err := createTestEvents(db, tableName, 10, eventTypes, [][]byte{[]byte("TestData")})
	require.NoError(t, err, "Failed to create events")
	defer func() {
		err = cleanup(db, tableName)
		require.NoError(t, err, "Could not perform DB cleanup")
	}()

	var records []eventsource.Record

	store := New(db, tableName)
	records, err = store.LoadBySequenceID(ctx, events[0].SequenceID, 0)
	assert.NoError(t, err, "LoadBySequenceID failed")
	assert.Equal(t, 9, len(records))

	records, err = store.LoadBySequenceID(ctx, events[len(events)-2].SequenceID, 0)
	assert.NoError(t, err, "LoadBySequenceID failed")
	assert.Equal(t, 1, len(records))

	records, err = store.LoadBySequenceIDAndType(ctx, events[0].SequenceID, "EventTypeA", 0)
	assert.NoError(t, err, "LoadBySequenceID failed")
	assert.Equal(t, 2, len(records))

	records, err = store.LoadBySequenceIDAndType(ctx, "", "EventTypeA", 0)
	assert.NoError(t, err, "LoadBySequenceID failed")
	assert.Equal(t, 3, len(records))

	records, err = store.LoadBySequenceIDAndType(ctx, "", "EventTypeA", 1)
	assert.NoError(t, err, "LoadBySequenceID failed")
	assert.Equal(t, 1, len(records))
}

func TestULID(t *testing.T) {
	var entropy = ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
	ulidNow := ulid.Now()
	var ulids []string
	ulidMap := make(map[string]int, 10000)
	for i := 0; i < 10000; i++ {
		ulids = append(ulids, ulid.MustNew(ulidNow, entropy).String())
		ulidMap[ulids[i]] = i
		if i > 0 {
			assert.True(t, ulids[i] > ulids[i-1])
		}
	}
	assert.Equal(t, len(ulidMap), 10000)
}

type TestEventA struct {
	*eventsource.BaseEvent
	TestString string `json:"testString"`
}

type TestEventB struct {
	*eventsource.BaseEvent
	TestInt int `json:"testInt"`
}

type TestObject struct {
	AggID  string
	FieldA string
	FieldB int
}

func (obj *TestObject) On(ctx context.Context, event eventsource.Event) error {
	switch v := event.(type) {
	case TestEventA:
		obj.FieldA += v.TestString
	case TestEventB:
		obj.FieldB += v.TestInt
	default:
		panic("Got unexpected event")
	}
	return nil
}

func (obj *TestObject) SetAggregateID(id string) {
	obj.AggID = id
}

func Test_SQLStoreE2E(t *testing.T) {
	if testing.Short() || os.Getenv("POSTGRES_CONN_STRING") == "" {
		t.Log("Skipping postgres e2e test")
		t.Skip()
	}

	db, err := sql.Open("postgres", os.Getenv("POSTGRES_CONN_STRING"))
	require.NoError(t, err, "Could not connect to db")
	defer db.Close()

	tableName, err := createTable(db)
	require.NoError(t, err, "Could not create table")
	defer func() {
		err = cleanup(db, tableName)
		require.NoError(t, err, "Could not perform DB cleanup")
	}()

	var aggregateID = uuid.New().String()
	var userIDA, userIDB = uuid.New().String(), uuid.New().String()

	repo := eventsource.NewRepository(New(db, tableName), json.NewSerializer(TestEventA{}, TestEventB{}))
	for _, event := range []eventsource.Event{
		TestEventA{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDA, Timestamp: 1}, TestString: "a"},
		TestEventA{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDB, Timestamp: 2}, TestString: "b"},
		TestEventA{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDA, Timestamp: 3}, TestString: "c"},
		TestEventA{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDB, Timestamp: 4}, TestString: "d"},
		TestEventB{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDA, Timestamp: 5}, TestInt: 1},
		TestEventB{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDB, Timestamp: 6}, TestInt: 2},
		TestEventB{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDA, Timestamp: 7}, TestInt: 3},
		TestEventB{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDB, Timestamp: 8}, TestInt: 4},
	} {
		err = repo.Save(ctx, event)
		assert.NoError(t, err, "Could not save event to DB")
	}

	var testObject TestObject
	deleted, err := repo.Load(ctx, aggregateID, &testObject)
	assert.NoError(t, err, "Could not load aggregate")
	assert.False(t, deleted)
	assert.Equal(t, aggregateID, testObject.AggID)
	assert.Equal(t, "abcd", testObject.FieldA)
	assert.Equal(t, 10, testObject.FieldB)

	events, err := repo.GetEventsBySequenceID(ctx, "", 0)
	assert.NoError(t, err, "Could not get events")
	assert.Equal(t, 8, len(events))
	assert.Equal(t, 8, events[0].GetTimestamp())
	assert.Equal(t, 1, events[len(events)].GetTimestamp())

	events, err = repo.GetEventsBySequenceID(ctx, events[len(events)-2].GetSequenceID(), 0)
	assert.NoError(t, err, "Could not get events")
	assert.Equal(t, 1, len(events))
}
