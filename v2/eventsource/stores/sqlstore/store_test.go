package sqlstore_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/SKF/go-eventsource/v2/eventsource"
	"github.com/SKF/go-eventsource/v2/eventsource/serializers/json"
	"github.com/SKF/go-eventsource/v2/eventsource/stores/sqlstore"
	"github.com/SKF/go-utility/v2/uuid"
)

var ctx = context.TODO()

type TestEventA struct {
	*eventsource.BaseEvent
	TestString string `json:"testString"`
}

type TestEventB struct {
	*eventsource.BaseEvent
	TestInt int `json:"testInt"`
}

type TestEventPosition struct {
	*eventsource.BaseEvent
	Position int
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

type testFunc func(*testing.T, eventsource.Store)

var allTests = map[string]testFunc{
	"Load by sequence ID":               testLoadBySequenceID,
	"Populate object by loading events": testLoadAggregate,
	"Load events given options":         testLoadEventOptions,
	"Test behaviour of ULIDs":           testULID,
}

func wrapTest(tf testFunc, store eventsource.Store) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()
		tf(t, store)
	}
}

func TestGenericDriver(t *testing.T) { // nolint:paralleltest
	for name, test := range allTests { // nolint:paralleltest
		db, tableName := setupDB(t)
		store := sqlstore.New(db, tableName)
		t.Run(name, wrapTest(test, store))
		cleanupDBGeneric(t, db, tableName)
	}
}

func TestPgxDriver(t *testing.T) { // nolint:paralleltest
	for name, test := range allTests { // nolint:paralleltest
		db, tableName := setupDBPgx(t)
		store := sqlstore.NewPgx(db, tableName)
		t.Run(name, wrapTest(test, store))
		cleanupDBPgx(t, db, tableName)
	}
}

func testLoadBySequenceID(t *testing.T, store eventsource.Store) { // nolint:thelper
	eventTypes := []string{"EventTypeA", "EventTypeB", "EventTypeA", "EventTypeC", "EventTypeA"}
	events, err := createTestEvents(store, 10, eventTypes, [][]byte{[]byte("TestData")})
	require.NoError(t, err, "Failed to create events")

	var records []eventsource.Record

	records, err = store.Load(ctx, sqlstore.BySequenceID(events[0].SequenceID))
	assert.NoError(t, err, "Load failed")
	assert.Equal(t, 9, len(records))

	records, err = store.Load(ctx, sqlstore.BySequenceID(events[len(events)-2].SequenceID))
	assert.NoError(t, err, "Load failed")
	assert.Equal(t, 1, len(records))

	records, err = store.Load(ctx, sqlstore.BySequenceID(events[0].SequenceID), sqlstore.ByType("EventTypeA"))
	assert.NoError(t, err, "Load failed")
	assert.Equal(t, 2, len(records))

	records, err = store.Load(ctx, sqlstore.BySequenceID(""), sqlstore.ByType("EventTypeA"))
	assert.NoError(t, err, "Load failed")
	assert.Equal(t, 3, len(records))

	records, err = store.Load(ctx, sqlstore.BySequenceID(""), sqlstore.ByType("EventTypeA"), sqlstore.WithLimit(1))
	assert.NoError(t, err, "Load failed")
	assert.Equal(t, 1, len(records))
}

func testULID(t *testing.T, _ eventsource.Store) { // nolint:thelper
	var (
		entropy = ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0) // nolint:gosec
		ulidNow = ulid.Now()
		ulids   []string
		ulidMap = make(map[string]int, 10000)
	)

	for i := 0; i < 10000; i++ {
		ulids = append(ulids, ulid.MustNew(ulidNow, entropy).String())
		ulidMap[ulids[i]] = i

		if i > 0 {
			assert.True(t, ulids[i] > ulids[i-1])
		}
	}
	assert.Equal(t, len(ulidMap), 10000)
}

func testLoadAggregate(t *testing.T, store eventsource.Store) { // nolint:thelper
	aggregateID := uuid.New().String()
	userIDA, userIDB := uuid.New().String(), uuid.New().String()

	repo := eventsource.NewRepository(store, json.NewSerializer(TestEventA{}, TestEventB{})) // nolint:exhaustivestruct
	for _, event := range []eventsource.Event{
		TestEventA{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDA, Timestamp: 1}, TestString: "a"}, // nolint:exhaustivestruct
		TestEventA{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDB, Timestamp: 2}, TestString: "b"}, // nolint:exhaustivestruct
		TestEventA{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDA, Timestamp: 3}, TestString: "c"}, // nolint:exhaustivestruct
		TestEventA{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDB, Timestamp: 4}, TestString: "d"}, // nolint:exhaustivestruct
		TestEventB{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDA, Timestamp: 5}, TestInt: 1},      // nolint:exhaustivestruct
		TestEventB{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDB, Timestamp: 6}, TestInt: 2},      // nolint:exhaustivestruct
		TestEventB{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDA, Timestamp: 7}, TestInt: 3},      // nolint:exhaustivestruct
		TestEventB{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDB, Timestamp: 8}, TestInt: 4},      // nolint:exhaustivestruct
	} {
		err := repo.Save(ctx, event)
		assert.NoError(t, err, "Could not save event to DB")
	}

	var testObject TestObject
	deleted, err := repo.Load(ctx, aggregateID, &testObject)
	assert.NoError(t, err, "Could not load aggregate")
	assert.False(t, deleted)
	assert.Equal(t, aggregateID, testObject.AggID)
	assert.Equal(t, "abcd", testObject.FieldA)
	assert.Equal(t, 10, testObject.FieldB)

	events, err := repo.LoadEvents(ctx, sqlstore.BySequenceID(""))
	assert.NoError(t, err, "Could not get events")

	if assert.Equal(t, 8, len(events)) {
		events, err = repo.LoadEvents(ctx, sqlstore.BySequenceID(events[len(events)-2].GetSequenceID()))
		assert.NoError(t, err, "Could not get events")
		assert.Equal(t, 1, len(events))
	}
}

func testLoadEventOptions(t *testing.T, store eventsource.Store) { // nolint:thelper
	repo := eventsource.NewRepository(store, json.NewSerializer(TestEventPosition{})) // nolint:exhaustivestruct

	var (
		aggregateID = uuid.New().String()
		testData    = []TestEventPosition{}
		testSize    = 15
	)

	require.True(t, testSize > 5, "testSize needs to be more than five")

	for i := 1; i <= testSize; i++ {
		testData = append(testData, TestEventPosition{
			BaseEvent: &eventsource.BaseEvent{ // nolint:exhaustivestruct
				AggregateID: aggregateID,
				UserID:      uuid.New().String(),
			},
			Position: i,
		})
	}

	for _, event := range testData {
		err := repo.Save(ctx, event)
		assert.NoError(t, err, "Could not save event to DB")
	}

	events, err := repo.LoadEvents(ctx, sqlstore.BySequenceID(""), sqlstore.WithAscending())
	assert.NoError(t, err, "Could not get events")
	require.Equal(t, len(testData), len(events))
	assert.Equal(t, testData[0].Position, events[0].(TestEventPosition).Position)
	assert.Equal(t, testData[testSize-4].Position, events[testSize-4].(TestEventPosition).Position)
	assert.Equal(t, testData[testSize-1].Position, events[testSize-1].(TestEventPosition).Position)

	events, err = repo.LoadEvents(ctx, sqlstore.BySequenceID(""), sqlstore.WithDescending())
	assert.NoError(t, err, "Could not get events")
	require.Equal(t, len(testData), len(events))
	assert.Equal(t, testData[testSize-1].Position, events[0].(TestEventPosition).Position)
	assert.Equal(t, testData[testSize-4].Position, events[3].(TestEventPosition).Position)
	assert.Equal(t, testData[0].Position, events[testSize-1].(TestEventPosition).Position)

	var (
		limit  = 5
		offset = 1
	)

	events, err = repo.LoadEvents(ctx, sqlstore.BySequenceID(""), sqlstore.WithOffset(offset), sqlstore.WithLimit(limit))

	assert.NoError(t, err, "Could not get events")
	require.Equal(t, limit, len(events))
	assert.Equal(t, testData[offset].Position, events[0].(TestEventPosition).Position)
	assert.Equal(t, testData[offset+4].Position, events[4].(TestEventPosition).Position)
}
