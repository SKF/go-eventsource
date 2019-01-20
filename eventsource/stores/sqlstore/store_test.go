package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	//"strings"
	"testing"
	"time"

	"github.com/SKF/go-eventsource/eventsource"
	"github.com/SKF/go-eventsource/eventsource/serializers/json"
	"github.com/SKF/go-utility/uuid"
	"github.com/stretchr/testify/assert"

	_ "github.com/lib/pq"
	"github.com/oklog/ulid"
)

func TestLoadBySequenceID(t *testing.T) {
	if testing.Short() || os.Getenv("POSTGRES_CONN_STRING") == "" {
		t.Log("Skipping postgres e2e test")
		t.Skip()
	}

	db, err := sql.Open("postgres", os.Getenv("POSTGRES_CONN_STRING"))
	if err != nil {
		t.Errorf("Could not connect to db: %s", err)
	}
	defer db.Close()

	eventTypes := []string{"EventTypeA", "EventTypeB", "EventTypeA", "EventTypeC", "EventTypeA"}
	events, err := createTestEvents(db, 10, eventTypes, [][]byte{[]byte("TestData")})
	if err != nil {
		t.Errorf("unable to create events err: %v", err)
	}

	var records []eventsource.Record

	store := New(db, "events")
	records, err = store.LoadBySequenceID(context.Background(), events[0].SequenceID)

	if err != nil {
		t.Errorf("LoadBySequenceID failed with: %s", err)
	}
	if len(records) != 9 {
		t.Errorf("Expected nine records from store, got %d", len(records))
	}

	records, err = store.LoadBySequenceID(context.Background(), events[len(events)-2].SequenceID)

	if err != nil {
		t.Errorf("LoadBySequenceID failed with: %s", err)
	}
	if len(records) != 1 {
		t.Errorf("Expected one record from store, got %d", len(records))
	}

	records, err = store.LoadBySequenceIDAndType(context.Background(), events[0].SequenceID, "EventTypeA")

	if err != nil {
		t.Errorf("LoadBySequenceIDAndType failed with: %s", err)
	}
	if len(records) != 2 {
		t.Errorf("Expected two records from store, got %d", len(records))
	}

	records, err = store.LoadBySequenceIDAndType(context.Background(), "", "EventTypeA")

	if err != nil {
		t.Errorf("LoadBySequenceIDAndType failed with: %s", err)
	}
	if len(records) != 3 {
		t.Errorf("Expected three records from store, got %d", len(records))
	}

	err = deleteEvents(db, events)
	if err != nil {
		t.Errorf("Unable to delete events: %v got err:%v", events, err)
	}
}

func TestSaveLoad(t *testing.T) {
	if testing.Short() || os.Getenv("POSTGRES_CONN_STRING") == "" {
		t.Log("Skipping postgres e2e test")
		t.Skip()
	}

	db, err := sql.Open("postgres", os.Getenv("POSTGRES_CONN_STRING"))
	if err != nil {
		t.Errorf("Could not connect to db: %s", err)
	}
	defer db.Close()

	events, err := createTestEvents(db, 10, []string{"Testing1", "Testing2"}, [][]byte{[]byte("TestData")})
	if err != nil {
		t.Errorf("unable to create events err: %v", err)
	}
	err = deleteEvents(db, events)
	if err != nil {
		t.Errorf("Unable to delete events: %v got err:%v", events, err)
	}
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
		obj.FieldA = obj.FieldA + v.TestString
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
	if err != nil {
		t.Errorf("Could not connect to db: %s", err)
	}
	defer db.Close()

	tmpTableName := "hejsan" //strings.Replace(uuid.New().String(), "-", "", -1)
	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTableName))
	assert.Nil(t, err, "Could not drop old event table")
	_, err = db.Exec(fmt.Sprintf(`-- Table Definition ----------------------------------------------
CREATE TABLE %s (
    sequence_id character(26) PRIMARY KEY,
    aggregate_id uuid,
    user_id uuid,
    created_at bigint NOT NULL,
    type character varying(255),
    data bytea
)`, tmpTableName))
	assert.Nil(t, err, "Could not create new event table")

	var aggregateID = uuid.New().String()
	var userIDA, userIDB = uuid.New().String(), uuid.New().String()

	ctx := context.Background()
	repo := eventsource.NewRepository(New(db, tmpTableName), json.NewSerializer(TestEventA{}, TestEventB{}))
	err = repo.Save(ctx, TestEventA{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDA, Timestamp: 1}, TestString: "a"})
	assert.Nil(t, err, "Could not save event to DB")
	err = repo.Save(ctx, TestEventA{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDB, Timestamp: 2}, TestString: "b"})
	assert.Nil(t, err, "Could not save event to DB")
	err = repo.Save(ctx, TestEventA{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDA, Timestamp: 3}, TestString: "c"})
	assert.Nil(t, err, "Could not save event to DB")
	err = repo.Save(ctx, TestEventA{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDB, Timestamp: 4}, TestString: "d"})
	assert.Nil(t, err, "Could not save event to DB")
	err = repo.Save(ctx, TestEventB{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDA, Timestamp: 1}, TestInt: 1})
	assert.Nil(t, err, "Could not save event to DB")
	err = repo.Save(ctx, TestEventB{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDB, Timestamp: 2}, TestInt: 2})
	assert.Nil(t, err, "Could not save event to DB")
	err = repo.Save(ctx, TestEventB{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDA, Timestamp: 3}, TestInt: 3})
	assert.Nil(t, err, "Could not save event to DB")
	err = repo.Save(ctx, TestEventB{BaseEvent: &eventsource.BaseEvent{AggregateID: aggregateID, UserID: userIDB, Timestamp: 4}, TestInt: 4})
	assert.Nil(t, err, "Could not save event to DB")

	var testObject TestObject
	if deleted, err := repo.Load(ctx, aggregateID, &testObject); err != nil {
		t.Errorf("Could not connect to db: %s", err)
	} else if deleted {
		t.Errorf("Could not load aggregate id %s: %s", aggregateID, err)
	}
	assert.Equal(t, aggregateID, testObject.AggID)
	assert.Equal(t, "abcd", testObject.FieldA)
	assert.Equal(t, 10, testObject.FieldB)

	events, err := repo.GetEventsBySequenceID(ctx, "")
	assert.Nil(t, err, "Could not get events")
	assert.Equal(t, 8, len(events))
	events, err = repo.GetEventsBySequenceID(ctx, events[len(events)-2].GetSequenceID())
	assert.Nil(t, err, "Could not get events")
	assert.Equal(t, 1, len(events))

	db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTableName))
}
