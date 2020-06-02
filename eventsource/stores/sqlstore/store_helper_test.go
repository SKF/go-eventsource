package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/SKF/go-eventsource/eventsource"
	"github.com/SKF/go-utility/v2/uuid"
)

func randomTableName() string {
	numChars := 30
	rand.Seed(time.Now().UnixNano())
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	tableName := make([]rune, numChars)
	for i := 0; i < numChars; i++ {
		tableName[i] = letters[rand.Intn(len(letters))]
	}
	return string(tableName)
}

func createTable(db *sql.DB) (tableName string, err error) {
	tableName = randomTableName()
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (
    sequence_id character(26) PRIMARY KEY,
    aggregate_id uuid,
    user_id uuid,
    created_at bigint NOT NULL,
    type character varying(255),
    data bytea
)`, tableName))
	return
}

// createTestEventsForAggregate - create some random test events in sequence
func createTestEventsForAggregates(db *sql.DB, tableName string, numberOfAggregates int, numberOfEvents int, eventTypeList []string, eventDataList [][]byte) (result map[string][]eventsource.Record, err error) {
	result = map[string][]eventsource.Record{}

	store := New(db, tableName)

	aggregates := map[int]string{}
	for i := 0; i < numberOfAggregates; i++ {
		aggregates[i] = uuid.New().String()
	}

	for i := 0; i < numberOfEvents; i++ {

		aggID := aggregates[rand.Intn(len(aggregates)-0)+0]
		fmt.Println(aggregates)

		fmt.Println(aggID)
		userID := uuid.New()
		eventType := fmt.Sprintf("TestEvent %d", i+1)
		if i < len(eventTypeList) {
			eventType = eventTypeList[i]
		}
		eventData := []byte(fmt.Sprintf("TestEventData %d", i+1))
		if i < len(eventDataList) {
			eventData = eventDataList[i]
		}

		event := eventsource.Record{
			AggregateID: aggID,
			UserID:      userID.String(),
			SequenceID:  eventsource.NewULID(),
			Type:        eventType,
			Timestamp:   time.Now().UnixNano(),
			Data:        eventData,
		}

		ctx := context.Background()

		var tx eventsource.StoreTransaction
		if tx, err = store.NewTransaction(ctx, event); err != nil {
			return
		}

		if err = tx.Commit(); err != nil {
			return
		}

		var records []eventsource.Record
		records, err = store.LoadByAggregate(ctx, aggID)
		if err != nil {
			return
		}
		if len(records) != len(result[aggID])+1 {
			err = fmt.Errorf("Expected %v result from store, got %d", len(result[aggID])+1, len(records))
			return
		}
		event.SequenceID = records[len(records)-1].SequenceID
		if !reflect.DeepEqual(event, records[len(records)-1]) {
			err = fmt.Errorf("Expected identical records, saved: %v  loaded: %v", event, records[len(records)-1])
			return
		}

		result[aggID] = records
	}
	return result, err
}

// createTestEvents - create some random test events in sequence
func createTestEvents(db *sql.DB, tableName string, numberOfEvents int, eventTypeList []string, eventDataList [][]byte) (result []eventsource.Record, err error) {
	result = []eventsource.Record{}

	store := New(db, tableName)

	for i := 0; i < numberOfEvents; i++ {
		aggID := uuid.New()
		userID := uuid.New()
		eventType := fmt.Sprintf("TestEvent %d", i+1)
		if i < len(eventTypeList) {
			eventType = eventTypeList[i]
		}
		eventData := []byte(fmt.Sprintf("TestEventData %d", i+1))
		if i < len(eventDataList) {
			eventData = eventDataList[i]
		}

		event := eventsource.Record{
			AggregateID: aggID.String(),
			UserID:      userID.String(),
			SequenceID:  eventsource.NewULID(),
			Type:        eventType,
			Timestamp:   time.Now().UnixNano(),
			Data:        eventData,
		}

		ctx := context.Background()

		var tx eventsource.StoreTransaction
		if tx, err = store.NewTransaction(ctx, event); err != nil {
			return
		}

		if err = tx.Commit(); err != nil {
			return
		}

		var records []eventsource.Record
		records, err = store.LoadByAggregate(ctx, aggID.String())
		if err != nil {
			return
		}
		if len(records) != 1 {
			err = fmt.Errorf("Expected one result from store, got %d", len(records))
			return
		}
		event.SequenceID = records[0].SequenceID
		if !reflect.DeepEqual(event, records[0]) {
			err = fmt.Errorf("Expected identical records, saved: %v  loaded: %v", event, records[0])
			return
		}
		result = append(result, records[0])
	}
	return result, err
}

// deleteEvents - delete events (using the SequenceID)
func cleanup(db *sql.DB, tableName string) error {
	_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
	return err
}
