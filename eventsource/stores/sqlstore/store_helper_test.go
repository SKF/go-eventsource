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

// createTestEvents - create some random test events in sequence
func createTestEvents(db *sql.DB, tableName string, numberOfEvents int, eventTypeList []string, eventDataList [][]byte) ([]eventsource.Record, error) {
	result := []eventsource.Record{}

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

		tx, err := store.NewTransaction(ctx, event)
		if err != nil {
			return result, err
		}

		if err = tx.Commit(); err != nil {
			return result, err
		}

		var records []eventsource.Record
		records, err = store.LoadByAggregate(ctx, aggID.String())
		if err != nil {
			return result, err
		}

		if len(records) != 1 {
			return result, fmt.Errorf("Expected one result from store, got %d", len(records))
		}

		event.SequenceID = records[0].SequenceID
		if !reflect.DeepEqual(event, records[0]) {
			return result, fmt.Errorf("Expected identical records, saved: %v  loaded: %v", event, records[0])
		}

		result = append(result, records[0])
	}

	return result, nil
}

// deleteEvents - delete events (using the SequenceID)
func cleanup(db *sql.DB, tableName string) error {
	_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
	return err
}
