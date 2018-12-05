package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/SKF/go-eventsource/eventsource"
	"github.com/SKF/go-utility/uuid"
)

// createTestEvents - create some random test events in sequence
func createTestEvents(db *sql.DB, numberOfEvents int, eventTypeList []string, eventDataList [][]byte) (result []eventsource.Record, err error) {
	result = []eventsource.Record{}

	store := New(db, "events")

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
	return
}

// deleteEvents - delete events (using the SequenceID)
func deleteEvents(db *sql.DB, events []eventsource.Record) error {
	for _, e := range events {
		_, err := db.Exec("DELETE from events WHERE sequence_id = $1", e.SequenceID)
		if err != nil {
			return err
		}
	}
	return nil
}
