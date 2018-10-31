package sqlstore_test

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/SKF/go-eventsource/eventsource"
	"github.com/SKF/go-eventsource/eventsource/stores/sqlstore"
	"github.com/SKF/go-utility/uuid"
	_ "github.com/lib/pq"
)

func CreateEvents(db *sql.DB, numberOfEvents int) (result []eventsource.Record, err error) {
	result = []eventsource.Record{}

	store := sqlstore.New(db, "events")

	for i := 0; i < numberOfEvents; i++ {
		aggID := uuid.New()
		userID := uuid.New()
		event := eventsource.Record{
			AggregateID: aggID.String(),
			UserID:      userID.String(),
			Type:        fmt.Sprintf("TestEvent %d", i+1),
			Timestamp:   time.Now().UTC(),
			Data:        []byte("hejhopp"),
		}

		if err = store.Save(event); err != nil {
			return
		}
		var records []eventsource.Record
		records, err = store.Load(aggID.String())
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

func DeleteEvents(db *sql.DB, events []eventsource.Record) error {
	for _, e := range events {
		_, err := db.Exec("DELETE from events WHERE sequence_id = $1", e.SequenceID)
		if err != nil {
			return err
		}
	}
	return nil
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

	events, err := CreateEvents(db, 10)
	if err != nil {
		t.Errorf("unable to create events err: %v", err)
	}
	err = DeleteEvents(db, events)
	if err != nil {
		t.Errorf("Unable to delete events: %v got err:%v", events, err)
	}
}
