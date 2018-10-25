package sqlstore_test

import (
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/SKF/go-eventsource/eventsource"
	"github.com/SKF/go-eventsource/eventsource/stores/sqlstore"
	_ "github.com/lib/pq"
)

func TestSaveLoad(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	db, err := sql.Open("postgres", os.Getenv("POSTGRES_CONN_STRING"))
	if err != nil {
		t.Errorf("Could not connect to db: %s", err)
	}
	defer db.Close()

	store := sqlstore.New(db, "events")

	aggID := "10261bb3-37f2-4718-8b44-112bbedf79bc"
	userID := "ef2f2eaa-7495-4c28-9814-33d3cdd89da7"
	event := eventsource.Record{
		AggregateID: aggID,
		UserID:      userID,
		Type:        "TestEvent",
		Timestamp:   time.Now().Unix(),
		Data:        []byte("hejhopp"),
	}
	if err := store.Save(event); err != nil {
		t.Errorf("Expected err to be nil: %s", err)
	}

	records, err := store.Load(aggID)
	if err != nil {
		t.Errorf("Expected err to be nil: %s", err)
	}
	if len(records) == 0 {
		t.Errorf("Expected results from store, got none")
	}
}
