package sqlstore_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/SKF/go-eventsource/eventsource/stores/sqlstore"
	_ "github.com/lib/pq"
)

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

	events, err := sqlstore.CreateTestEvents(db, 10)
	if err != nil {
		t.Errorf("unable to create events err: %v", err)
	}
	err = sqlstore.DeleteEvents(db, events)
	if err != nil {
		t.Errorf("Unable to delete events: %v got err:%v", events, err)
	}
}
