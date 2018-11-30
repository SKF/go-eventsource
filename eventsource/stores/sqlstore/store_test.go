package sqlstore

import (
	"context"
	"database/sql"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/SKF/go-eventsource/eventsource"

	_ "github.com/lib/pq"
	"github.com/oklog/ulid"
)

func TestLoadNewerThan(t *testing.T) {
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

	var records []eventsource.Record

	store := New(db, "events")
	records, err = store.LoadNewerThan(context.Background(), events[0].SequenceID)

	if err != nil {
		t.Errorf("LoadNewerThan failed with: %s", err)
	}
	if len(records) != 9 {
		t.Errorf("Expected nine records from store, got %d", len(records))
	}

	records, err = store.LoadNewerThan(context.Background(), events[len(events)-2].SequenceID)

	if err != nil {
		t.Errorf("LoadNewerThan failed with: %s", err)
	}
	if len(records) != 1 {
		t.Errorf("Expected one record from store, got %d", len(records))
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
