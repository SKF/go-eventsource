package sqlstore

import (
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/SKF/go-eventsource/eventsource"
	"github.com/oklog/ulid"
)

var (
	entropy      = rand.New(rand.NewSource(time.Now().UnixNano()))
	entropyMutex sync.Mutex
)

func NewULID() string {
	entropyMutex.Lock()
	defer entropyMutex.Unlock()
	return ulid.MustNew(ulid.Now(), entropy).String()
}

type store struct {
	db        *sql.DB
	tablename string
}

const (
	saveSQL = "INSERT INTO %s (aggregate_id, sequence_id, created_at, user_id, type, data) VALUES ($1, $2, $3, $4, $5, $6)"
	loadSQL = "SELECT aggregate_id, sequence_id, created_at, user_id, type, data FROM %s WHERE aggregate_id = $1 ORDER BY sequence_id ASC LIMIT 1000000"
)

// New ...
func New(db *sql.DB, tableName string) eventsource.Store {
	return &store{
		db:        db,
		tablename: tableName,
	}
}

// Save ...
func (store *store) Save(record eventsource.Record) (err error) {
	stmt, err := store.db.Prepare(fmt.Sprintf(saveSQL, store.tablename))
	if err != nil {
		return
	}
	defer stmt.Close()

	_, err = stmt.Exec(record.AggregateID, NewULID(), record.Timestamp, record.UserID, record.Type, record.Data)
	if err != nil {
		return
	}
	return nil
}

// Load ...
func (store *store) Load(id string) (records []eventsource.Record, err error) {
	stmt, err := store.db.Prepare(fmt.Sprintf(loadSQL, store.tablename))
	if err != nil {
		return
	}
	defer stmt.Close()
	rows, err := stmt.Query(id)
	for rows.Next() {
		var record eventsource.Record
		if err = rows.Scan(&record.AggregateID, &record.SequenceID, &record.Timestamp, &record.UserID, &record.Type, &record.Data); err != nil {
			return
		}
		records = append(records, record)
	}
	if err = rows.Err(); err != nil {
		return
	}
	return
}
