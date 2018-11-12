package sqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/SKF/go-eventsource/eventsource"
)

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
func (store *store) Save(records ...eventsource.Record) (err error) {
	return store.SaveWithContext(context.Background(), records...)
}

// SaveWithContext ...
func (store *store) SaveWithContext(ctx context.Context, records ...eventsource.Record) (err error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(saveSQL, store.tablename))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, record := range records {
		_, err = stmt.ExecContext(ctx, record.AggregateID, record.SequenceID, record.Timestamp, record.UserID, record.Type, record.Data)
		if err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = fmt.Errorf("Rollback error: %v, source: %v", errRollback, err)
		}

		return err
	}

	return nil
}

// Load ...
func (store *store) Load(id string) (records []eventsource.Record, err error) {
	return store.LoadWithContext(context.Background(), id)
}

// LoadWithContext ...
func (store *store) LoadWithContext(ctx context.Context, id string) (records []eventsource.Record, err error) {
	stmt, err := store.db.PrepareContext(ctx, fmt.Sprintf(loadSQL, store.tablename))
	if err != nil {
		return
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx, id)
	for rows.Next() {
		var record eventsource.Record
		if err = rows.Scan(
			&record.AggregateID, &record.SequenceID, &record.Timestamp,
			&record.UserID, &record.Type, &record.Data,
		); err != nil {
			return
		}
		records = append(records, record)
	}
	if err = rows.Err(); err != nil {
		return
	}
	return
}
