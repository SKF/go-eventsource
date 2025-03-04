package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/pkg/errors"

	"github.com/SKF/go-eventsource/eventsource"
)

type store struct {
	db        *sql.DB
	tablename string
}

const (
	saveSQL                    = "INSERT INTO %s (aggregate_id, sequence_id, created_at, user_id, type, data) VALUES ($1, $2, $3, $4, $5, $6)"
	loadAggregateSQL           = "SELECT aggregate_id, sequence_id, created_at, user_id, type, data FROM %s WHERE aggregate_id = $1 ORDER BY sequence_id ASC LIMIT %s"
	loadBySequenceIDSQL        = "SELECT aggregate_id, sequence_id, created_at, user_id, type, data FROM %s WHERE sequence_id > $1 ORDER BY sequence_id ASC LIMIT %s"
	loadBySequenceIDAndTypeSQL = "SELECT aggregate_id, sequence_id, created_at, user_id, type, data FROM %s WHERE sequence_id > $1 AND type = $2 ORDER BY sequence_id ASC LIMIT %s"
	loadByTimestampSQL         = "SELECT aggregate_id, sequence_id, created_at, user_id, type, data FROM %s WHERE created_at > $1 ORDER BY created_at ASC LIMIT %s"
)

// New ...
func New(db *sql.DB, tableName string) eventsource.Store {
	return &store{
		db:        db,
		tablename: tableName,
	}
}

func (store *store) fetchRecords(ctx context.Context, query string, limit int, args ...interface{}) ([]eventsource.Record, error) {
	var limitStr string
	if limit == 0 {
		limitStr = "ALL"
	} else {
		limitStr = strconv.Itoa(limit)
	}

	records := []eventsource.Record{}
	stmt, err := store.db.PrepareContext(ctx, fmt.Sprintf(query, store.tablename, limitStr))
	if err != nil {
		err = errors.Wrap(err, "failed to prepare sql query")
		return records, err
	}

	defer func() {
		if errClose := stmt.Close(); errClose != nil {
			if err != nil {
				err = errors.Wrapf(err, "failed to close sql statement: %s", errClose)
			} else {
				err = errors.Wrap(errClose, "failed to close sql statement")
			}
		}
	}()

	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		err = errors.Wrap(err, "failed to execute sql query")
		return records, err
	}

	for rows.Next() {
		var record eventsource.Record
		if err = rows.Scan(
			&record.AggregateID, &record.SequenceID, &record.Timestamp,
			&record.UserID, &record.Type, &record.Data,
		); err != nil {
			err = errors.Wrap(err, "failed to scan sql row")
			return records, err
		}
		records = append(records, record)
	}

	if err = rows.Err(); err != nil {
		err = errors.Wrap(err, "errors returned from sql store")
		return records, err
	}

	return records, err
}

// Load ...
func (store *store) LoadByAggregate(ctx context.Context, aggregateID string) (records []eventsource.Record, err error) {
	return store.fetchRecords(ctx, loadAggregateSQL, 0, aggregateID)
}

func (store *store) LoadBySequenceID(ctx context.Context, sequenceID string, limit int) (records []eventsource.Record, err error) {
	return store.fetchRecords(ctx, loadBySequenceIDSQL, limit, sequenceID)
}

func (store *store) LoadBySequenceIDAndType(ctx context.Context, sequenceID string, eventType string, limit int) (records []eventsource.Record, err error) {
	return store.fetchRecords(ctx, loadBySequenceIDAndTypeSQL, limit, sequenceID, eventType)
}

func (store *store) LoadByTimestamp(ctx context.Context, timestamp int64, limit int) (records []eventsource.Record, err error) {
	return store.fetchRecords(ctx, loadByTimestampSQL, limit, timestamp)
}
