package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/SKF/go-eventsource/v2/eventsource"
)

type store struct {
	db        *sql.DB
	tablename string
}

const (
	saveSQL                    = "INSERT INTO %s (aggregate_id, sequence_id, created_at, user_id, type, data) VALUES ($1, $2, $3, $4, $5, $6)"
	loadAggregateSQL           = "SELECT aggregate_id, sequence_id, created_at, user_id, type, data FROM %s WHERE aggregate_id = $1"
	loadBySequenceIDSQL        = "SELECT aggregate_id, sequence_id, created_at, user_id, type, data FROM %s WHERE sequence_id > $1"
	loadBySequenceIDAndTypeSQL = "SELECT aggregate_id, sequence_id, created_at, user_id, type, data FROM %s WHERE sequence_id > $1 AND type = $2"
	loadByTimestampSQL         = "SELECT aggregate_id, sequence_id, created_at, user_id, type, data FROM %s WHERE created_at > $1"
)

// New ...
func New(db *sql.DB, tableName string) eventsource.Store {
	return &store{
		db:        db,
		tablename: tableName,
	}
}

func (store *store) buildQuery(queryOpts []eventsource.QueryOption, query string) string {
	fullQuery := []string{fmt.Sprintf(query, store.tablename)}
	opts := evaluateQueryOptions(queryOpts)

	if opts.descending {
		fullQuery = append(fullQuery, "ORDER BY sequence_id DESC")
	} else {
		fullQuery = append(fullQuery, "ORDER BY sequence_id ASC")
	}

	if opts.limit != nil {
		fullQuery = append(fullQuery, fmt.Sprintf("LIMIT %d", *opts.limit))
	}

	if opts.offset != nil {
		fullQuery = append(fullQuery, fmt.Sprintf("OFFSET %d", *opts.offset))
	}

	return strings.Join(fullQuery, " ")
}

func (store *store) fetchRecords(ctx context.Context, queryOpts []eventsource.QueryOption, query string, args ...interface{}) (records []eventsource.Record, err error) {
	fullQuery := store.buildQuery(queryOpts, query)
	stmt, err := store.db.PrepareContext(ctx, fullQuery)
	if err != nil {
		err = errors.Wrap(err, "failed to prepare sql query")
		return
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
		return
	}
	for rows.Next() {
		var record eventsource.Record
		if err = rows.Scan(
			&record.AggregateID, &record.SequenceID, &record.Timestamp,
			&record.UserID, &record.Type, &record.Data,
		); err != nil {
			err = errors.Wrap(err, "failed to scan sql row")
			return
		}
		records = append(records, record)
	}

	if err = rows.Err(); err != nil {
		err = errors.Wrap(err, "errors returned from sql store")
		return
	}
	return records, err
}

// Load ...
func (store *store) LoadByAggregate(ctx context.Context, aggregateID string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return store.fetchRecords(ctx, opts, loadAggregateSQL, aggregateID)
}

func (store *store) LoadBySequenceID(ctx context.Context, sequenceID string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return store.fetchRecords(ctx, opts, loadBySequenceIDSQL, sequenceID)
}

func (store *store) LoadBySequenceIDAndType(ctx context.Context, sequenceID string, eventType string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return store.fetchRecords(ctx, opts, loadBySequenceIDAndTypeSQL, sequenceID, eventType)
}

func (store *store) LoadByTimestamp(ctx context.Context, timestamp int64, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return store.fetchRecords(ctx, opts, loadByTimestampSQL, timestamp)
}
