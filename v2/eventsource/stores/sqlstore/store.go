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
	columns = []string{"aggregate_id", "sequence_id", "created_at", "user_id", "type", "data"}
	saveSQL = "INSERT INTO %s (aggregate_id, sequence_id, created_at, user_id, type, data) VALUES ($1, $2, $3, $4, $5, $6)"
	loadSQL = "SELECT aggregate_id, sequence_id, created_at, user_id, type, data FROM %s"
)

// New ...
func New(db *sql.DB, tableName string) eventsource.Store {
	return &store{
		db:        db,
		tablename: tableName,
	}
}

func columnExist(key string) bool {
	for _, column := range columns {
		if key == column {
			return true
		}
	}

	return false
}

func (store *store) buildQuery(queryOpts []eventsource.QueryOption, query string) (returnedQuery string, args []interface{}, err error) {
	fullQuery := []string{fmt.Sprintf(query, store.tablename)}
	opts := evaluateQueryOptions(queryOpts)

	if len(opts.where) > 0 {
		fullQuery = append(fullQuery, "WHERE")
	}

	for key, data := range opts.where {
		if !columnExist(key) {
			err = errors.Errorf("Column '%s' cannot be applied to", key)
			return
		}
		args = append(args, data.value)
		fullQuery = append(fullQuery, fmt.Sprintf("%s %s $%d", key, data.operator, len(args)))
	}

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

	returnedQuery = strings.Join(fullQuery, " ")
	return
}

func (store *store) fetchRecords(ctx context.Context, queryOpts []eventsource.QueryOption, query string) (records []eventsource.Record, err error) {
	fullQuery, args := store.buildQuery(queryOpts, query)
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
func (store *store) Load(ctx context.Context, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return store.fetchRecords(ctx, opts, loadSQL)
}

// Deprecated functions
func (store *store) LoadByAggregate(ctx context.Context, aggregateID string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return store.Load(ctx, append(opts, Equals("aggregate_id", aggregateID))...)
}

func (store *store) LoadBySequenceID(ctx context.Context, sequenceID string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return store.Load(ctx, append(opts, GreaterThan("sequence_id", sequenceID))...)
}

func (store *store) LoadBySequenceIDAndType(ctx context.Context, sequenceID string, eventType string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return store.Load(ctx, append(opts, GreaterThan("sequence_id", sequenceID), Equals("type", eventType))...)
}

func (store *store) LoadByTimestamp(ctx context.Context, timestamp int64, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return store.Load(ctx, append(opts, GreaterThan("created_at", timestamp))...)
}
