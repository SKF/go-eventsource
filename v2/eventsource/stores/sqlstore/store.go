package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/pkg/errors"

	"github.com/SKF/go-eventsource/v2/eventsource"
	"github.com/SKF/go-eventsource/v2/eventsource/stores/sqlstore/driver"
)

type EventDB interface {
	Load(ctx context.Context, query string, args []interface{}) ([]eventsource.Record, error)
	NewTransaction(ctx context.Context, query string, records ...eventsource.Record) (eventsource.StoreTransaction, error)
}

type PGXStore interface {
	eventsource.Store
	WithPostgresNotify() PGXStore
}

type store struct {
	db        EventDB
	tableName string
}

var (
	columns = []column{columnAggregateID, columnSequenceID, columnCreatedAt, columnUserID, columnType, columnData}
	saveSQL = "INSERT INTO %s (aggregate_id, sequence_id, created_at, user_id, type, data) VALUES ($1, $2, $3, $4, $5, $6)"
	loadSQL = "SELECT aggregate_id, sequence_id, created_at, user_id, type, data FROM %s"
)

// New creates a new event source store.
func New(db *sql.DB, tableName string) eventsource.Store {
	return &store{
		db:        &driver.Generic{DB: db},
		tableName: tableName,
	}
}

// NewPgx creates a new event source store.
func NewPgx(db driver.PgxPool, tableName string) PGXStore {
	return &store{
		db:        &driver.PGX{DB: db, NotificationChannel: nil},
		tableName: tableName,
	}
}

func columnExist(key column) bool {
	return slices.Contains(columns, key)
}

func (s *store) WithPostgresNotify() PGXStore {
	if db, ok := s.db.(*driver.PGX); ok {
		db.NotificationChannel = &s.tableName
	}

	return s
}

func (s *store) NewTransaction(ctx context.Context, records ...eventsource.Record) (eventsource.StoreTransaction, error) {
	return s.db.NewTransaction(ctx, fmt.Sprintf(saveSQL, s.tableName), records...) // nolint:wrapcheck
}

func (s *store) buildQuery(queryOpts []eventsource.QueryOption, query string) (string, []any, error) {
	fullQuery := []string{fmt.Sprintf(query, s.tableName)}
	opts := evaluateQueryOptions(queryOpts)

	var args []any
	if len(opts.where) > 0 {
		whereStatements := make([]string, 0, len(opts.where))

		for key, data := range opts.where {
			if !columnExist(key) {
				return "", args, errors.Errorf("column '%s' cannot be applied to", key)
			}

			args = append(args, data.value)
			whereStatements = append(whereStatements, fmt.Sprintf("%s %s $%d", key, data.operator, len(args)))
		}

		whereQuery := strings.Join(whereStatements, " AND ")
		fullQuery = append(fullQuery, "WHERE", whereQuery)
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

	return strings.Join(fullQuery, " "), args, nil
}

func (s *store) fetchRecords(ctx context.Context, queryOpts []eventsource.QueryOption, query string) (records []eventsource.Record, err error) {
	fullQuery, args, err := s.buildQuery(queryOpts, query)
	if err != nil {
		return
	}

	return s.db.Load(ctx, fullQuery, args) // nolint:wrapcheck
}

// Load will load records based on specified query options.
func (s *store) Load(ctx context.Context, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return s.fetchRecords(ctx, opts, loadSQL)
}

func (s *store) LoadByAggregate(ctx context.Context, aggregateID string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return s.Load(ctx, append(opts, equals(columnAggregateID, aggregateID))...)
}

// Deprecated.
func (s *store) LoadBySequenceID(ctx context.Context, sequenceID string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return s.Load(ctx, append(opts, BySequenceID(sequenceID))...)
}

// Deprecated.
func (s *store) LoadBySequenceIDAndType(ctx context.Context, sequenceID string, eventType string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return s.Load(ctx, append(opts, BySequenceID(sequenceID), ByType(eventType))...)
}

// Deprecated.
func (s *store) LoadByTimestamp(ctx context.Context, timestamp int64, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	return s.Load(ctx, append(opts, ByTimestamp(timestamp))...)
}
