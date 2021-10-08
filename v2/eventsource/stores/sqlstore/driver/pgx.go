package driver

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"

	"github.com/SKF/go-eventsource/v2/eventsource"
	"github.com/SKF/go-utility/v2/uuid"
)

type PgxPool interface {
	Begin(context.Context) (pgx.Tx, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
}

type PGX struct {
	DB PgxPool
}

func (pgx *PGX) Load(ctx context.Context, query string, args []interface{}) (records []eventsource.Record, err error) {
	rows, err := pgx.DB.Query(ctx, query, args...)
	if err != nil {
		return records, errors.Wrap(err, "failed to load events using pgx")
	}

	for rows.Next() {
		var (
			record      eventsource.Record
			aggregateID uuid.UUID
			userID      uuid.UUID
		)

		// Scan aggregateID and userID to intermediate uuid, so they are transferred using binary representation
		if err = rows.Scan(
			&aggregateID, &record.SequenceID, &record.Timestamp,
			&userID, &record.Type, &record.Data,
		); err != nil {
			err = errors.Wrap(err, "failed to scan sql row")

			return
		}

		record.AggregateID = aggregateID.String()
		record.UserID = userID.String()

		records = append(records, record)
	}

	if err = rows.Err(); err != nil {
		err = errors.Wrap(err, "errors returned from sql store")

		return
	}

	return records, err
}

func (pgx *PGX) NewTransaction(ctx context.Context, query string, records ...eventsource.Record) (eventsource.StoreTransaction, error) {
	tx, err := pgx.DB.Begin(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start new transaction")
	}

	for _, record := range records {
		_, err = tx.Exec(ctx, query, uuid.UUID(record.AggregateID), record.SequenceID, record.Timestamp, uuid.UUID(record.UserID), record.Type, record.Data)
		if err != nil {
			return nil, errors.Wrap(err, "failed to execute query")
		}
	}

	return &pgxTransaction{
		sqlTx:   tx,
		records: records,
	}, nil
}

type pgxTransaction struct {
	sqlTx   pgx.Tx
	records []eventsource.Record
}

// Commit ...
func (tx *pgxTransaction) Commit() (err error) {
	if err := tx.sqlTx.Commit(context.Background()); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	return nil
}

func (tx *pgxTransaction) Rollback() error {
	if err := tx.sqlTx.Rollback(context.Background()); err != nil {
		return errors.Wrap(err, "failed to rollback transaction")
	}

	return nil
}

func (tx *pgxTransaction) GetRecords() []eventsource.Record {
	return tx.records
}
