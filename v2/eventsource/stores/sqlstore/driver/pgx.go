package driver

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"

	"github.com/SKF/go-eventsource/v2/eventsource"
)

type PGX struct {
	DB *pgxpool.Pool
}

func (pgx *PGX) Load(ctx context.Context, query string, args []interface{}) (records []eventsource.Record, err error) {
	rows, err := pgx.DB.Query(ctx, query, args)
	if err != nil {
		return records, errors.Wrap(err, "failed to load events using pgx")
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

func (pgx *PGX) NewTransaction(ctx context.Context, query string, records ...eventsource.Record) (eventsource.StoreTransaction, error) {
	tx, err := pgx.DB.Begin(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start new transaction")
	}

	for _, record := range records {
		_, err = tx.Exec(ctx, record.AggregateID, record.SequenceID, record.Timestamp, record.UserID, record.Type, record.Data)
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
