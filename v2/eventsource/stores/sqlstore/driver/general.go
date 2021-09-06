package driver

import (
	"context"
	"database/sql"

	"github.com/pkg/errors"

	"github.com/SKF/go-eventsource/v2/eventsource"
)

type General struct {
	DB *sql.DB
}

func New(db *sql.DB) *General {
	return &General{DB: db}
}

func (dwWrap *General) Load(ctx context.Context, query string, args []interface{}) (records []eventsource.Record, err error) {
	stmt, err := dwWrap.DB.PrepareContext(ctx, query)
	if err != nil {
		return records, errors.Wrap(err, "failed to prepare sql query")
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
	defer rows.Close()

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

func (dwWrap *General) NewTransaction(ctx context.Context, query string, records ...eventsource.Record) (eventsource.StoreTransaction, error) {
	tx, err := dwWrap.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start new transaction")
	}

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to prepare query")
	}
	defer stmt.Close()

	for _, record := range records {
		_, err = stmt.ExecContext(ctx, record.AggregateID, record.SequenceID, record.Timestamp, record.UserID, record.Type, record.Data)
		if err != nil {
			return nil, errors.Wrap(err, "failed to execute query")
		}
	}

	return &generalTransaction{
		sqlTx:   tx,
		records: records,
	}, nil
}

type generalTransaction struct {
	sqlTx   *sql.Tx
	records []eventsource.Record
}

// Commit ...
func (tx *generalTransaction) Commit() (err error) {
	if err := tx.sqlTx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	return nil
}

func (tx *generalTransaction) Rollback() error {
	if err := tx.sqlTx.Rollback(); err != nil {
		return errors.Wrap(err, "failed to rollback transaction")
	}

	return nil
}

func (tx *generalTransaction) GetRecords() []eventsource.Record {
	return tx.records
}
