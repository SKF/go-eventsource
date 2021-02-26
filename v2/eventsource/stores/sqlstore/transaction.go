package sqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pkg/errors"

	"github.com/SKF/go-eventsource/v2/eventsource"
)

type transaction struct {
	sqlTx   *sql.Tx
	records []eventsource.Record
}

func (store *store) NewTransaction(ctx context.Context, records ...eventsource.Record) (eventsource.StoreTransaction, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start new transaction")
	}

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(saveSQL, store.tablename))
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

	return &transaction{
		sqlTx:   tx,
		records: records,
	}, nil
}

// Commit ...
func (tx *transaction) Commit() (err error) {
	if err := tx.sqlTx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}
	return nil
}

func (tx *transaction) Rollback() error {
	if err := tx.sqlTx.Rollback(); err != nil {
		return errors.Wrap(err, "failed to rollback transaction")
	}
	return nil
}

func (tx *transaction) GetRecords() []eventsource.Record {
	return tx.records
}
