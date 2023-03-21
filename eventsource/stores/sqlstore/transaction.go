package sqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pkg/errors"

	"github.com/SKF/go-eventsource/eventsource"
)

type transaction struct {
	sqlTx *sql.Tx
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

	defer func() {
		if errClose := stmt.Close(); errClose != nil {
			if err != nil {
				err = errors.Wrapf(err, "failed to close stmt statement: %s", errClose)
			} else {
				err = errors.Wrap(errClose, "failed to close stmt statement")
			}
		}
	}()

	for _, record := range records {
		_, err = stmt.ExecContext(ctx, record.AggregateID, record.SequenceID, record.Timestamp, record.UserID, record.Type, record.Data)
		if err != nil {
			return nil, errors.Wrap(err, "failed to execute query")
		}
	}

	return &transaction{
		sqlTx: tx,
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
