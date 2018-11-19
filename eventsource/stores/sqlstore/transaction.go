package sqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/SKF/go-eventsource/eventsource"
)

type transaction struct {
	sqlTx *sql.Tx
}

func (store *store) NewTransaction(ctx context.Context, records ...eventsource.Record) (eventsource.StoreTransaction, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(saveSQL, store.tablename))
	if err != nil {
		return nil, err
	}

	for _, record := range records {
		_, err = stmt.ExecContext(ctx, record.AggregateID, record.SequenceID, record.Timestamp, record.UserID, record.Type, record.Data)
		if err != nil {
			return nil, err
		}
	}

	return &transaction{
		sqlTx: tx,
	}, nil
}

// Commit ...
func (tx *transaction) Commit() (err error) {
	return tx.sqlTx.Commit()
}

func (tx *transaction) Rollback() error {
	return tx.sqlTx.Rollback()
}
