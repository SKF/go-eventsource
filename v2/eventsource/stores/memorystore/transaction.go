package memorystore

import (
	"context"

	"github.com/SKF/go-eventsource/v2/eventsource"
)

type transaction struct {
	mem     *store
	records []eventsource.Record
}

func (mem *store) NewTransaction(_ context.Context, records ...eventsource.Record) (eventsource.StoreTransaction, error) {
	return &transaction{
		mem:     mem,
		records: records,
	}, nil
}

// Commit ...
func (tx *transaction) Commit() error {
	tx.mem.mutex.Lock()
	defer tx.mem.mutex.Unlock()

	for _, record := range tx.records {
		id := record.AggregateID
		tx.mem.Data[id] = append(tx.mem.Data[id], record)
	}

	return nil
}

func (tx *transaction) Rollback() error {
	tx.mem.mutex.Lock()
	defer tx.mem.mutex.Unlock()

	for _, record := range tx.records {
		id := record.AggregateID
		if rows, ok := tx.mem.Data[id]; ok {
			newRows := []eventsource.Record{}

			for _, row := range rows {
				if row.SequenceID != record.SequenceID {
					newRows = append(newRows, row)
				}
			}

			if len(newRows) == 0 {
				delete(tx.mem.Data, id)
			} else {
				tx.mem.Data[id] = newRows
			}
		}
	}

	return nil
}

func (tx *transaction) GetRecords() []eventsource.Record {
	return tx.records
}
