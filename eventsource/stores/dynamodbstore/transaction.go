package dynamodbstore

import (
	"context"
	"errors"

	"github.com/SKF/go-eventsource/eventsource"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type transaction struct {
	store   *store
	ctx     context.Context
	records []eventsource.Record
}

func (store *store) NewTransaction(ctx context.Context, records ...eventsource.Record) (eventsource.StoreTransaction, error) {
	return &transaction{
		store:   store,
		ctx:     ctx,
		records: records,
	}, nil
}

// Commit ...
func (tx *transaction) Commit() (err error) {
	for _, record := range tx.records {
		result, err := dynamodbattribute.MarshalMap(record)
		if err != nil {
			return err
		}

		_, err = tx.store.db.PutItemWithContext(tx.ctx, &dynamodb.PutItemInput{
			TableName: &tx.store.tableName,
			Item:      result,
		})
		if err != nil {
			return err
		}
	}
	return
}

func (tx *transaction) Rollback() error {
	return errors.New("Not supported yet")
}
