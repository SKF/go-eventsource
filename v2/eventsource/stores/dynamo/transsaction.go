package dynamo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"

	"github.com/SKF/go-eventsource/v2/eventsource"
)

type transaction struct {
	store   *store
	ctx     context.Context
	records []eventsource.Record
	saved   []eventsource.Record
}

func (store *store) NewTransaction(ctx context.Context, records ...eventsource.Record) (eventsource.StoreTransaction, error) {
	return &transaction{
		store:   store,
		ctx:     ctx,
		records: records,
		saved:   []eventsource.Record{},
	}, nil
}

// Commit ...
func (tx *transaction) Commit() error {
	for _, record := range tx.records {
		result, err := attributevalue.MarshalMap(record)
		if err != nil {
			return errors.Wrap(err, "couldn't marshal record")
		}

		_, err = tx.store.db.PutItem(tx.ctx, &dynamodb.PutItemInput{
			TableName: &tx.store.tableName,
			Item:      result,
		})
		if err != nil {
			return errors.Wrap(err, "couldn't put record to dynamodb store")
		}

		tx.saved = append(tx.saved, record)
	}

	return nil
}

func (tx *transaction) Rollback() error {
	for _, record := range tx.saved {
		_, err := tx.store.db.DeleteItem(tx.ctx, &dynamodb.DeleteItemInput{
			TableName: &tx.store.tableName,
			Key: map[string]types.AttributeValue{
				"aggregateId": &types.AttributeValueMemberS{Value: record.AggregateID},
				"timestamp":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", record.Timestamp)},
			},
		})
		if err != nil {
			return errors.Wrap(err, "couldn't delete record in dynamodb store")
		}
	}

	return nil
}

func (tx *transaction) GetRecords() []eventsource.Record {
	return tx.records
}
