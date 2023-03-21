package dynamodbstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/SKF/go-eventsource/eventsource"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type store struct {
	db             *dynamodb.DynamoDB
	tableName      string
	consistentRead bool
}

// New ...
func New(db *dynamodb.DynamoDB, tableName string) eventsource.Store {
	return &store{
		db:             db,
		tableName:      tableName,
		consistentRead: true,
	}
}

// LoadByAggregate ...
func (store *store) LoadByAggregate(ctx context.Context, aggregateID string) (records []eventsource.Record, err error) {
	records = []eventsource.Record{}
	key := map[string]*dynamodb.AttributeValue{
		":id": {S: &aggregateID},
	}

	input := dynamodb.QueryInput{
		TableName:                 &store.tableName,
		KeyConditionExpression:    aws.String("aggregateId = :id"),
		ExpressionAttributeValues: key,
		ConsistentRead:            &store.consistentRead,
	}

	output, err := store.db.QueryWithContext(ctx, &input)
	if err != nil {
		err = fmt.Errorf("couldn't scan pages (input=%+v): %w", input, err)
		return
	}

	err = dynamodbattribute.UnmarshalListOfMaps(output.Items, &records)
	if err != nil {
		err = fmt.Errorf("couldn't unmarshal list of maps: %w", err)
		return
	}

	return records, err
}

func (store *store) LoadBySequenceID(_ context.Context, _ string, _ int) (records []eventsource.Record, err error) {
	err = errors.New("operation not supported on DynamoDB")
	return
}

func (store *store) LoadBySequenceIDAndType(_ context.Context, _ string, _ string, _ int) (records []eventsource.Record, err error) {
	err = errors.New("operation not supported on DynamoDB")
	return
}

func (store *store) LoadByTimestamp(_ context.Context, _ int64, _ int) (records []eventsource.Record, err error) {
	err = errors.New("operation not supported on DynamoDB")
	return
}
