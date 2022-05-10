package dynamodbstore

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/SKF/go-eventsource/eventsource"
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

//LoadByAggregate ...
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
		log.
			WithField("error", err).
			Error("Couldn't unmarshal list of maps")
		err = errors.Wrap(err, "couldn't unmarshal list of maps")
		return
	}

	return records, err
}

// LoadBySequenceID ...
func (store *store) LoadBySequenceID(ctx context.Context, sequenceID string, limit int) (records []eventsource.Record, err error) {
	err = errors.New("operation not supported on DynamoDB")
	log.Error(err.Error())
	return
}

// LoadBySequenceIDAndType ...
func (store *store) LoadBySequenceIDAndType(ctx context.Context, sequenceID string, eventType string, limit int) (records []eventsource.Record, err error) {
	err = errors.New("operation not supported on DynamoDB")
	log.Error(err.Error())
	return
}

// LoadByTimestamp
func (store *store) LoadByTimestamp(ctx context.Context, timestamp int64, limit int) (records []eventsource.Record, err error) {
	err = errors.New("operation not supported on DynamoDB")
	log.Error(err.Error())
	return
}
