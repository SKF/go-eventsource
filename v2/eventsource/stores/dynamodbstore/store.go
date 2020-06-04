package dynamodbstore

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/SKF/go-eventsource/v2/eventsource"
)

type store struct {
	db             *dynamodb.DynamoDB
	tableName      string
	consistentRead bool
}

// New creates a new event source store
func New(db *dynamodb.DynamoDB, tableName string) eventsource.Store {
	return &store{
		db:             db,
		tableName:      tableName,
		consistentRead: true,
	}
}

//LoadByAggregate ...
func (store *store) LoadByAggregate(ctx context.Context, aggregateID string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
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
		log.
			WithField("input", input).
			WithField("error", err).
			Error("Couldn't scan pages")
		err = errors.Wrap(err, "couldn't scan pages")
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

// Load will load records based on specified query options
func (store *store) Load(ctx context.Context, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	err = errors.New("operation not supported on DynamoDB")
	log.Error(err.Error())
	return
}

// Deprecated
func (store *store) LoadBySequenceID(ctx context.Context, sequenceID string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	err = errors.New("operation not supported on DynamoDB")
	log.Error(err.Error())
	return
}

// Deprecated
func (store *store) LoadBySequenceIDAndType(ctx context.Context, sequenceID string, eventType string, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	err = errors.New("operation not supported on DynamoDB")
	log.Error(err.Error())
	return
}

// Deprecated
func (store *store) LoadByTimestamp(ctx context.Context, timestamp int64, opts ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	err = errors.New("operation not supported on DynamoDB")
	log.Error(err.Error())
	return
}
