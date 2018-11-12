package dynamodbstore

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	log "github.com/sirupsen/logrus"

	"github.com/SKF/go-eventsource/eventsource"
)

type store struct {
	db             *dynamodb.DynamoDB
	tableName      string
	consistentRead bool
}

// New ...
func New(sess *session.Session, tableName string) eventsource.Store {
	return &store{
		db:             dynamodb.New(sess),
		tableName:      tableName,
		consistentRead: true,
	}
}

// Save ...
func (store *store) Save(records ...eventsource.Record) (err error) {
	return store.SaveWithContext(context.Background(), records...)
}

// SaveWithContext ...
func (store *store) SaveWithContext(ctx context.Context, records ...eventsource.Record) (err error) {
	for _, record := range records {
		result, err := dynamodbattribute.MarshalMap(record)
		if err != nil {
			return err
		}

		_, err = store.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
			TableName: &store.tableName,
			Item:      result,
		})
		if err != nil {
			return err
		}
	}
	return
}

//Load ...
func (store *store) Load(id string) (records []eventsource.Record, err error) {
	return store.LoadWithContext(context.Background(), id)
}

//LoadWithContext ...
func (store *store) LoadWithContext(ctx context.Context, id string) (records []eventsource.Record, err error) {
	records = []eventsource.Record{}
	key := map[string]*dynamodb.AttributeValue{
		":id": &dynamodb.AttributeValue{S: &id},
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
		return
	}

	err = dynamodbattribute.UnmarshalListOfMaps(output.Items, &records)
	if err != nil {
		log.
			WithField("error", err).
			Error("Couldn't unmarshal list of maps")
		return
	}

	return
}
