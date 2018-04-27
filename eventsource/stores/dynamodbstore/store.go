package dynamodbstore

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	log "github.com/sirupsen/logrus"

	"github.com/SKF/go-eventsource/eventsource"
)

type store struct {
	db        *dynamodb.DynamoDB
	tableName string
}

// New ...
func New(tableName string) eventsource.Store {
	return &store{
		db: dynamodb.New(
			session.Must(session.NewSession()),
		),
		tableName: tableName,
	}
}

// Save ...
func (store *store) Save(record eventsource.Record) (err error) {
	result, err := dynamodbattribute.MarshalMap(record)
	if err != nil {
		return
	}
	_, err = store.db.PutItem(&dynamodb.PutItemInput{
		TableName: &store.tableName,
		Item:      result,
	})
	return
}

//Load ...
func (store *store) Load(id string) (records []eventsource.Record, err error) {
	records = []eventsource.Record{}
	key := map[string]*dynamodb.AttributeValue{
		":id": &dynamodb.AttributeValue{S: &id},
	}

	input := dynamodb.QueryInput{
		TableName:                 &store.tableName,
		KeyConditionExpression:    aws.String("aggregateId = :id"),
		ExpressionAttributeValues: key,
	}

	output, err := store.db.Query(&input)
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
