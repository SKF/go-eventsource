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

	input := dynamodb.ScanInput{
		TableName:                 &store.tableName,
		FilterExpression:          aws.String("aggregateId = :id"),
		ExpressionAttributeValues: key,
	}

	var uerr error
	pageFunc := func(page *dynamodb.ScanOutput, last bool) bool {
		recs := []eventsource.Record{}
		uerr = dynamodbattribute.UnmarshalListOfMaps(page.Items, &recs)
		if uerr != nil {
			log.
				WithField("error", err).
				Error("Couldn't unmarshal list of maps")
			return false
		}

		records = append(records, recs...)
		return true // to keep paging
	}

	if err = store.db.ScanPages(&input, pageFunc); err != nil {
		log.
			WithField("input", input).
			WithField("error", err).
			Error("Couldn't scan pages")
		return
	}

	if uerr != nil {
		return records, uerr
	}

	return
}
