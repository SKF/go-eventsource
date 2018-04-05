package dynamodbstore

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	eventsource "github.com/skf/go-eventsource"
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
func (store *store) Load(aggregateID string) (evt []eventsource.Record, err error) {
	records := []eventsource.Record{}
	key := map[string]*dynamodb.AttributeValue{
		":id": &dynamodb.AttributeValue{S: &aggregateID},
	}

	var uerr error
	err = store.db.ScanPages(&dynamodb.ScanInput{
		TableName:                 &store.tableName,
		FilterExpression:          aws.String("aggregateId = :id"),
		ExpressionAttributeValues: key,
	}, func(page *dynamodb.ScanOutput, last bool) bool {
		recs := []eventsource.Record{}
		uerr = dynamodbattribute.UnmarshalListOfMaps(page.Items, &recs)
		if err != nil {
			log.Printf("Error unmarshal list: %+v", err)
			return false
		}

		records = append(records, recs...)
		return true // to keep paging
	})

	if uerr != nil {
		return records, uerr
	}

	return records, err
}
