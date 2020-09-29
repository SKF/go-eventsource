package dynamodbstore

import (
	"context"

	"github.com/SKF/go-eventsource/v2/eventsource"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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

	queryOpts := evaluateQueryOptions(opts)

	addTimestampToQuery(&input, queryOpts.timestamp)
	addFilteringOnQuery(&input, queryOpts.filterOptions)

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
	records = []eventsource.Record{}
	queryOpts := evaluateQueryOptions(opts)

	scanInput := dynamodb.ScanInput{
		Limit:          queryOpts.limit,
		IndexName:      queryOpts.index,
		TableName:      &store.tableName,
		ConsistentRead: &store.consistentRead,
	}

	addTimestampOnScan(&scanInput, queryOpts.timestamp)
	addFilteringOnScan(&scanInput, queryOpts.filterOptions)

	var scanItems = []map[string]*dynamodb.AttributeValue{}

	err = store.db.ScanPagesWithContext(ctx, &scanInput, func(output *dynamodb.ScanOutput, lastPage bool) bool {
		scanItems = append(scanItems, output.Items...)
		return !lastPage
	})
	if err != nil {
		log.
			WithField("input", scanInput).
			WithField("error", err).
			Error("Couldn't scan pages")
		err = errors.Wrap(err, "couldn't scan pages")
		return
	}

	err = dynamodbattribute.UnmarshalListOfMaps(scanItems, &records)
	if err != nil {
		log.
			WithField("error", err).
			Error("Couldn't unmarshal list of maps")
		err = errors.Wrap(err, "couldn't unmarshal list of maps")
		return
	}

	return records, err
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

func addFilteringOnScan(scanInput *dynamodb.ScanInput, filterOption *filterOpt) {
	if filterOption != nil {
		if filterOption.getDynamoAttributeValue() == nil {
			log.Warnf("Unable to add filtering on unimplmented data type: %s", filterOption.attributeType)
			return
		}

		scanInput.FilterExpression = filterOption.mapFilterExpression(scanInput.FilterExpression)
		scanInput.ExpressionAttributeValues = filterOption.mapExpressionAttributeValues(scanInput.ExpressionAttributeValues)
		scanInput.ExpressionAttributeNames = filterOption.mapExpressionAttributeNames(scanInput.ExpressionAttributeNames)
	}
}

func addFilteringOnQuery(queryInput *dynamodb.QueryInput, filterOption *filterOpt) {
	if filterOption != nil {
		if filterOption.getDynamoAttributeValue() == nil {
			log.Warnf("Unable to add filtering on unimplmented data type: %s", filterOption.attributeType)
			return
		}

		queryInput.FilterExpression = filterOption.mapFilterExpression(queryInput.FilterExpression)
		queryInput.ExpressionAttributeValues = filterOption.mapExpressionAttributeValues(queryInput.ExpressionAttributeValues)
		queryInput.ExpressionAttributeNames = filterOption.mapExpressionAttributeNames(queryInput.ExpressionAttributeNames)
	}
}

func addTimestampOnScan(scanInput *dynamodb.ScanInput, timestamp *string) {
	if timestamp != nil {
		exprWithTs, values, names := mapTimestampToDynamoExpr(scanInput.FilterExpression, scanInput.ExpressionAttributeValues, scanInput.ExpressionAttributeNames, timestamp)

		scanInput.FilterExpression = &exprWithTs
		scanInput.ExpressionAttributeValues = values
		scanInput.ExpressionAttributeNames = names
	}
}

func addTimestampToQuery(queryInput *dynamodb.QueryInput, timestamp *string) {
	if timestamp != nil {
		exprWithTs, values, names := mapTimestampToDynamoExpr(queryInput.KeyConditionExpression, queryInput.ExpressionAttributeValues, queryInput.ExpressionAttributeNames, timestamp)

		queryInput.KeyConditionExpression = &exprWithTs
		queryInput.ExpressionAttributeValues = values
		queryInput.ExpressionAttributeNames = names
	}
}
