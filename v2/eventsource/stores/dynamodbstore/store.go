package dynamodbstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/SKF/go-eventsource/v2/eventsource"
	"github.com/SKF/go-utility/v2/log"
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

// LoadByAggregate ...
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

	var resultItems []map[string]*dynamodb.AttributeValue

	if err = store.db.QueryPagesWithContext(ctx, &input,
		func(result *dynamodb.QueryOutput, lastPage bool) bool {
			resultItems = append(resultItems, result.Items...)
			return !lastPage
		},
	); err != nil {
		err = fmt.Errorf("couldn't scan pages (input=%+v): %w", input, err)
		return
	}

	err = dynamodbattribute.UnmarshalListOfMaps(resultItems, &records)
	if err != nil {
		err = fmt.Errorf("couldn't unmarshal list of maps: %w", err)
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

	scanItems := make([]map[string]*dynamodb.AttributeValue, 0)

	err = store.db.ScanPagesWithContext(ctx, &scanInput, func(output *dynamodb.ScanOutput, lastPage bool) bool {
		scanItems = append(scanItems, output.Items...)
		return !lastPage
	})
	if err != nil {
		err = fmt.Errorf("couldn't scan pages (input=%+v): %w", scanInput, err)
		return
	}

	err = dynamodbattribute.UnmarshalListOfMaps(scanItems, &records)
	if err != nil {
		err = fmt.Errorf("couldn't unmarshal list of maps: %w", err)
		return
	}

	return records, err
}

func (store *store) LoadBySequenceID(_ context.Context, _ string, _ ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	err = errors.New("operation not supported on DynamoDB")
	return
}

func (store *store) LoadBySequenceIDAndType(_ context.Context, _ string, _ string, _ ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	err = errors.New("operation not supported on DynamoDB")
	return
}

func (store *store) LoadByTimestamp(_ context.Context, _ int64, _ ...eventsource.QueryOption) (records []eventsource.Record, err error) {
	err = errors.New("operation not supported on DynamoDB")
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
