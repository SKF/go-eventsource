package dynamo

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/SKF/go-eventsource/v2/eventsource"
	"github.com/SKF/go-utility/v2/log"
)

type store struct {
	db        *dynamodb.Client
	tableName string
}

// New creates a new event source store
func New(db *dynamodb.Client, tableName string) eventsource.Store {
	return &store{
		db:        db,
		tableName: tableName,
	}
}

// LoadByAggregate ...
func (store *store) LoadByAggregate(ctx context.Context, aggregateID string, opts ...eventsource.QueryOption) ([]eventsource.Record, error) {
	var (
		records = []eventsource.Record{}
		key     = map[string]types.AttributeValue{
			":id": &types.AttributeValueMemberS{Value: aggregateID},
		}
		input = dynamodb.QueryInput{
			TableName:                 &store.tableName,
			KeyConditionExpression:    aws.String("aggregateId = :id"),
			ExpressionAttributeValues: key,
			ConsistentRead:            aws.Bool(true),
		}
		resultItems []map[string]types.AttributeValue
		queryOpts   = evaluateQueryOptions(opts)
	)

	addTimestampToQuery(&input, queryOpts.timestamp)
	addFilteringOnQuery(&input, queryOpts.filterOptions)

	for pagniator := dynamodb.NewQueryPaginator(store.db, &input); pagniator.HasMorePages(); {
		page, err := pagniator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("couldn't scan pages (input=%+v): %w", input, err)
		}

		resultItems = append(resultItems, page.Items...)
	}

	if err := attributevalue.UnmarshalListOfMaps(resultItems, &records); err != nil {
		return nil, fmt.Errorf("couldn't unmarshal list of maps: %w", err)
	}

	return records, nil
}

// Load will load records based on specified query options
func (store *store) Load(ctx context.Context, opts ...eventsource.QueryOption) ([]eventsource.Record, error) {
	var (
		records   = []eventsource.Record{}
		queryOpts = evaluateQueryOptions(opts)

		scanInput = dynamodb.ScanInput{
			Limit:          queryOpts.limit,
			IndexName:      queryOpts.index,
			TableName:      &store.tableName,
			ConsistentRead: aws.Bool(true),
		}
		scanItems = make([]map[string]types.AttributeValue, 0)
	)

	addTimestampOnScan(&scanInput, queryOpts.timestamp)
	addFilteringOnScan(&scanInput, queryOpts.filterOptions)

	for paginator := dynamodb.NewScanPaginator(store.db, &scanInput); paginator.HasMorePages(); {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("couldn't scan pages (input=%+v): %w", scanInput, err)
		}

		scanItems = append(scanItems, page.Items...)
	}

	if err := attributevalue.UnmarshalListOfMaps(scanItems, &records); err != nil {
		return nil, fmt.Errorf("couldn't unmarshal list of maps: %w", err)
	}

	return records, nil
}

func (store *store) LoadBySequenceID(context.Context, string, ...eventsource.QueryOption) ([]eventsource.Record, error) {
	return nil, errors.New("operation not supported on DynamoDB")
}

func (store *store) LoadBySequenceIDAndType(context.Context, string, string, ...eventsource.QueryOption) ([]eventsource.Record, error) {
	return nil, errors.New("operation not supported on DynamoDB")
}

func (store *store) LoadByTimestamp(context.Context, int64, ...eventsource.QueryOption) ([]eventsource.Record, error) {
	return nil, errors.New("operation not supported on DynamoDB")
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
