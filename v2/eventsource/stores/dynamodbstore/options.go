package dynamodbstore

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/SKF/go-eventsource/v2/eventsource"
	"github.com/SKF/go-utility/v2/array"
	"github.com/SKF/go-utility/v2/log"
)

type column string

const (
	columnAggregateID column = "aggregateId"
	columnSequenceID  column = "sequenceId"
	columnTimestamp   column = "timestamp"
	columnUserID      column = "userId"
	columnType        column = "type"
	columnData        column = "data"
)

var typeByColumn = map[column]string{
	columnAggregateID: "S",
	columnSequenceID:  "S",
	columnTimestamp:   "N",
	columnUserID:      "S",
	columnType:        "S",
	columnData:        "B",
}

type filterOpt struct {
	columnName     string
	attributeName  string
	attributeValue string
	attributeType  string
	filterOperator string
}

type options struct {
	limit         *int64
	index         *string
	filterOptions *filterOpt
	timestamp     *string
}

// WithLimit will limit the result
func WithLimit(limit int64) eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			o.limit = &limit
		} else {
			log.Warn("Trying to put limit option to a non dynamodbstore.options")
		}
	}
}

// WithIndex will query on an index instead of primarykey
func WithIndex(indexName string) eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			o.index = &indexName
		} else {
			log.Warn("Trying to put index option to a non dynamodbstore.options")
		}
	}
}

func withFilter(onColumn column, againstValue, withOperator string) eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			columnName := string(onColumn)
			if !array.ContainsEmpty(columnName, againstValue, withOperator) {
				filter := filterOpt{
					columnName:     columnName,
					attributeType:  typeByColumn[onColumn],
					attributeName:  fmt.Sprintf("comparable%s", columnName),
					attributeValue: againstValue,
					filterOperator: withOperator,
				}
				o.filterOptions = &filter
			}
		} else {
			log.Warn("Trying to put filter option to a non dynamodbstore.options")
		}
	}
}

func greaterThan(columnName column, value string) eventsource.QueryOption {
	return withFilter(columnName, value, ">")
}

// BySequenceID will set filter to only return records with sequence id greater than value
func BySequenceID(value string) eventsource.QueryOption {
	return greaterThan(columnSequenceID, value)
}

// ByTimestamp will set filter to only return records with timestamp greater than value
func ByTimestamp(value string) eventsource.QueryOption {
	return func(i interface{}) {
		if o, ok := i.(*options); ok {
			o.timestamp = &value
		} else {
			log.Warn("Trying to put timestamp option to a non dynamodbstore.options")
		}
	}
}

// ByType will set filter to only return records with type equal to value
func ByType(value string) eventsource.QueryOption {
	return withFilter(columnType, value, "=")
}

// evaluate a list of options by extending the default options
func evaluateQueryOptions(queryOpts []eventsource.QueryOption) *options {
	opts := &options{}

	for _, opt := range queryOpts {
		opt(opts)
	}

	return opts
}

func (f *filterOpt) getDynamoAttributeValue() (dynamoValueMapping *dynamodb.AttributeValue) {
	switch f.attributeType {
	case "S":
		mapping := dynamodb.AttributeValue{S: &f.attributeValue}
		dynamoValueMapping = &mapping
	case "N":
		mapping := dynamodb.AttributeValue{N: &f.attributeValue}
		dynamoValueMapping = &mapping
	}

	return dynamoValueMapping
}

func (f *filterOpt) mapExpressionAttributeValues(valueMap map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue {
	if valueMap == nil {
		valueMap = make(map[string]*dynamodb.AttributeValue)
	}

	attrName := fmt.Sprintf(":%s", f.attributeName)
	valueMap[attrName] = f.getDynamoAttributeValue()

	return valueMap
}

func (f *filterOpt) mapExpressionAttributeNames(nameMap map[string]*string) map[string]*string {
	if nameMap == nil {
		nameMap = make(map[string]*string)
	}

	keyAttrName := fmt.Sprintf("#%s", f.columnName)
	nameMap[keyAttrName] = &f.columnName

	return nameMap
}

func (f *filterOpt) mapFilterExpression(filterExpr *string) *string {
	expr := fmt.Sprintf("#%s %s :%s", f.columnName, f.filterOperator, f.attributeName)

	if filterExpr != nil {
		expr = fmt.Sprintf("%s AND %s", *filterExpr, expr)
	}

	return &expr
}

func mapTimestampToDynamoExpr(inputExpression *string, inputValues map[string]*dynamodb.AttributeValue, inputNames map[string]*string, timestamp *string) (expression string, expressionAttributeValues map[string]*dynamodb.AttributeValue, expressionAttributeNames map[string]*string) {
	expression = "#timestamp > :ts"
	if inputExpression != nil {
		expression = *inputExpression + " AND #timestamp > :ts"
	}

	expressionAttributeValues = inputValues
	if expressionAttributeValues == nil {
		expressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
	}

	expressionAttributeValues[":ts"] = &dynamodb.AttributeValue{N: timestamp}

	expressionAttributeNames = inputNames
	if expressionAttributeNames == nil {
		expressionAttributeNames = make(map[string]*string)
	}

	expressionAttributeNames["#timestamp"] = aws.String("timestamp")

	return expression, expressionAttributeValues, expressionAttributeNames
}
