package dynamodbstore

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/SKF/go-eventsource/v2/eventsource"
	"github.com/SKF/go-utility/v2/env"
)

const dynamoTableName = "Events"

func Test_SaveLoadRollback_AllInOne(t *testing.T) {
	if testing.Short() || env.GetAsString("AWS_REGION", "") == "" {
		t.Skip("Do not run dynamodb integration test")
	}

	sess, err := session.NewSession()
	require.NoError(t, err)

	ctx := context.TODO()
	store := New(dynamodb.New(sess), dynamoTableName)
	tx, err := store.NewTransaction(ctx, []eventsource.Record{
		{AggregateID: "A", Timestamp: 1},
		{AggregateID: "A", Timestamp: 2},
		{AggregateID: "B", Timestamp: 3},
	}...)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	records, err := store.LoadByAggregate(ctx, "A")
	require.NoError(t, err)
	assert.Len(t, records, 2)

	records, err = store.LoadByAggregate(ctx, "A", ByTimestamp("1"))
	require.NoError(t, err)
	assert.Len(t, records, 1)

	records, err = store.LoadByAggregate(ctx, "B")
	require.NoError(t, err)
	assert.Len(t, records, 1)

	records, err = store.LoadByAggregate(ctx, "C")
	require.NoError(t, err)
	assert.Len(t, records, 0)

	err = tx.Rollback()
	require.NoError(t, err)

	records, err = store.LoadByAggregate(ctx, "A")
	require.NoError(t, err)
	assert.Len(t, records, 0)

	records, err = store.LoadByAggregate(ctx, "B")
	require.NoError(t, err)
	assert.Len(t, records, 0)
}

func Test_LoadByAggrWithFiltering(t *testing.T) {
	if testing.Short() || env.GetAsString("AWS_REGION", "") == "" {
		t.Skip("Do not run integration test")
	}

	sess, err := session.NewSession()
	require.NoError(t, err)

	ctx := context.TODO()
	store := New(dynamodb.New(sess), dynamoTableName)

	tx, err := store.NewTransaction(ctx, []eventsource.Record{
		{AggregateID: "A", Timestamp: 1, Type: "TestType", SequenceID: "1a"},
		{AggregateID: "A", Timestamp: 2, Type: "OtherType", SequenceID: "1b"},
		{AggregateID: "B", Timestamp: 3, Type: "TestType", SequenceID: "1c"},
	}...)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	records, err := store.LoadByAggregate(ctx, "A")
	require.NoError(t, err)
	assert.Len(t, records, 2)

	records, err = store.LoadByAggregate(ctx, "A", ByTimestamp("1"))
	require.NoError(t, err)
	assert.Len(t, records, 1)

	records, err = store.LoadByAggregate(ctx, "A", ByType("OtherType"))
	require.NoError(t, err)
	assert.Len(t, records, 1)

	records, err = store.LoadByAggregate(ctx, "A", BySequenceID("1a"))
	require.NoError(t, err)
	assert.Len(t, records, 1)

	err = tx.Rollback()
	require.NoError(t, err)
}

func Test_LoadWithFiltering(t *testing.T) {
	if testing.Short() || env.GetAsString("AWS_REGION", "") == "" {
		t.Skip("Do not run integration test")
	}

	sess, err := session.NewSession()
	require.NoError(t, err)

	ctx := context.TODO()
	store := New(dynamodb.New(sess), dynamoTableName)

	tx, err := store.NewTransaction(ctx, []eventsource.Record{
		{AggregateID: "A", Timestamp: 1, Type: "TestType", SequenceID: "1a"},
		{AggregateID: "A", Timestamp: 2, Type: "OtherType", SequenceID: "1b"},
		{AggregateID: "B", Timestamp: 3, Type: "TestType", SequenceID: "1c"},
	}...)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	records, err := store.Load(ctx, ByTimestamp("1"))
	require.NoError(t, err)
	assert.Len(t, records, 2)

	records, err = store.Load(ctx, ByType("TestType"))
	require.NoError(t, err)
	assert.Len(t, records, 2)

	records, err = store.Load(ctx, BySequenceID("1a"))
	require.NoError(t, err)
	assert.Len(t, records, 2)

	err = tx.Rollback()
	require.NoError(t, err)
}
