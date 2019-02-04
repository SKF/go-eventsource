package dynamodbstore

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/SKF/go-eventsource/eventsource"
)

func Test_SaveLoadRollback_AllInOne(t *testing.T) {
	if testing.Short() {
		t.Skip("Do not run integration test")
	}

	sess, err := session.NewSession()
	require.NoError(t, err)

	ctx := context.TODO()
	store := New(dynamodb.New(sess), "Events")
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
