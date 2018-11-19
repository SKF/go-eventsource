package dynamodbstore

import (
	"context"
	"testing"

	"github.com/SKF/go-eventsource/eventsource"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_SaveLoadRollback_AllInOne(t *testing.T) {
	if testing.Short() {
		t.Skip("Do not run integration test")
	}

	sess, err := session.NewSession()
	require.NoError(t, err)

	ctx := context.Background()
	store := New(sess, "Events")
	tx, err := store.NewTransaction(ctx, []eventsource.Record{
		{AggregateID: "A", Timestamp: 1},
		{AggregateID: "A", Timestamp: 2},
		{AggregateID: "B", Timestamp: 3},
	}...)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	records, err := store.Load(ctx, "A")
	require.NoError(t, err)
	assert.Len(t, records, 2)

	records, err = store.Load(ctx, "B")
	require.NoError(t, err)
	assert.Len(t, records, 1)

	records, err = store.Load(ctx, "C")
	require.NoError(t, err)
	assert.Len(t, records, 0)

	err = tx.Rollback()
	require.NoError(t, err)

	records, err = store.Load(ctx, "A")
	require.NoError(t, err)
	assert.Len(t, records, 0)

	records, err = store.Load(ctx, "B")
	require.NoError(t, err)
	assert.Len(t, records, 0)
}
