package memorystore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/SKF/go-eventsource/v2/eventsource"
)

func Test_SaveLoadRollback_AllInOne(t *testing.T) {
	ctx := context.TODO()
	store := New()
	tx, err := store.NewTransaction(ctx, []eventsource.Record{
		{AggregateID: "A", SequenceID: "1", Type: "TestEventA"},
		{AggregateID: "B", SequenceID: "1", Type: "TestEventB"},
		{AggregateID: "C", SequenceID: "4", Type: "TestEventA"},
		{AggregateID: "D", SequenceID: "3", Type: "TestEventA"},
		{AggregateID: "A", SequenceID: "2", Type: "TestEventB"},
	}...)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	records, err := store.Load(ctx, BySequenceID("1"))
	require.NoError(t, err)
	assert.Len(t, records, 3)
	assert.Equal(t, records[0].SequenceID, "2")
	assert.Equal(t, records[1].SequenceID, "3")
	assert.Equal(t, records[2].SequenceID, "4")

	records, err = store.Load(ctx, BySequenceID("1"), WithLimit(1))
	require.NoError(t, err)
	assert.Len(t, records, 1)
	assert.Equal(t, records[0].SequenceID, "2")

	records, err = store.Load(ctx, BySequenceID("1"), ByType("TestEventA"))
	require.NoError(t, err)
	assert.Len(t, records, 2)
	assert.Equal(t, records[0].SequenceID, "3")
	assert.Equal(t, records[1].SequenceID, "4")

	records, err = store.LoadByAggregate(ctx, "A")
	require.NoError(t, err)
	assert.Len(t, records, 2)

	records, err = store.LoadByAggregate(ctx, "B")
	require.NoError(t, err)
	assert.Len(t, records, 1)

	records, err = store.LoadByAggregate(ctx, "E")
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
