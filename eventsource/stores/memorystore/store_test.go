package memorystore

import (
	"context"
	"testing"

	"github.com/SKF/go-eventsource/eventsource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_SaveLoadRollback_AllInOne(t *testing.T) {
	ctx := context.Background()
	store := New()
	tx, err := store.NewTransaction(ctx, []eventsource.Record{
		{AggregateID: "A", SequenceID: "1"},
		{AggregateID: "A", SequenceID: "2"},
		{AggregateID: "B", SequenceID: "1"},
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
