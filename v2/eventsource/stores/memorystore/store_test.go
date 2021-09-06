package memorystore

import (
	"context"
	"sync"
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

func TestMemoryStoreConcurrentSave(t *testing.T) {
	ctx := context.Background()
	store := New()
	repo := eventsource.NewRepository(store, &serializer{})

	const n = 10_000
	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			evt := eventsource.BaseEvent{}
			err := repo.Save(ctx, &evt)
			require.NoError(t, err)
		}()
	}
	wg.Wait()
}

type serializer struct{}

func (s *serializer) Unmarshal(data []byte, eventType string) (event eventsource.Event, err error) {
	return &eventsource.BaseEvent{}, nil

}
func (s *serializer) Marshal(event eventsource.Event) (data []byte, err error) {
	return nil, nil
}
