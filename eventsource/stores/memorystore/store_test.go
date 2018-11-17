package memorystore

import (
	"testing"

	"github.com/SKF/go-eventsource/eventsource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_SaveLoadRollback_AllInOne(t *testing.T) {
	s := New()
	tx, err := s.NewTransaction(nil, []eventsource.Record{
		{AggregateID: "A", SequenceID: "1"},
		{AggregateID: "A", SequenceID: "2"},
		{AggregateID: "B", SequenceID: "1"},
	}...)
	require.NoError(t, err)

	err = tx.Save()
	require.NoError(t, err)

	records, err := s.Load(nil, "A")
	require.NoError(t, err)
	assert.Len(t, records, 2)

	records, err = s.Load(nil, "B")
	require.NoError(t, err)
	assert.Len(t, records, 1)

	records, err = s.Load(nil, "C")
	require.Error(t, err, "Not found")
	assert.Len(t, records, 0)

	err = tx.Rollback()
	require.NoError(t, err)

	records, err = s.Load(nil, "A")
	require.Error(t, err, "Not found")
	assert.Len(t, records, 0)

	records, err = s.Load(nil, "B")
	require.Error(t, err, "Not found")
	assert.Len(t, records, 0)
}
