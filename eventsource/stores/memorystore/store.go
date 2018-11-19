package memorystore

import (
	"context"

	"github.com/SKF/go-eventsource/eventsource"
)

type store struct {
	Data map[string][]eventsource.Record
}

func New() eventsource.Store {
	return &store{
		Data: map[string][]eventsource.Record{},
	}
}

// Load ...
func (mem *store) Load(_ context.Context, id string) (evt []eventsource.Record, err error) {
	if rows, ok := mem.Data[id]; ok {
		return rows, nil
	}
	return evt, nil
}
