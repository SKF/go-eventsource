package recorder

import (
	"context"
	"encoding/json"

	"github.com/SKF/go-eventsource/v2/eventsource"
)

type Recorder struct {
	events []eventsource.Record
}

func (n *Recorder) Send(record eventsource.Record) error {
	return n.SendWithContext(context.Background(), record)
}

func (n *Recorder) SendWithContext(_ context.Context, record eventsource.Record) error {
	n.events = append(n.events, record)

	return nil
}

func (n *Recorder) GetEventDatas() []map[string]interface{} {
	events := make([]map[string]interface{}, len(n.events))

	for i := range n.events {
		m := map[string]interface{}{}

		err := json.Unmarshal(n.events[i].Data, &m)
		if err != nil {
			panic(err)
		}
		events[i] = m
	}

	return events
}
