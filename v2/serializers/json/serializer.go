package json

import (
	"encoding/json"
	"reflect"

	"github.com/pkg/errors"

	"github.com/SKF/go-eventsource/v2"
)

// JSONSerializer takes events and marshals
type serializer struct {
	eventTypes map[string]reflect.Type
}

// NewSerializer returns a seriablizable eventsource
func NewSerializer(events ...eventsource.Event) eventsource.Serializer {
	eventTypes := map[string]reflect.Type{}
	for _, event := range events {
		eventType := eventsource.GetType(event)
		eventTypes[eventType.Name()] = eventType
	}
	return &serializer{eventTypes: eventTypes}
}

// Unmarshal implements the Marshaler encoding interface
func (s *serializer) Unmarshal(data []byte, eventType string) (out eventsource.Event, err error) {
	recordType, ok := s.eventTypes[eventType]
	if !ok {
		err = errors.Errorf("Unmarshal error, unbound event type, %v", eventType)
		return
	}

	event := reflect.New(recordType).Interface()
	if err = json.Unmarshal(data, event); err != nil {
		err = errors.Wrap(err, "failed to unmarshal event")
		return
	}

	out, ok = reflect.ValueOf(event).Elem().Interface().(eventsource.Event)
	if !ok {
		err = errors.New("event doesn't implement struct Event")
		return
	}

	return
}

// Marshal implements the Unmarshaler encoding interface
func (s *serializer) Marshal(event eventsource.Event) (data []byte, err error) {
	if data, err = json.Marshal(event); err != nil {
		err = errors.Wrap(err, "failed to marshal event")
		return
	}
	return
}
