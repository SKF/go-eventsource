# Introduction [![Go Report Card](https://goreportcard.com/badge/github.com/SKF/go-eventsource)](https://goreportcard.com/report/github.com/SKF/go-eventsource) [![Build Status on master](https://travis-ci.org/SKF/go-eventsource.svg?branch=master)](https://travis-ci.org/SKF/go-eventsource)

go-eventsource is a package for building an Event Store

The package is under development and the interfaces may change.

[Event Sourcing Basics](http://eventstore.org.s3-website.eu-west-2.amazonaws.com/docs/event-sourcing-basics)

# How to use it

To create a new repository:
`NewRepository(store, serializer)`

It has an interface for saving events and loading an aggregate.

```
ype Repository interface {
	// Return store
	Store() Store

	// Save one or more events to the repository
	Save(ctx context.Context, events ...Event) error

	// Save one or more events to the repository, within a transaction
	SaveTransaction(ctx context.Context, events ...Event) (StoreTransaction, error)

	// Load events from repository for the given aggregate ID. For each event e,
	// call aggr.On(e) to update the state of aggr. When done, aggr has been
	// "fast forwarded" to the current state.
	Load(ctx context.Context, id string, aggr Aggregate) (deleted bool, err error)

	// Get all events with query options (definied in the store)
	// Query options can be used for filter by sequence ID (see https://github.com/oklog/ulid)
	// or options like limit, offset
	LoadEvents(ctx context.Context, opts ...QueryOption) (events []Event, err error)

	// Deprecated: Use LoadEvents(ctx, store.BySequenceId(...))
	// Get all events with sequence ID newer than the given ID (see https://github.com/oklog/ulid)
	// Return at most limit records. If limit is 0, don't limit the number of records returned.
	GetEventsBySequenceID(ctx context.Context, sequenceID string, opts ...QueryOption) (events []Event, err error)

	// Deprecated: Use LoadEvents(ctx, store.BySequenceId(...), store.ByType(...))
	// Same as GetEventsBySequenceID, but only returns events of the same type
	// as the one provided in the eventType parameter.
	GetEventsBySequenceIDAndType(ctx context.Context, sequenceID string, eventType Event, opts ...QueryOption) (events []Event, err error)

	// Deprecated: Use LoadEvents(ctx, store.ByTimestamp(...))
	// Get all events newer than the given timestamp
	// Return at most limit records. If limit is 0, don't limit the number of records returned.
	GetEventsByTimestamp(ctx context.Context, timestamp int64, opts ...QueryOption) (events []Event, err error)

	// Add notification service
	AddNotificationService(service NotificationService)
}
```

The package comes with one serializer and two stores:

Included serializer:

- `json`

Included stores:

- `dynamodb`
- `memory`
- `sql`

If you want to add your own store or serializer, the package has these defined interfaces.

```
type CallOptions func(i interface{})

type Store interface {
	NewTransaction(ctx context.Context, records ...Record) (StoreTransaction, error)
	LoadByAggregate(ctx context.Context, aggregateID string, opts ...QueryOption) ([]Record, error)
	Load(ctx context.Context, opts ...QueryOption) ([]Record, error)

	// Deprecated: Use Load(ctx, store.BySequenceID(...))
	LoadBySequenceID(ctx context.Context, sequenceID string, opts ...QueryOption) (record []Record, err error)

	// Deprecated: Use Load(ctx, store.BySequenceID(...), store.ByType(...))
	LoadBySequenceIDAndType(ctx context.Context, sequenceID string, eventType string, opts ...QueryOption) (records []Record, err error)

	// Deprecated: Use Load(ctx, store.ByTimestamp(...))
	LoadByTimestamp(ctx context.Context, timestamp int64, opts ...QueryOption) (record []Record, err error)
}

type StoreTransaction interface {
	Commit() error
	Rollback() error
}
```

```
type Serializer interface {
	Unmarshal(data []byte, eventType string) (Event, error)
	Marshal(event Event) ([]byte, error)
}
```
