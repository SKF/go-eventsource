# Introduction [![Go Report Card](https://goreportcard.com/badge/github.com/SKF/go-eventsource)](https://goreportcard.com/report/github.com/SKF/go-eventsource) [![Build Status on master](https://travis-ci.org/SKF/go-eventsource.svg?branch=master)](https://travis-ci.org/SKF/go-eventsource)

go-eventsource is a package for building an Event Store

The package is under development and the interfaces may change.

[Event Sourcing Basics](http://eventstore.org.s3-website.eu-west-2.amazonaws.com/docs/event-sourcing-basics)

# How to use it

To create a new repository:
`NewRepository(store, serializer)`

It has an interface for saving events and loading an aggregate.

```
type Repository interface {
	// Save one or more events to the repository
	Save(ctx context.Context, events ...Event) error

	// Save one or more events to the repository, within a transaction
	SaveTransaction(ctx context.Context, events ...Event) (StoreTransaction, error)

	// Load events from repository for the given aggregate ID. For each event e,
	// call aggr.On(e) to update the state of aggr. When done, aggr has been
	// "fast forwarded" to the current state.
	Load(ctx context.Context, id string, aggr Aggregate) (deleted bool, err error)

	// Get all events
	// Use store specific query options to either limit or sort the events
	// or filters for sequence ID to get newer than the given ID (see https://github.com/oklog/ulid)
	LoadEvents(ctx context.Context, opts ...QueryOptions) (events []Event, err error)
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
	LoadByAggregate(ctx context.Context, aggregateID string, opts ...QueryOptions) (record []Record, err error)
	Load(ctx context.Context, opts ...QueryOptions) (record []Record, err error)
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
