# Introduction 
go-eventsource is a package for building an Event Store

[Event Sourcing Basics](http://eventstore.org.s3-website.eu-west-2.amazonaws.com/docs/event-sourcing-basics)

# How to use it
To create a new repository:
`NewRepository(store, aggregate, serializer)`

It has an interface for saving events and loading an aggregate.
```
type Repository interface {
	Save(events ...Event) (error)
	Load(id string) (Aggregate, error)
}
```

The package comes with one serializer and two stores:

Included serializer:
- `json`

Included stores:
- `dynamodb`
- `memory`

If you want to add your own store or serializer, the package has these defined interfaces.

```
type Store interface {
	Save(record Record) error
	Load(id string) ([]Record, error)
}
```
```
type Serializer interface {
	Unmarshal(data []byte, eventType string) (Event, error)
	Marshal(event Event) ([]byte, error)
}
```