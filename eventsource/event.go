package eventsource

// Event ...
type Event interface {
	GetAggregateID() string
	GetUserID() (userID string)
}

// BaseEvent ...
type BaseEvent struct {
	AggregateID string
	UserID      string
}

// GetAggregateID ...
func (e BaseEvent) GetAggregateID() string {
	return e.AggregateID
}

// GetUserID ...
func (e BaseEvent) GetUserID() string {
	return e.UserID
}
