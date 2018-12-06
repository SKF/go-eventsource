package eventsource

// Event ...
type Event interface {
	GetAggregateID() string
	GetUserID() string
	GetSequenceID() string
	GetTimestamp() int64
	SetSequenceID(string)
	SetTimestamp(int64)
}

// BaseEvent ...
type BaseEvent struct {
	AggregateID string `json:"aggregateId"`
	UserID      string `json:"userId"`
	SequenceID  string `json:"sequenceId"`
	Timestamp   int64  `json:"timestamp"`
}

// GetAggregateID ...
func (e BaseEvent) GetAggregateID() string {
	return e.AggregateID
}

// GetUserID ...
func (e BaseEvent) GetUserID() string {
	return e.UserID
}

// GetSequenceID ...
func (e BaseEvent) GetSequenceID() string {
	return e.SequenceID
}

// GetTimestamp ...
func (e BaseEvent) GetTimestamp() int64 {
	return e.Timestamp
}

// SetSequenceID ...
func (e *BaseEvent) SetSequenceID(sequenceID string) {
	e.SequenceID = sequenceID
}

// SetTimestamp ...
func (e *BaseEvent) SetTimestamp(timestamp int64) {
	e.Timestamp = timestamp
}
