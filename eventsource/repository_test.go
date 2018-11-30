package eventsource

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func createMockHistory() []Record {
	return []Record{
		{
			Data: []byte{byte(2)},
			Type: "BaseEvent",
			SequenceID: "1",
		},
		{
			Data: []byte{byte(0)},
			Type: "BaseEvent",
			SequenceID: "2",
		},
		{
			Data: []byte{byte(1)},
			Type: "BaseEvent",
			SequenceID: "3",
		},
		{
			Data: []byte{byte(3)},
			Type: "OtherEvent",
			SequenceID: "4",
		},
	}
}

func filterHistoryBySeqID(history []Record, sequenceID string) (filtered []Record) {
	for _, record := range history {
		if record.SequenceID > sequenceID {
			filtered = append(filtered, record)
		}
	}
	return
}

type OtherEvent struct{
	*BaseEvent
	OtherEventField int
}

func createMockDataForLoadAggregate() (history []Record, baseEvent BaseEvent, id string) {
	history = createMockHistory()
	baseEvent = BaseEvent{AggregateID: "1-22-333-4444-55555", UserID: "TestMan"}
	id = "1234-1234-1234"
	return
}

func createMockDataForSave() (testEvent BaseEvent, testData []byte) {
	testEvent = BaseEvent{AggregateID: "123", UserID: "KalleKula"}
	testData = []byte{byte(5)}
	return
}

func setupMocks() (storeMock *StoreMock, storeTransactionMock *StoreTransactionMock, serializerMock *SerializerMock, aggregatorMock *AggregatorMock) {
	storeMock = CreateStoreMock()
	storeTransactionMock = CreateStoreTransactionMock()
	serializerMock = CreateSerializerMock()
	aggregatorMock = CreateAggregatorMock()
	return
}

func Test_RepoGetRecords(t *testing.T) {
	storeMock, _, serializerMock, aggregatorMock := setupMocks()

	history, baseEvent, _ := createMockDataForLoadAggregate()
	otherEvent := OtherEvent{BaseEvent: &baseEvent, OtherEventField: 42}

	ctx := context.Background()
	storeMock.On("LoadNewerThan", ctx, "2").Return(filterHistoryBySeqID(history, "2"), nil)
	serializerMock.On("Unmarshal", []byte{byte(1)}, "BaseEvent").Return(baseEvent, nil)
	serializerMock.On("Unmarshal", []byte{byte(3)}, "OtherEvent").Return(otherEvent, nil)

	repo := NewRepository(storeMock, serializerMock)
	records, err := repo.GetRecords(ctx, "2")

	aggregatorMock.Mock.AssertExpectations(t)
	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	assert.Nil(t, err)
	assert.Equal(t, len(records), 2)
	assert.Equal(t, records[0].Record.Data, []byte{byte(1)})
	assert.Equal(t, records[1].Record.Data, []byte{byte(3)})
	assert.Equal(t, records[0].Event.(BaseEvent), baseEvent, true)
	assert.Equal(t, records[1].Event.(OtherEvent), otherEvent, true)
}

func Test_RepoLoadSuccess(t *testing.T) {
	storeMock, _, serializerMock, aggregatorMock := setupMocks()

	history, baseEvent, id := createMockDataForLoadAggregate()
	otherEvent := OtherEvent{BaseEvent: &baseEvent, OtherEventField: 42}

	ctx := context.Background()
	storeMock.On("LoadAggregate", ctx, id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(2)}, "BaseEvent").Return(baseEvent, nil)
	serializerMock.On("Unmarshal", []byte{byte(0)}, "BaseEvent").Return(baseEvent, nil)
	serializerMock.On("Unmarshal", []byte{byte(1)}, "BaseEvent").Return(baseEvent, nil)
	serializerMock.On("Unmarshal", []byte{byte(3)}, "OtherEvent").Return(otherEvent, nil)
	aggregatorMock.Mock.On("On", ctx, baseEvent).Return(nil)
	aggregatorMock.Mock.On("On", ctx, baseEvent).Return(nil)
	aggregatorMock.Mock.On("On", ctx, baseEvent).Return(nil)
	aggregatorMock.Mock.On("On", ctx, otherEvent).Return(nil)

	repo := NewRepository(storeMock, serializerMock)
	deleted, err := repo.Load(ctx, id, aggregatorMock)

	aggregatorMock.Mock.AssertExpectations(t)
	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	assert.Nil(t, err)
	assert.False(t, deleted)
}

func Test_RepoLoadFail_NoHistory(t *testing.T) {
	storeMock, _, serializerMock, aggregatorMock := setupMocks()

	history := []Record{}
	_, _, id := createMockDataForLoadAggregate()

	storeMock.On("LoadAggregate", context.Background(), id).Return(history, nil)

	ctx := context.Background()
	repo := NewRepository(storeMock, serializerMock)
	deleted, err := repo.Load(ctx, id, aggregatorMock)

	storeMock.AssertExpectations(t)
	assert.EqualError(t, err, ErrNoHistory.Error())
	assert.False(t, deleted)
}

func Test_RepoLoadFail_StoreLoadErr(t *testing.T) {
	storeMock, _, serializerMock, aggregatorMock := setupMocks()

	expectedError := errors.New("Some error with store.Load")

	history := []Record{}
	_, _, id := createMockDataForLoadAggregate()

	storeMock.On("LoadAggregate", context.Background(), id).Return(history, expectedError)

	ctx := context.Background()
	repo := NewRepository(storeMock, serializerMock)
	deleted, err := repo.Load(ctx, id, aggregatorMock)

	storeMock.AssertExpectations(t)
	assert.EqualError(t, err, expectedError.Error())
	assert.False(t, deleted)
}

func Test_RepoLoadFail_UnmarshalErr(t *testing.T) {
	storeMock, _, serializerMock, aggregatorMock := setupMocks()

	expectedError := errors.New("Some error with serializer.Unmarshal")

	history, baseEvent, id := createMockDataForLoadAggregate()

	storeMock.On("LoadAggregate", context.Background(), id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(2)}, "BaseEvent").Return(baseEvent, expectedError)

	ctx := context.Background()
	repo := NewRepository(storeMock, serializerMock)
	deleted, err := repo.Load(ctx, id, aggregatorMock)

	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	assert.EqualError(t, err, expectedError.Error())
	assert.False(t, deleted)
}

func Test_RepoLoadFail_AggrOnUnkownErr(t *testing.T) {
	storeMock, _, serializerMock, aggregatorMock := setupMocks()

	expectedError := errors.New("Some error with aggr.On")

	history, baseEvent, id := createMockDataForLoadAggregate()

	ctx := context.Background()
	storeMock.On("LoadAggregate", ctx, id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(2)}, "BaseEvent").Return(baseEvent, nil)
	aggregatorMock.Mock.On("On", ctx, baseEvent).Return(expectedError)

	repo := NewRepository(storeMock, serializerMock)
	deleted, err := repo.Load(ctx, id, aggregatorMock)

	aggregatorMock.Mock.AssertExpectations(t)
	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	assert.EqualError(t, err, expectedError.Error())
	assert.False(t, deleted)
}

func Test_RepoLoadFail_AggrOnErrDeleted(t *testing.T) {
	storeMock, _, serializerMock, aggregatorMock := setupMocks()

	history, baseEvent, id := createMockDataForLoadAggregate()

	ctx := context.Background()
	storeMock.On("LoadAggregate", ctx, id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(2)}, "BaseEvent").Return(baseEvent, nil)
	aggregatorMock.Mock.On("On", ctx, baseEvent).Return(ErrDeleted)

	repo := NewRepository(storeMock, serializerMock)
	deleted, err := repo.Load(ctx, id, aggregatorMock)

	aggregatorMock.Mock.AssertExpectations(t)
	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	assert.Nil(t, err)
	assert.True(t, deleted)
}

func Test_RepoSaveSuccess(t *testing.T) {
	storeMock, storeTransactionMock, serializerMock, _ := setupMocks()
	testEvent, testData := createMockDataForSave()

	ctx := context.Background()
	serializerMock.On("Marshal", testEvent).Return(testData, nil)
	storeMock.On("NewTransaction", ctx, mock.MatchedBy(func(rs []Record) bool {
		return len(rs) == 1 && matchRecord(rs[0], testEvent, testData)
	})).Return(storeTransactionMock, nil).Once()
	storeTransactionMock.On("Commit").Return(nil).Once()

	repo := NewRepository(storeMock, serializerMock)
	err := repo.Save(ctx, testEvent)

	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	storeTransactionMock.AssertExpectations(t)
	assert.Nil(t, err)
}

func matchRecord(r Record, e Event, testData []byte) bool {
	return r.AggregateID == e.GetAggregateID() && r.UserID == e.GetUserID() && bytes.Equal(r.Data, testData)
}

func Test_RepoSaveFail_MarshalErr(t *testing.T) {
	storeMock, _, serializerMock, _ := setupMocks()

	expectedError := errors.New("Some error with serializer.Marshal")
	testEvent, testData := createMockDataForSave()

	serializerMock.On("Marshal", testEvent).Return(testData, expectedError)

	ctx := context.Background()
	repo := NewRepository(storeMock, serializerMock)
	err := repo.Save(ctx, testEvent)

	serializerMock.AssertExpectations(t)
	assert.EqualError(t, err, expectedError.Error())
}

func Test_RepoSaveFail_SaveErr(t *testing.T) {
	storeMock, storeTransactionMock, serializerMock, _ := setupMocks()

	expectedError := errors.New("Some error with store.Save")
	testEvent, testData := createMockDataForSave()

	ctx := context.Background()
	serializerMock.On("Marshal", testEvent).Return(testData, nil)
	storeMock.On("NewTransaction", ctx, mock.Anything).Return(storeTransactionMock, nil).Once()
	storeTransactionMock.On("Commit").Return(expectedError).Once()
	storeTransactionMock.On("Rollback").Return(nil).Once()

	repo := NewRepository(storeMock, serializerMock)
	err := repo.Save(ctx, testEvent)

	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	storeTransactionMock.AssertExpectations(t)
	assert.EqualError(t, err, fmt.Sprintf("Rollback error: <nil>, Save error: %+v", expectedError))
}

func Test_RepoMock_OK(t *testing.T) {
	repoMock := CreateRepositoryMock()
	assert.NotNil(t, repoMock)

	// Test save
	ctx := context.Background()
	repoMock.On("Save", ctx, mock.Anything).Return(nil).Once()

	err := repoMock.Save(ctx, BaseEvent{})
	assert.Nil(t, err)

	expectedError := errors.New("Some error with store.Save")
	repoMock.On("Save", ctx, mock.Anything).Return(expectedError)
	err = repoMock.Save(ctx, BaseEvent{})
	assert.EqualError(t, err, expectedError.Error())

	// Load
	repoMock.On("Load", ctx, mock.Anything, mock.Anything).Return(false, nil).Once()

	deleted, err := repoMock.Load(ctx, "123", nil)
	assert.Nil(t, err)
	assert.False(t, deleted)

	repoMock.On("Load", ctx, mock.Anything, mock.Anything).Return(false, expectedError)
	deleted, err = repoMock.Load(ctx, "123", nil)
	assert.EqualError(t, err, expectedError.Error())
	assert.False(t, deleted)
}
