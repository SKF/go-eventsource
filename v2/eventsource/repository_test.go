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
			Data:       []byte{byte(2)},
			Type:       "BaseEvent",
			SequenceID: "1",
		},
		{
			Data:       []byte{byte(0)},
			Type:       "BaseEvent",
			SequenceID: "2",
		},
		{
			Data:       []byte{byte(1)},
			Type:       "BaseEvent",
			SequenceID: "3",
		},
		{
			Data:       []byte{byte(3)},
			Type:       "OtherEvent",
			SequenceID: "4",
			Timestamp:  555,
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

type OtherEvent struct {
	*BaseEvent
	OtherEventField int
}

func createMockDataForLoadAggregate() (history []Record, baseEvent *BaseEvent, id string) {
	history = createMockHistory()
	baseEvent = &BaseEvent{AggregateID: "1-22-333-4444-55555", UserID: "TestMan"}
	id = "1234-1234-1234"
	return
}

func createMockDataForSave() (testEvent *BaseEvent, testData []byte) {
	testEvent = &BaseEvent{AggregateID: "123", UserID: "KalleKula"}
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
	otherEvent := &OtherEvent{BaseEvent: baseEvent, OtherEventField: 42}

	ctx := context.TODO()
	storeMock.On("Load", ctx, []QueryOption(nil)).Return(filterHistoryBySeqID(history, "2"), nil)
	serializerMock.On("Unmarshal", []byte{byte(1)}, "BaseEvent").Return(baseEvent, nil)
	serializerMock.On("Unmarshal", []byte{byte(3)}, "OtherEvent").Return(otherEvent, nil)

	repo := NewRepository(storeMock, serializerMock)
	records, err := repo.LoadEvents(ctx)

	aggregatorMock.Mock.AssertExpectations(t)
	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	assert.Nil(t, err)
	assert.Equal(t, len(records), 2)
	assert.Equal(t, records[0].(*BaseEvent), baseEvent, true)
	assert.Equal(t, records[1].(*OtherEvent), otherEvent, true)
}

func Test_RepoLoadSuccess(t *testing.T) {
	storeMock, _, serializerMock, aggregatorMock := setupMocks()

	history, baseEvent, id := createMockDataForLoadAggregate()
	otherEvent := OtherEvent{BaseEvent: baseEvent, OtherEventField: 42}
	otherBaseEvent := &BaseEvent{AggregateID: "1-22-333-4444-55555", UserID: "TestMan", Timestamp: 555}
	otherEventCompare := OtherEvent{BaseEvent: otherBaseEvent, OtherEventField: 42}

	ctx := context.TODO()
	storeMock.On("LoadByAggregate", ctx, id, []QueryOption(nil)).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(2)}, "BaseEvent").Return(baseEvent, nil)
	serializerMock.On("Unmarshal", []byte{byte(0)}, "BaseEvent").Return(baseEvent, nil)
	serializerMock.On("Unmarshal", []byte{byte(1)}, "BaseEvent").Return(baseEvent, nil)
	serializerMock.On("Unmarshal", []byte{byte(3)}, "OtherEvent").Return(otherEvent, nil)
	aggregatorMock.Mock.On("On", ctx, baseEvent).Return(nil)
	aggregatorMock.Mock.On("On", ctx, baseEvent).Return(nil)
	aggregatorMock.Mock.On("On", ctx, baseEvent).Return(nil)
	aggregatorMock.Mock.On("On", ctx, otherEventCompare).Return(nil)

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

	ctx := context.TODO()

	storeMock.On("LoadByAggregate", ctx, id, []QueryOption(nil)).Return(history, nil)

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

	ctx := context.TODO()

	storeMock.On("LoadByAggregate", ctx, id, []QueryOption(nil)).Return(history, expectedError)

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
	ctx := context.TODO()

	storeMock.On("LoadByAggregate", ctx, id, []QueryOption(nil)).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(2)}, "BaseEvent").Return(baseEvent, expectedError)

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

	ctx := context.TODO()
	storeMock.On("LoadByAggregate", ctx, id, []QueryOption(nil)).Return(history, nil)
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

	ctx := context.TODO()
	storeMock.On("LoadByAggregate", ctx, id, []QueryOption(nil)).Return(history, nil)
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

	ctx := context.TODO()

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

func Test_RepoSaveSuccessNotification(t *testing.T) {
	storeMock, storeTransactionMock, serializerMock, _ := setupMocks()
	testEvent, testData := createMockDataForSave()
	notificationService := CreateNotificationServiceMock()

	ctx := context.TODO()

	serializerMock.On("Marshal", testEvent).Return(testData, nil)
	storeMock.On("NewTransaction", ctx, mock.MatchedBy(func(rs []Record) bool {
		return len(rs) == 1 && matchRecord(rs[0], testEvent, testData)
	})).Return(storeTransactionMock, nil).Once()
	storeTransactionMock.On("GetRecords").Return([]Record{{UserID: testEvent.UserID, AggregateID: testEvent.AggregateID, Data: testData}}).Twice()
	storeTransactionMock.On("Commit").Return(nil).Once()
	notificationService.On("SendWithContext", ctx, mock.MatchedBy(func(r Record) bool {
		return matchRecord(r, testEvent, testData)
	})).Return(nil).Twice()

	repo := NewRepository(storeMock, serializerMock)
	repo.AddNotificationService(notificationService)
	repo.AddNotificationService(notificationService)
	err := repo.Save(ctx, testEvent)

	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	storeTransactionMock.AssertExpectations(t)
	notificationService.AssertExpectations(t)
	assert.NoError(t, err)
}

func Test_RepoSaveFailNoNotification(t *testing.T) {
	storeMock, storeTransactionMock, serializerMock, _ := setupMocks()
	testEvent, testData := createMockDataForSave()
	notificationService := CreateNotificationServiceMock()

	ctx := context.TODO()

	serializerMock.On("Marshal", testEvent).Return(testData, nil)
	storeMock.On("NewTransaction", ctx, mock.MatchedBy(func(rs []Record) bool {
		return len(rs) == 1 && matchRecord(rs[0], testEvent, testData)
	})).Return(storeTransactionMock, nil).Once()
	storeTransactionMock.On("Commit").Return(errors.New("some error")).Once()
	storeTransactionMock.On("Rollback").Return(nil).Once()

	repo := NewRepository(storeMock, serializerMock)
	repo.AddNotificationService(notificationService)
	err := repo.Save(ctx, testEvent)
	assert.EqualError(t, err, "failed to commit transaction: some error")

	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	storeTransactionMock.AssertExpectations(t)
	notificationService.AssertExpectations(t)
}

func matchRecord(r Record, e Event, testData []byte) bool {
	return r.AggregateID == e.GetAggregateID() && r.UserID == e.GetUserID() && bytes.Equal(r.Data, testData)
}

func Test_RepoSaveFail_MarshalErr(t *testing.T) {
	storeMock, _, serializerMock, _ := setupMocks()

	expectedError := errors.New("Some error with serializer.Marshal")
	testEvent, testData := createMockDataForSave()

	serializerMock.On("Marshal", testEvent).Return(testData, expectedError)

	ctx := context.TODO()
	repo := NewRepository(storeMock, serializerMock)
	err := repo.Save(ctx, testEvent)

	serializerMock.AssertExpectations(t)
	assert.EqualError(t, err, expectedError.Error())
}

func Test_RepoSaveFail_SaveErr(t *testing.T) {
	storeMock, storeTransactionMock, serializerMock, _ := setupMocks()

	expectedError := errors.New("Some error with store.Save")
	testEvent, testData := createMockDataForSave()

	ctx := context.TODO()

	serializerMock.On("Marshal", testEvent).Return(testData, nil)
	storeMock.On("NewTransaction", ctx, mock.Anything).Return(storeTransactionMock, nil).Once()
	storeTransactionMock.On("Commit").Return(expectedError).Once()
	storeTransactionMock.On("Rollback").Return(nil).Once()

	repo := NewRepository(storeMock, serializerMock)
	err := repo.Save(ctx, testEvent)

	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	storeTransactionMock.AssertExpectations(t)
	assert.EqualError(t, err, fmt.Sprintf("failed to commit transaction: %s", expectedError))
}

func Test_RepoMock_OK(t *testing.T) {
	repoMock := CreateRepositoryMock()
	assert.NotNil(t, repoMock)

	// Test save
	ctx := context.TODO()
	repoMock.On("Save", ctx, mock.Anything).Return(nil).Once()

	err := repoMock.Save(ctx, &BaseEvent{})
	assert.Nil(t, err)

	expectedError := errors.New("Some error with store.Save")
	repoMock.On("Save", ctx, mock.Anything).Return(expectedError)
	err = repoMock.Save(ctx, &BaseEvent{})
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

func TestSaveTransaction_WithPredefinedTimestamp(t *testing.T) {
	event := &BaseEvent{
		AggregateID: "timestamp-test",
		UserID:      "Kalle Banka",
		SequenceID:  "0000XSNJG0MQJHBF4QX1EFD6Y3",
		Timestamp:   1257894000000000000,
	}

	store, transaction, serializer, _ := setupMocks()
	store.On("NewTransaction", mock.Anything, mock.Anything).Return(transaction, nil)
	serializer.On("Marshal", mock.Anything).Return([]byte{1}, nil)

	repo := NewRepository(store, serializer)

	_, err := repo.SaveTransaction(context.Background(), event)

	assert.NoError(t, err)
	assert.Equal(t, int64(1257894000000000000), event.Timestamp)
}

func TestSaveTransaction_TimestampZero(t *testing.T) {
	event := &BaseEvent{
		AggregateID: "timestamp-test",
		UserID:      "Kalle Anka",
		SequenceID:  "0000XSNJG0MQJHBF4QX1EFD6Y3",
		Timestamp:   0,
	}

	store, transaction, serializer, _ := setupMocks()
	store.On("NewTransaction", mock.Anything, mock.Anything).Return(transaction, nil)
	serializer.On("Marshal", mock.Anything).Return([]byte{1}, nil)

	repo := NewRepository(store, serializer)

	_, err := repo.SaveTransaction(context.Background(), event)

	assert.NoError(t, err)
	assert.NotEqual(t, 0, event.Timestamp)
}
