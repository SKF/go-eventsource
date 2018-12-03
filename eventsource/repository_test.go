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

func createMockHistory() Record {
	return Record{
		Data: []byte{byte(3)},
		Type: "mockType",
	}
}

func createMockDataForLoad() (history []Record, baseEvent Event, id string) {
	history = []Record{createMockHistory()}
	baseEvent = BaseEvent{AggregateID: "1-22-333-4444-55555", UserID: "TestMan"}
	id = "1234-1234-1234"
	return
}

func createMockDataForSave() (testEvent Event, testData []byte) {
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

func Test_RepoLoadSuccess(t *testing.T) {
	storeMock, _, serializerMock, aggregatorMock := setupMocks()

	history, baseEvent, id := createMockDataForLoad()

	ctx := context.Background()
	storeMock.On("Load", ctx, id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(3)}, "mockType").Return(baseEvent, nil)
	aggregatorMock.Mock.On("On", ctx, baseEvent).Return(nil)

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
	_, _, id := createMockDataForLoad()

	storeMock.On("Load", context.Background(), id).Return(history, nil)

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
	_, _, id := createMockDataForLoad()

	storeMock.On("Load", context.Background(), id).Return(history, expectedError)

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

	history, baseEvent, id := createMockDataForLoad()

	storeMock.On("Load", context.Background(), id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(3)}, "mockType").Return(baseEvent, expectedError)

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

	history, baseEvent, id := createMockDataForLoad()

	ctx := context.Background()
	storeMock.On("Load", ctx, id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(3)}, "mockType").Return(baseEvent, nil)
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

	history, baseEvent, id := createMockDataForLoad()

	ctx := context.Background()
	storeMock.On("Load", ctx, id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(3)}, "mockType").Return(baseEvent, nil)
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
