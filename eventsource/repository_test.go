package eventsource

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type OnSaveEvent struct {
	*BaseEvent
	OnSaveError error
}

func (s OnSaveEvent) GetAggregateID() string {
	return s.BaseEvent.GetAggregateID()
}

func (s OnSaveEvent) GetUserID() string {
	return s.BaseEvent.GetUserID()
}

func (s OnSaveEvent) OnSave(_ Record) error {
	return s.OnSaveError
}

type OnSaveEventWithContext struct {
	*BaseEvent
	OnSaveError error
}

func (s OnSaveEventWithContext) GetAggregateID() string {
	return s.BaseEvent.GetAggregateID()
}

func (s OnSaveEventWithContext) GetUserID() string {
	return s.BaseEvent.GetUserID()
}

func (s OnSaveEventWithContext) OnSave(_ context.Context, _ Record) error {
	return s.OnSaveError
}

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

func setupMocks() (storeMock *StoreMock, serializerMock *SerializerMock, aggregatorMock *AggregatorMock) {
	storeMock = CreateStoreMock()
	serializerMock = CreateSerializerMock()
	aggregatorMock = CreateAggregatorMock()
	return
}

func Test_RepoLoadSuccess(t *testing.T) {
	storeMock, serializerMock, aggregatorMock := setupMocks()

	history, baseEvent, id := createMockDataForLoad()

	ctx := context.Background()
	storeMock.On("LoadWithContext", ctx, id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(3)}, "mockType").Return(baseEvent, nil)
	aggregatorMock.Mock.On("On", ctx, baseEvent).Return(nil)

	repo := NewRepository(storeMock, serializerMock)
	deleted, err := repo.Load(id, aggregatorMock)

	aggregatorMock.Mock.AssertExpectations(t)
	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	assert.Nil(t, err)
	assert.False(t, deleted)
}

func Test_RepoLoadFail_NoHistory(t *testing.T) {
	storeMock, serializerMock, aggregatorMock := setupMocks()

	history := []Record{}
	_, _, id := createMockDataForLoad()

	storeMock.On("LoadWithContext", context.Background(), id).Return(history, nil)

	repo := NewRepository(storeMock, serializerMock)
	deleted, err := repo.Load(id, aggregatorMock)

	storeMock.AssertExpectations(t)
	assert.EqualError(t, err, ErrNoHistory.Error())
	assert.False(t, deleted)
}

func Test_RepoLoadFail_StoreLoadErr(t *testing.T) {
	storeMock, serializerMock, aggregatorMock := setupMocks()

	expectedError := errors.New("Some error with store.Load")

	history := []Record{}
	_, _, id := createMockDataForLoad()

	storeMock.On("LoadWithContext", context.Background(), id).Return(history, expectedError)

	repo := NewRepository(storeMock, serializerMock)
	deleted, err := repo.Load(id, aggregatorMock)

	storeMock.AssertExpectations(t)
	assert.EqualError(t, err, expectedError.Error())
	assert.False(t, deleted)
}

func Test_RepoLoadFail_UnmarshalErr(t *testing.T) {
	storeMock, serializerMock, aggregatorMock := setupMocks()

	expectedError := errors.New("Some error with serializer.Unmarshal")

	history, baseEvent, id := createMockDataForLoad()

	storeMock.On("LoadWithContext", context.Background(), id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(3)}, "mockType").Return(baseEvent, expectedError)

	repo := NewRepository(storeMock, serializerMock)
	deleted, err := repo.Load(id, aggregatorMock)

	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	assert.EqualError(t, err, expectedError.Error())
	assert.False(t, deleted)
}

func Test_RepoLoadFail_AggrOnUnkownErr(t *testing.T) {
	storeMock, serializerMock, aggregatorMock := setupMocks()

	expectedError := errors.New("Some error with aggr.On")

	history, baseEvent, id := createMockDataForLoad()

	ctx := context.Background()
	storeMock.On("LoadWithContext", ctx, id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(3)}, "mockType").Return(baseEvent, nil)
	aggregatorMock.Mock.On("On", ctx, baseEvent).Return(expectedError)

	repo := NewRepository(storeMock, serializerMock)
	deleted, err := repo.Load(id, aggregatorMock)

	aggregatorMock.Mock.AssertExpectations(t)
	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	assert.EqualError(t, err, expectedError.Error())
	assert.False(t, deleted)
}

func Test_RepoLoadFail_AggrOnErrDeleted(t *testing.T) {
	storeMock, serializerMock, aggregatorMock := setupMocks()

	history, baseEvent, id := createMockDataForLoad()

	ctx := context.Background()
	storeMock.On("LoadWithContext", ctx, id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(3)}, "mockType").Return(baseEvent, nil)
	aggregatorMock.Mock.On("On", ctx, baseEvent).Return(ErrDeleted)

	repo := NewRepository(storeMock, serializerMock)
	deleted, err := repo.Load(id, aggregatorMock)

	aggregatorMock.Mock.AssertExpectations(t)
	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	assert.Nil(t, err)
	assert.True(t, deleted)
}

func Test_RepoSaveSuccess(t *testing.T) {
	storeMock, serializerMock, _ := setupMocks()
	testEvent, testData := createMockDataForSave()

	ctx := context.Background()
	serializerMock.On("Marshal", testEvent).Return(testData, nil)
	storeMock.On("SaveWithContext", ctx, mock.MatchedBy(func(rs []Record) bool {
		return len(rs) == 1 && matchRecord(rs[0], testEvent, testData)
	})).Return(nil)

	repo := NewRepository(storeMock, serializerMock)
	err := repo.Save(testEvent)

	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	assert.Nil(t, err)
}

func matchRecord(r Record, e Event, testData []byte) bool {
	return r.AggregateID == e.GetAggregateID() && r.UserID == e.GetUserID() && bytes.Equal(r.Data, testData)
}

func Test_RepoSaveFail_MarshalErr(t *testing.T) {
	storeMock, serializerMock, _ := setupMocks()

	expectedError := errors.New("Some error with serializer.Marshal")
	testEvent, testData := createMockDataForSave()

	serializerMock.On("Marshal", testEvent).Return(testData, expectedError)

	repo := NewRepository(storeMock, serializerMock)
	err := repo.Save(testEvent)

	serializerMock.AssertExpectations(t)
	assert.EqualError(t, err, expectedError.Error())
}

func Test_RepoOnSave_SaveFail(t *testing.T) {
	storeMock, serializerMock, _ := setupMocks()

	expectedError := errors.New("OnSaveFailed")
	testEvent := OnSaveEvent{
		BaseEvent:   &BaseEvent{AggregateID: "123", UserID: "KalleKula"},
		OnSaveError: expectedError,
	}
	testData := []byte{byte(5)}

	ctx := context.Background()
	serializerMock.On("Marshal", testEvent).Return(testData, nil)
	storeMock.On("SaveWithContext", ctx, mock.Anything).Return(nil)

	repo := NewRepository(storeMock, serializerMock)
	err := repo.Save(testEvent)

	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	assert.EqualError(t, err, expectedError.Error())
}

func Test_RepoOnSaveWithContext_SaveFail(t *testing.T) {
	storeMock, serializerMock, _ := setupMocks()

	expectedError := errors.New("OnSaveWithContextFailed")
	testEvent := OnSaveEventWithContext{
		BaseEvent:   &BaseEvent{AggregateID: "123", UserID: "KalleKula"},
		OnSaveError: expectedError,
	}
	testData := []byte{byte(5)}

	ctx := context.Background()
	serializerMock.On("Marshal", testEvent).Return(testData, nil)
	storeMock.On("SaveWithContext", ctx, mock.Anything).Return(nil)

	repo := NewRepository(storeMock, serializerMock)
	err := repo.SaveWithContext(context.Background(), testEvent)

	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	assert.EqualError(t, err, expectedError.Error())
}

func Test_RepoOnSave_SaveSuccess(t *testing.T) {
	storeMock, serializerMock, _ := setupMocks()

	testEvent := OnSaveEvent{
		BaseEvent:   &BaseEvent{AggregateID: "123", UserID: "KalleKula"},
		OnSaveError: nil,
	}
	testData := []byte{byte(5)}

	ctx := context.Background()
	serializerMock.On("Marshal", testEvent).Return(testData, nil)
	storeMock.On("SaveWithContext", ctx, mock.Anything).Return(nil)

	repo := NewRepository(storeMock, serializerMock)
	err := repo.Save(testEvent)

	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	assert.Nil(t, err)
}

func Test_RepoSaveFail_SaveErr(t *testing.T) {
	storeMock, serializerMock, _ := setupMocks()

	expectedError := errors.New("Some error with store.Save")
	testEvent, testData := createMockDataForSave()

	ctx := context.Background()
	serializerMock.On("Marshal", testEvent).Return(testData, nil)
	storeMock.On("SaveWithContext", ctx, mock.Anything).Return(expectedError)

	repo := NewRepository(storeMock, serializerMock)
	err := repo.Save(testEvent)

	serializerMock.AssertExpectations(t)
	storeMock.AssertExpectations(t)
	assert.EqualError(t, err, expectedError.Error())
}

func Test_RepoMock_OK(t *testing.T) {
	repoMock := CreateRepositoryMock()
	assert.NotNil(t, repoMock)

	// Test save
	repoMock.On("Save", mock.Anything).Return(nil).Once()

	err := repoMock.Save(BaseEvent{})
	assert.Nil(t, err)

	expectedError := errors.New("Some error with store.Save")
	repoMock.On("Save", mock.Anything).Return(expectedError)
	err = repoMock.Save(BaseEvent{})
	assert.EqualError(t, err, expectedError.Error())

	// Load
	repoMock.On("Load", mock.Anything, mock.Anything).Return(false, nil).Once()

	deleted, err := repoMock.Load("123", nil)
	assert.Nil(t, err)
	assert.False(t, deleted)

	repoMock.On("Load", mock.Anything, mock.Anything).Return(false, expectedError)
	deleted, err = repoMock.Load("123", nil)
	assert.EqualError(t, err, expectedError.Error())
	assert.False(t, deleted)
}
