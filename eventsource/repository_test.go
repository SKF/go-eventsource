package eventsource

import (
	"bytes"
	"errors"
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

func setupMocks() (storeMock *storeMock, serializerMock *serializerMock, aggregatorMock *aggregatorMock) {
	storeMock = CreateStoreMock()
	serializerMock = CreateSerializerMock()
	aggregatorMock = CreateAggregatorMock()
	return
}

func Test_RepoLoadSuccess(t *testing.T) {
	storeMock, serializerMock, aggregatorMock := setupMocks()

	history, baseEvent, id := createMockDataForLoad()

	storeMock.On("Load", id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(3)}, "mockType").Return(baseEvent, nil)
	aggregatorMock.Mock.On("On", baseEvent).Return(nil)

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

	storeMock.On("Load", id).Return(history, nil)

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

	storeMock.On("Load", id).Return(history, expectedError)

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

	storeMock.On("Load", id).Return(history, nil)
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

	storeMock.On("Load", id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(3)}, "mockType").Return(baseEvent, nil)
	aggregatorMock.Mock.On("On", baseEvent).Return(expectedError)

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

	storeMock.On("Load", id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{byte(3)}, "mockType").Return(baseEvent, nil)
	aggregatorMock.Mock.On("On", baseEvent).Return(ErrDeleted)

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

	serializerMock.On("Marshal", testEvent).Return(testData, nil)
	storeMock.On("Save", mock.MatchedBy(func(r Record) bool {
		return matchRecord(r, testEvent, testData)
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

func Test_RepoSaveFail_SaveErr(t *testing.T) {
	storeMock, serializerMock, _ := setupMocks()

	expectedError := errors.New("Some error with store.Save")
	testEvent, testData := createMockDataForSave()

	serializerMock.On("Marshal", testEvent).Return(testData, nil)
	storeMock.On("Save", mock.Anything).Return(expectedError)

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
	repoMock.On("Load", mock.Anything, mock.Anything).Return(nil).Once()

	err = repoMock.Load("123", nil)
	assert.Nil(t, err)

	repoMock.On("Load", mock.Anything, mock.Anything).Return(expectedError)
	err = repoMock.Load("123", nil)
	assert.EqualError(t, err, expectedError.Error())
}
