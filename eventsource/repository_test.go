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
		Data: []byte{},
		Type: "mockType",
	}
}

func createMockDataForLoad() (history []Record, baseEvent Event, id string) {
	history = []Record{createMockHistory()}
	baseEvent = BaseEvent{AggregateID: "", UserID: ""}
	id = "1234-1234-1234"
	return
}

func createMockDataForSave() (testEvent Event, testData []byte) {
	testEvent = BaseEvent{AggregateID: "123", UserID: "KalleKula"}
	testData = []byte{byte(5)}
	return
}

func setupMocks() (storeMock *storeMock, serializerMock *serializerMock, aggrMock *aggrMock) {
	storeMock = CreateStoreMock()
	serializerMock = CreateSerializerMock()
	aggrMock = CreateAggrMock()
	return
}

func Test_RepoLoadSucess(t *testing.T) {
	storeMock, serializerMock, aggregatorMock := setupMocks()

	history, baseEvent, id := createMockDataForLoad()

	storeMock.On("Load", id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{}, "mockType").Return(baseEvent, nil)
	aggregatorMock.Mock.On("On", baseEvent).Return(nil)

	repo := NewRepository(storeMock, serializerMock)
	err := repo.Load(id, aggregatorMock)
	assert.Nil(t, err)

}

func Test_RepoLoadFail_NoHistory(t *testing.T) {
	storeMock, serializerMock, aggregatorMock := setupMocks()

	expectedError := errors.New("No history found")

	history := []Record{}
	_, baseEvent, id := createMockDataForLoad()

	storeMock.On("Load", id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{}, "mockType").Return(baseEvent, nil)
	aggregatorMock.Mock.On("On", baseEvent).Return(nil)

	repo := NewRepository(storeMock, serializerMock)
	err := repo.Load(id, aggregatorMock)

	assert.EqualError(t, err, expectedError.Error())
}

func Test_RepoLoadFail_StoreLoadErr(t *testing.T) {
	storeMock, serializerMock, aggregatorMock := setupMocks()

	expectedError := errors.New("Some error with store.Load")

	history := []Record{}
	_, _, id := createMockDataForLoad()

	storeMock.On("Load", id).Return(history, expectedError)

	repo := NewRepository(storeMock, serializerMock)
	err := repo.Load(id, aggregatorMock)

	assert.EqualError(t, err, expectedError.Error())
}

func Test_RepoLoadFail_UnmarshalErr(t *testing.T) {
	storeMock, serializerMock, aggregatorMock := setupMocks()

	expectedError := errors.New("Some error with serializer.Unmarshal")

	history, baseEvent, id := createMockDataForLoad()

	storeMock.On("Load", id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{}, "mockType").Return(baseEvent, expectedError)

	repo := NewRepository(storeMock, serializerMock)
	err := repo.Load(id, aggregatorMock)

	assert.EqualError(t, err, expectedError.Error())
}

func Test_RepoLoadFail_AggrOnUnkownErr(t *testing.T) {
	storeMock, serializerMock, aggregatorMock := setupMocks()

	expectedError := errors.New("Some error with aggr.On")

	history, baseEvent, id := createMockDataForLoad()

	storeMock.On("Load", id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{}, "mockType").Return(baseEvent, nil)
	aggregatorMock.Mock.On("On", baseEvent).Return(expectedError)

	repo := NewRepository(storeMock, serializerMock)
	err := repo.Load(id, aggregatorMock)

	assert.NotNil(t, aggregatorMock)
	assert.EqualError(t, err, expectedError.Error())
}

func Test_RepoLoadFail_AggrOnUnkownErr2(t *testing.T) {
	storeMock, serializerMock, aggregatorMock := setupMocks()

	history, baseEvent, id := createMockDataForLoad()

	storeMock.On("Load", id).Return(history, nil)
	serializerMock.On("Unmarshal", []byte{}, "mockType").Return(baseEvent, nil)
	aggregatorMock.Mock.On("On", baseEvent).Return(ErrDeleted)

	repo := NewRepository(storeMock, serializerMock)
	err := repo.Load(id, aggregatorMock)
	assert.Nil(t, err)
}

func Test_RepoSaveSucess(t *testing.T) {
	storeMock, serializerMock, _ := setupMocks()

	testEvent, testData := createMockDataForSave()

	serializerMock.On("Marshal", testEvent).Return(testData, nil)
	storeMock.On("Save", mock.MatchedBy(func(r Record) bool {
		return matchRecord(r, testEvent, testData)
	})).Return(nil)

	repo := NewRepository(storeMock, serializerMock)
	err := repo.Save(testEvent)

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

	assert.EqualError(t, err, expectedError.Error())

}
