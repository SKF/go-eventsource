package recorder_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/SKF/go-eventsource/v2/eventsource"
	"github.com/SKF/go-eventsource/v2/eventsource/notification/recorder"
)

func TestRecorder_SendEvent(t *testing.T) {
	r := recorder.Recorder{}

	d := struct {
		Value1 string `json:"value1"`
	}{
		Value1: "apa",
	}

	bytes, err := json.Marshal(&d)
	require.NoError(t, err)

	e := eventsource.Record{
		Data:        bytes,
	}

	err = r.Send(e)
	require.NoError(t, err)


	events := r.GetEventDatas()
	require.Equal(t, "apa", events[0]["value1"])
}
