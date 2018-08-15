package json

import (
	"strings"
	"testing"
)

func Test_getTypeOfValue(t *testing.T) {
	var tests = []struct {
		input  interface{}
		output string
	}{
		{&struct{}{}, "struct {}"},
		{struct{}{}, "struct {}"},
		{1.0, "float64"},
		{"foobar", "string"},
		{1, "int"},
	}
	for _, test := range tests {
		actual := getTypeOfValue(test.input).String()
		if strings.Compare(actual, test.output) != 0 {
			t.Errorf("Expected %v to equal %s", actual, test.output)
		}
	}
}
