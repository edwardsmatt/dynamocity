package dynamocity

import (
	"encoding/json"
	"testing"
	"time"
)

func Test_StringSortedness(t *testing.T) {
	sortedStrings := []string{
		"2019-12-09T06:50:02.533237329Z",
		"2019-12-09T06:50:02.53323732Z",
		"2019-12-09T06:50:02.5332373Z",
		"2019-12-09T06:50:02.533237Z",
		"2019-12-09T06:50:02.53323Z",
		"2019-12-09T06:50:02.5332Z",
		"2019-12-09T06:50:02.533Z",
		"2019-12-09T06:50:02.53Z",
		"2019-12-09T06:50:02.5Z",
		"2019-12-09T06:50:02Z",
	}

	isFirst := true
	for i, timeStr := range sortedStrings {
		if isFirst {
			isFirst = false
			continue
		}

		strFmt := flexibleNanoUnmarshallingFmt

		prevTime, err := time.Parse(strFmt, sortedStrings[i-1])
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
		thisTime, err := time.Parse(strFmt, timeStr)
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
		if !thisTime.Before(prevTime) {
			t.Errorf("Expected '%s' to be before '%s'", thisTime, prevTime)
		}
	}
}

func Test_DynamocityTimeJsonRoundTrip(t *testing.T) {
	expectedMarshaledBytes := `{"TimeValue":"2020-01-01T14:00:00.000000000Z"}`
	type TestStruct struct {
		TimeValue Time
	}

	testCase := TestStruct{
		TimeValue: Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
	}

	actualBytes, err := json.Marshal(testCase)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	actualString := string(actualBytes)
	if actualString != expectedMarshaledBytes {
		t.Errorf("Unexpected unmarshal/marshal round trip. Got '%v', want '%v'", actualString, expectedMarshaledBytes)
	}

	var unmarshalled TestStruct

	err = json.Unmarshal(actualBytes, &unmarshalled)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if !unmarshalled.TimeValue.Time().Equal(testCase.TimeValue.Time()) {
		t.Errorf("Unexpected unmarshalled time. Got '%v', want '%v'", unmarshalled.TimeValue, unmarshalled.TimeValue)
	}
}

func Test_BetweenStartInc(t *testing.T) {
	cases := []struct {
		name       string
		startRange Time
		endRange   Time
		test       Time
		expected   bool
	}{
		{
			name:       "Given a testValue that is equal to the start range, then return true",
			startRange: Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			test:       Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			expected:   true,
		},
		{
			name:       "Given a testValue that is equal to the end range, then return false",
			startRange: Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			test:       Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			expected:   false,
		},
		{
			name:       "Given a testValue that is equal to the end range, when evaluating nano precision, then return true",
			startRange: Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   Time(time.Date(2020, time.January, 1, 14, 0, 0, 10, time.UTC)),
			test:       Time(time.Date(2020, time.January, 1, 14, 0, 0, 9, time.UTC)),
			expected:   true,
		},
	}

	for _, tc := range cases {
		if tc.test.BetweenStartInc(tc.startRange, tc.endRange) != tc.expected {
			t.Errorf("Given %s (inclusive) and %s, expected %s range check to equal %v", tc.startRange, tc.endRange, tc.test, tc.expected)
		}
	}
}

func Test_BetweenEndInc(t *testing.T) {
	cases := []struct {
		name       string
		startRange Time
		endRange   Time
		test       Time
		expected   bool
	}{
		{
			name:       "Given a testValue that is equal to the end range, then return true",
			startRange: Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			test:       Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			expected:   true,
		},
		{
			name:       "Given a testValue that is equal to the start range, then return false",
			startRange: Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			test:       Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			expected:   false,
		},
		{
			name:       "Given a testValue that is equal to the end range, when evaluating nano precision, then return true",
			startRange: Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   Time(time.Date(2020, time.January, 1, 14, 0, 0, 10, time.UTC)),
			test:       Time(time.Date(2020, time.January, 1, 14, 0, 0, 10, time.UTC)),
			expected:   true,
		},
	}

	for _, tc := range cases {
		if tc.test.BetweenEndInc(tc.startRange, tc.endRange) != tc.expected {
			t.Errorf("Given %s and %s (inclusive), expected %s range check to equal %v", tc.startRange, tc.endRange, tc.test, tc.expected)
		}
	}
}

func Test_BetweenExclusive(t *testing.T) {
	cases := []struct {
		name       string
		startRange Time
		endRange   Time
		test       Time
		expected   bool
	}{
		{
			name:       "Given a testValue that is in end range (exclusive), then return true",
			startRange: Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   Time(time.Date(2020, time.January, 1, 14, 0, 0, 2, time.UTC)),
			test:       Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			expected:   true,
		},
		{
			name:       "Given a testValue that is equal to the start range, then return false",
			startRange: Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			test:       Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			expected:   false,
		},
		{
			name:       "Given a testValue that is equal to the end range, then return false",
			startRange: Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			test:       Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			expected:   false,
		},
	}

	for _, tc := range cases {
		if tc.test.BetweenExclusive(tc.startRange, tc.endRange) != tc.expected {
			t.Errorf("Given %s and %s, expected %s range check to equal %v", tc.startRange, tc.endRange, tc.test, tc.expected)
		}
	}
}

func Test_BetweenInclusive(t *testing.T) {
	cases := []struct {
		name       string
		startRange Time
		endRange   Time
		test       Time
		expected   bool
	}{
		{
			name:       "Given a testValue that equals the end range, then return true",
			startRange: Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   Time(time.Date(2020, time.January, 1, 14, 0, 0, 2, time.UTC)),
			test:       Time(time.Date(2020, time.January, 1, 14, 0, 0, 2, time.UTC)),
			expected:   true,
		},
		{
			name:       "Given a testValue that equals the start range, then return true",
			startRange: Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   Time(time.Date(2020, time.January, 1, 14, 0, 0, 2, time.UTC)),
			test:       Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			expected:   true,
		},
		{
			name:       "Given a testValue that is between the range values, then return true",
			startRange: Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   Time(time.Date(2020, time.January, 1, 14, 0, 0, 2, time.UTC)),
			test:       Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			expected:   true,
		},
	}

	for _, tc := range cases {
		if tc.test.BetweenInclusive(tc.startRange, tc.endRange) != tc.expected {
			t.Errorf("Given %s (inclusive) and %s (inclusive), expected %s range check to equal %v", tc.startRange, tc.endRange, tc.test, tc.expected)
		}
	}
}
