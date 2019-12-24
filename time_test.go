package dynamocity_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/expression"
	"github.com/edwardsmatt/dynamocity"
	"github.com/edwardsmatt/dynamocity/internal/testutils"
)

var db *dynamodb.Client
var tableName *string
var itemsSortedOrder []testutils.TestDynamoItem
var err error

func init() {
	/* load test data */
	db, tableName, itemsSortedOrder, err = testutils.SetupTestFixtures()
}

func Test_DynamocityTime(t *testing.T) {
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	cases := []testutils.SortKeyTestCase{
		{
			Name:       "Given a RFC3339 Timestamp, when using a sortkey with dynamocity time, then apply sort key greaterThan based on nanotime precision",
			Timestamp:  "2019-12-09T06:50:02.53323Z",
			SortKey:    "dynamocityTime",
			KeyName:    "dynamocity-time-index",
			KeyBuilder: testutils.DynamocityTimeKeyBuilder,
			Verify: func(allItems []map[string]dynamodb.AttributeValue, tc testutils.SortKeyTestCase, t *testing.T) {
				actualItems := make([]testutils.TestDynamoItem, len(allItems))
				if err = dynamodbattribute.UnmarshalListOfMaps(allItems, &actualItems); err != nil {
					t.Error(err)
					t.FailNow()
				}

				lastFiveItems := itemsSortedOrder[5:]

				if len(actualItems) != len(lastFiveItems) {
					t.Errorf("Unexpected number of query items returned. Expected '%d', Got '%d'", len(lastFiveItems), len(actualItems))
				}

				for i, actual := range actualItems {
					expectedItem := lastFiveItems[i]
					if actual.SortKey != expectedItem.SortKey {
						t.Errorf("Unexpected item ID at index %d. Expected '%s', Got '%s'", i, expectedItem.SortKey, actual.SortKey)
					}
					if !actual.DynamocityTime.Time().Equal(expectedItem.DynamocityTime.Time()) {
						t.Errorf("Unexpected item time at index %d. Expected '%s', Got '%s'", i, expectedItem.DynamocityTime, actual.DynamocityTime)
					}
				}
			},
		},
		{
			Name:       "Given a RFC3339Nano Timestamp, when using a default RFC3339Nano timestamp, then apply sort key greaterThan based on non-precise nano precision",
			Timestamp:  "2019-12-09T06:50:02.53323Z",
			SortKey:    "nanoTime",
			KeyName:    "nano-time-index",
			KeyBuilder: testutils.NanoTimeKeyBuilder,
			Verify: func(allItems []map[string]dynamodb.AttributeValue, tc testutils.SortKeyTestCase, t *testing.T) {
				actualItems := make([]testutils.TestDynamoItem, len(allItems))
				if err = dynamodbattribute.UnmarshalListOfMaps(allItems, &actualItems); err != nil {
					t.Error(err)
					t.FailNow()
				}

				expectedItemTimestampsInStringOrder := []string{
					"2019-12-09T06:50:02.53323Z",
					"2019-12-09T06:50:02.5332Z",
					"2019-12-09T06:50:02.533Z",
					"2019-12-09T06:50:02.53Z",
					"2019-12-09T06:50:02.5Z",
					"2019-12-09T06:50:02Z",
				}

				if len(actualItems) != 6 {
					t.Errorf("Unexpected number of saved items. Expected '%d', Got '%d'", 6, len(actualItems))
				}
				isFirstElement := true
				for i, actual := range actualItems {
					if actual.StringTime != expectedItemTimestampsInStringOrder[i] {
						t.Errorf("Unexpected String sorting order. Expected '%s', Got '%s'", expectedItemTimestampsInStringOrder[i], actual.StringTime)
					}
					if isFirstElement {
						isFirstElement = false
						continue
					}
					previousElement := actualItems[i-1]
					if actual.NanoTime.After(previousElement.NanoTime) {
						t.Errorf("Item NanoTime is unexpectedly after previous item NanoTime. This '%s', Previous '%s'", actual.NanoTime, previousElement.NanoTime)
					}
				}
			},
		},
		{
			Name:       "Given a RFC3339Nano Timestamp, then verify the string attribute value has truncated nanos",
			Timestamp:  "2018-12-31T00:00:00Z",
			SortKey:    "nanoTime",
			KeyName:    "nano-time-index",
			KeyBuilder: testutils.NanoTimeKeyBuilder,
			Verify: func(allItems []map[string]dynamodb.AttributeValue, tc testutils.SortKeyTestCase, t *testing.T) {

				expectedItemTimestampsInStringOrder := []string{
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

				if len(allItems) != 10 {
					t.Errorf("Unexpected number of items. Expected '%d', Got '%d'", 10, len(allItems))
				}

				for i, actual := range allItems {
					avString := actual[tc.SortKey].S
					expectedItem := expectedItemTimestampsInStringOrder[i]
					if *avString != expectedItem {
						t.Errorf("Unexpected string attribute value %d. Expected '%s', Got '%s'", i, *avString, expectedItem)
					}
				}
			},
		},
		{
			Name:       "Given a dynamocity.Time Timestamp, then verify the string attribute value has retained nanos",
			Timestamp:  "2018-12-31T00:00:00Z",
			SortKey:    "dynamocityTime",
			KeyName:    "dynamocity-time-index",
			KeyBuilder: testutils.DynamocityTimeKeyBuilder,
			Verify: func(allItems []map[string]dynamodb.AttributeValue, tc testutils.SortKeyTestCase, t *testing.T) {

				expectedItemTimestampsInStringOrder := []string{
					"2019-12-09T06:50:02.000000000Z",
					"2019-12-09T06:50:02.500000000Z",
					"2019-12-09T06:50:02.530000000Z",
					"2019-12-09T06:50:02.533000000Z",
					"2019-12-09T06:50:02.533200000Z",
					"2019-12-09T06:50:02.533230000Z",
					"2019-12-09T06:50:02.533237000Z",
					"2019-12-09T06:50:02.533237300Z",
					"2019-12-09T06:50:02.533237320Z",
					"2019-12-09T06:50:02.533237329Z",
				}

				if len(allItems) != 10 {
					t.Errorf("Unexpected number of items. Expected '%d', Got '%d'", 10, len(allItems))
				}

				for i, actual := range allItems {
					avString := actual[tc.SortKey].S
					expectedItem := expectedItemTimestampsInStringOrder[i]
					if *avString != expectedItem {
						t.Errorf("Unexpected string attribute value %d. Expected '%s', Got '%s'", i, *avString, expectedItem)
					}
				}
			},
		},
	}

	for _, tc := range cases {

		lsiKeyCondition := expression.KeyAnd(
			expression.Key("pk").Equal(expression.Value("TEST")),
			expression.Key(tc.SortKey).GreaterThanEqual(expression.Value(tc.KeyBuilder(tc, t))),
		)

		expr, err := expression.NewBuilder().
			WithKeyCondition(lsiKeyCondition).
			Build()

		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		allItems := []map[string]dynamodb.AttributeValue{}
		input := &dynamodb.QueryInput{
			TableName:                 tableName,
			IndexName:                 aws.String(tc.KeyName),
			KeyConditionExpression:    expr.KeyCondition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
		}

		query := db.QueryRequest(input)
		paginator := dynamodb.NewQueryPaginator(query)
		for paginator.Next(query.Context()) {
			page := paginator.CurrentPage()
			allItems = append(allItems, page.Items...)
		}

		if err := paginator.Err(); err != nil {
			t.Error(err)
			t.FailNow()
		}

		queryResultItems := make([]testutils.TestDynamoItem, len(allItems))
		if err = dynamodbattribute.UnmarshalListOfMaps(allItems, &queryResultItems); err != nil {
			t.Error(err)
			t.FailNow()
		}
		tc.Verify(allItems, tc, t)
	}
}

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

		strFmt := dynamocity.FlexibleNanoUnmarshallingFmt

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
		TimeValue dynamocity.Time
	}

	testCase := TestStruct{
		TimeValue: dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
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
		startRange dynamocity.Time
		endRange   dynamocity.Time
		test       dynamocity.Time
		expected   bool
	}{
		{
			name:       "Given a testValue that is equal to the start range, then return true",
			startRange: dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			test:       dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			expected:   true,
		},
		{
			name:       "Given a testValue that is equal to the end range, then return false",
			startRange: dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			test:       dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			expected:   false,
		},
		{
			name:       "Given a testValue that is equal to the end range, when evaluating nano precision, then return true",
			startRange: dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 10, time.UTC)),
			test:       dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 9, time.UTC)),
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
		startRange dynamocity.Time
		endRange   dynamocity.Time
		test       dynamocity.Time
		expected   bool
	}{
		{
			name:       "Given a testValue that is equal to the end range, then return true",
			startRange: dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			test:       dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			expected:   true,
		},
		{
			name:       "Given a testValue that is equal to the start range, then return false",
			startRange: dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			test:       dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			expected:   false,
		},
		{
			name:       "Given a testValue that is equal to the end range, when evaluating nano precision, then return true",
			startRange: dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 10, time.UTC)),
			test:       dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 10, time.UTC)),
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
		startRange dynamocity.Time
		endRange   dynamocity.Time
		test       dynamocity.Time
		expected   bool
	}{
		{
			name:       "Given a testValue that is in end range (exclusive), then return true",
			startRange: dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 2, time.UTC)),
			test:       dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			expected:   true,
		},
		{
			name:       "Given a testValue that is equal to the start range, then return false",
			startRange: dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			test:       dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			expected:   false,
		},
		{
			name:       "Given a testValue that is equal to the end range, then return false",
			startRange: dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			test:       dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
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
		startRange dynamocity.Time
		endRange   dynamocity.Time
		test       dynamocity.Time
		expected   bool
	}{
		{
			name:       "Given a testValue that equals the end range, then return true",
			startRange: dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 2, time.UTC)),
			test:       dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 2, time.UTC)),
			expected:   true,
		},
		{
			name:       "Given a testValue that equals the start range, then return true",
			startRange: dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 2, time.UTC)),
			test:       dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			expected:   true,
		},
		{
			name:       "Given a testValue that is between the range values, then return true",
			startRange: dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			endRange:   dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 2, time.UTC)),
			test:       dynamocity.Time(time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC)),
			expected:   true,
		},
	}

	for _, tc := range cases {
		if tc.test.BetweenInclusive(tc.startRange, tc.endRange) != tc.expected {
			t.Errorf("Given %s (inclusive) and %s (inclusive), expected %s range check to equal %v", tc.startRange, tc.endRange, tc.test, tc.expected)
		}
	}
}
