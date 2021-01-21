package dynamocity_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go/aws"
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

func decodeAttributeValue(av types.AttributeValue, t *testing.T) string {
	tv, ok := av.(*types.AttributeValueMemberS)
	if !ok {
		t.Errorf("Unexpected Attribute Value Member Type %T", av)
		t.FailNow()
	}
	return tv.Value
}
func Test_DynamocityTime(t *testing.T) {
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	cases := []testutils.SortKeyTestCase{
		{
			Name:       "Given a RFC3339 Timestamp, when using a sortkey with dynamocity.NanoTime, then apply sort key greaterThanEqual based on nanosecond precision",
			Timestamp:  "2019-12-09T06:50:02.53323Z",
			SortKey:    "nanoTime",
			IndexName:  "nano-time-index",
			KeyBuilder: testutils.NanoTimeKeyBuilder,
			Verify: func(allItems []map[string]types.AttributeValue, tc testutils.SortKeyTestCase, t *testing.T) {
				actualItems := make([]testutils.TestDynamoItem, len(allItems))
				if err = attributevalue.UnmarshalListOfMaps(allItems, &actualItems); err != nil {
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
					if !actual.NanoTime.Time().Equal(expectedItem.NanoTime.Time()) {
						t.Errorf("Unexpected item time at index %d. Expected '%s', Got '%s'", i, expectedItem.NanoTime, actual.NanoTime)
					}
				}
			},
		},
		{
			Name:       "Given a RFC3339Nano Timestamp, when using a default RFC3339Nano timestamp, then apply sort key greaterThanEqual based on non-precise nano precision",
			Timestamp:  "2019-12-09T06:50:02.53323Z",
			SortKey:    "goTime",
			IndexName:  "go-time-index",
			KeyBuilder: testutils.GoTimeKeyBuilder,
			Verify: func(allItems []map[string]types.AttributeValue, tc testutils.SortKeyTestCase, t *testing.T) {
				actualItems := make([]testutils.TestDynamoItem, len(allItems))
				if err = attributevalue.UnmarshalListOfMaps(allItems, &actualItems); err != nil {
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
					if actual.GoTime.After(previousElement.GoTime) {
						t.Errorf("Item GoTime is unexpectedly after previous item GoTime. This '%s', Previous '%s'", actual.GoTime, previousElement.GoTime)
					}
				}
			},
		},
		{
			Name:       "Given a RFC3339Nano Timestamp, then verify the string attribute value has truncated nanos",
			Timestamp:  "2018-12-31T00:00:00Z",
			SortKey:    "goTime",
			IndexName:  "go-time-index",
			KeyBuilder: testutils.GoTimeKeyBuilder,
			Verify: func(allItems []map[string]types.AttributeValue, tc testutils.SortKeyTestCase, t *testing.T) {

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
					avString := decodeAttributeValue(actual[tc.SortKey], t)
					expectedItem := expectedItemTimestampsInStringOrder[i]
					if avString != expectedItem {
						t.Errorf("Unexpected string attribute value %d. Expected '%s', Got '%s'", i, avString, expectedItem)
					}
				}
			},
		},
		{
			Name:       "Given a dynamocity.NanoTime Timestamp, then verify the string attribute value has retained nanos",
			Timestamp:  "2018-12-31T00:00:00Z",
			SortKey:    "nanoTime",
			IndexName:  "nano-time-index",
			KeyBuilder: testutils.NanoTimeKeyBuilder,
			Verify: func(allItems []map[string]types.AttributeValue, tc testutils.SortKeyTestCase, t *testing.T) {

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
					avString := decodeAttributeValue(actual[tc.SortKey], t)
					expectedItem := expectedItemTimestampsInStringOrder[i]
					if avString != expectedItem {
						t.Errorf("Unexpected string attribute value %d. Expected '%s', Got '%s'", i, avString, expectedItem)
					}
				}
			},
		},
		{
			Name:       "Given a dynamocity.MillisTime Timestamp, then verify the string attribute value has retained millseconds",
			Timestamp:  "2018-12-31T00:00:00Z",
			SortKey:    "millisTime",
			IndexName:  "millis-time-index",
			KeyBuilder: testutils.MillisTimeKeyBuilder,
			Verify: func(allItems []map[string]types.AttributeValue, tc testutils.SortKeyTestCase, t *testing.T) {

				expectedItemTimestampsInStringOrder := []string{
					"2019-12-09T06:50:02.000Z",
					"2019-12-09T06:50:02.500Z",
					"2019-12-09T06:50:02.530Z",
					"2019-12-09T06:50:02.533Z",
					"2019-12-09T06:50:02.533Z",
					"2019-12-09T06:50:02.533Z",
					"2019-12-09T06:50:02.533Z",
					"2019-12-09T06:50:02.533Z",
					"2019-12-09T06:50:02.533Z",
					"2019-12-09T06:50:02.533Z",
				}

				if len(allItems) != 10 {
					t.Errorf("Unexpected number of items. Expected '%d', Got '%d'", 10, len(allItems))
				}

				for i, actual := range allItems {
					avString := decodeAttributeValue(actual[tc.SortKey], t)
					expectedItem := expectedItemTimestampsInStringOrder[i]
					if avString != expectedItem {
						t.Errorf("Unexpected string attribute value %d. Expected '%s', Got '%s'", i, avString, expectedItem)
					}
				}
			},
		},
		{
			Name:       "Given a Timestamp, when using a sort key with dynamocity.MillisTime, then apply sort key greaterThanEqual based on millsecond precision",
			Timestamp:  "2019-12-09T06:50:02.533Z",
			SortKey:    "millisTime",
			IndexName:  "millis-time-index",
			KeyBuilder: testutils.MillisTimeKeyBuilder,
			Verify: func(allItems []map[string]types.AttributeValue, tc testutils.SortKeyTestCase, t *testing.T) {

				expectedItems := map[string]string{
					"d5ba7130-3c9d-43e9-8596-8ce372d5ebe5": "2019-12-09T06:50:02.533Z",
					"92edbce8-7271-44fe-9e7b-83adabf406cc": "2019-12-09T06:50:02.533Z",
					"9e8a5d44-8a14-4594-b677-85f8e9f22670": "2019-12-09T06:50:02.533Z",
					"883dc7f6-384b-4d17-8bcf-4bf1a310d582": "2019-12-09T06:50:02.533Z",
					"7bb99219-46d6-4ba5-8a40-2adc80e58dd0": "2019-12-09T06:50:02.533Z",
					"2e53bcda-9451-4da3-a1b4-afd165479766": "2019-12-09T06:50:02.533Z",
					"7721ad03-bcca-4e4c-91dc-97c30d0e85ee": "2019-12-09T06:50:02.533Z",
				}

				if len(allItems) != len(expectedItems) {
					t.Errorf("Unexpected number of items. Expected '%d', Got '%d'", 7, len(allItems))
				}

				for i, actual := range allItems {
					itemID := decodeAttributeValue(actual["sk"], t)
					millisTimestampString := decodeAttributeValue(actual[tc.SortKey], t)
					expectedItemTimestamp := expectedItems[itemID]
					if millisTimestampString != expectedItemTimestamp {
						t.Errorf("Unexpected string attribute value %d. Expected '%s', Got '%s'", i, millisTimestampString, expectedItemTimestamp)
					}
				}
			},
		},
		{
			Name:       "Given a dynamocity.SecondsTime Timestamp, then verify the string attribute value has marshaled with seconds precision",
			Timestamp:  "2018-12-31T00:00:00Z",
			SortKey:    "secondsTime",
			IndexName:  "seconds-time-index",
			KeyBuilder: testutils.SecondsTimeKeyBuilder,
			Verify: func(allItems []map[string]types.AttributeValue, tc testutils.SortKeyTestCase, t *testing.T) {

				expectedItemTimestampsInStringOrder := []string{
					"2019-12-09T06:50:02Z",
					"2019-12-09T06:50:02Z",
					"2019-12-09T06:50:02Z",
					"2019-12-09T06:50:02Z",
					"2019-12-09T06:50:02Z",
					"2019-12-09T06:50:02Z",
					"2019-12-09T06:50:02Z",
					"2019-12-09T06:50:02Z",
					"2019-12-09T06:50:02Z",
					"2019-12-09T06:50:02Z",
				}

				if len(allItems) != 10 {
					t.Errorf("Unexpected number of items. Expected '%d', Got '%d'", 10, len(allItems))
				}

				for i, actual := range allItems {
					avString := decodeAttributeValue(actual[tc.SortKey], t)
					expectedItem := expectedItemTimestampsInStringOrder[i]
					if avString != expectedItem {
						t.Errorf("Unexpected string attribute value %d. Expected '%s', Got '%s'", i, avString, expectedItem)
					}
				}
			},
		},
		{
			Name:       "Given a Timestamp, when using a sort key with dynamocity.SecondsTime, then apply sort key greaterThanEqual based on seconds precision",
			Timestamp:  "2019-12-09T06:50:02Z",
			SortKey:    "secondsTime",
			IndexName:  "seconds-time-index",
			KeyBuilder: testutils.SecondsTimeKeyBuilder,
			Verify: func(allItems []map[string]types.AttributeValue, tc testutils.SortKeyTestCase, t *testing.T) {

				expectedItems := map[string]string{
					"72fdbec6-63aa-489c-a126-e928bb6210b3": "2019-12-09T06:50:02Z",
					"2ffb86c0-9b5e-47c5-b4e4-0f222e4c2990": "2019-12-09T06:50:02Z",
					"6cbf5f88-bf3e-4705-8923-985e048d355c": "2019-12-09T06:50:02Z",
					"7bb99219-46d6-4ba5-8a40-2adc80e58dd0": "2019-12-09T06:50:02Z",
					"d5ba7130-3c9d-43e9-8596-8ce372d5ebe5": "2019-12-09T06:50:02Z",
					"92edbce8-7271-44fe-9e7b-83adabf406cc": "2019-12-09T06:50:02Z",
					"883dc7f6-384b-4d17-8bcf-4bf1a310d582": "2019-12-09T06:50:02Z",
					"9e8a5d44-8a14-4594-b677-85f8e9f22670": "2019-12-09T06:50:02Z",
					"2e53bcda-9451-4da3-a1b4-afd165479766": "2019-12-09T06:50:02Z",
					"7721ad03-bcca-4e4c-91dc-97c30d0e85ee": "2019-12-09T06:50:02Z",
				}

				if len(allItems) != len(expectedItems) {
					t.Errorf("Unexpected number of items. Expected '%d', Got '%d'", len(expectedItems), len(allItems))
				}

				for i, actual := range allItems {
					itemID := decodeAttributeValue(actual["sk"], t)
					millisTimestampString := decodeAttributeValue(actual[tc.SortKey], t)
					expectedItemTimestamp := expectedItems[itemID]
					if millisTimestampString != expectedItemTimestamp {
						t.Errorf("Unexpected string attribute value %d. Expected '%s', Got '%s'", i, millisTimestampString, expectedItemTimestamp)
					}
				}
			},
		},
	}

	for _, tc := range cases {

		keyCondition := expression.KeyAnd(
			expression.Key("pk").Equal(expression.Value("TEST")),
			expression.Key(tc.SortKey).GreaterThanEqual(expression.Value(tc.KeyBuilder(tc, t))),
		)

		expr, err := expression.NewBuilder().
			WithKeyCondition(keyCondition).
			Build()

		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		allItems := []map[string]types.AttributeValue{}
		input := &dynamodb.QueryInput{
			TableName:                 tableName,
			IndexName:                 aws.String(tc.IndexName),
			KeyConditionExpression:    expr.KeyCondition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
		}

		qryContext := context.TODO()
		paginator := dynamodb.NewQueryPaginator(db, input)
		for paginator.HasMorePages() {
			out, err := paginator.NextPage(qryContext)
			if err != nil {
				t.Error(err)
				t.FailNow()
			}
			allItems = append(allItems, out.Items...)
		}

		queryResultItems := make([]testutils.TestDynamoItem, len(allItems))
		if err = attributevalue.UnmarshalListOfMaps(allItems, &queryResultItems); err != nil {
			t.Error(err)
			t.FailNow()
		}
		tc.Verify(allItems, tc, t)
	}
}

func Test_FlexibleNanoFmtUnMarshalling(t *testing.T) {
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

		strFmt := dynamocity.FlexibleNanoFmt

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

func Test_JSONRoundTrip(t *testing.T) {
	type TestType struct {
		MillisTime  dynamocity.MillisTime  `json:"millisTime,omitempty"`
		NanoTime    dynamocity.NanoTime    `json:"nanoTime,omitempty"`
		SecondsTime dynamocity.SecondsTime `json:"secondsTime,omitempty"`
	}
	cases := []struct {
		name                   string
		expectedMarshaledBytes []byte
		testCase               TestType
	}{
		{
			name:                   "Given expected times, then marshal and unmarshal JSON correctly",
			expectedMarshaledBytes: []byte(`{"millisTime":"2020-01-01T14:00:00.100Z","nanoTime":"2020-01-01T14:00:00.999000000Z","secondsTime":"2020-01-01T14:00:00Z"}`),
			testCase: TestType{
				MillisTime:  dynamocity.MillisTime(time.Date(2020, time.January, 1, 14, 0, 0, 100000000, time.UTC)),
				NanoTime:    dynamocity.NanoTime(time.Date(2020, time.January, 1, 14, 0, 0, 999000000, time.UTC)),
				SecondsTime: dynamocity.SecondsTime(time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC)),
			},
		},
	}

	for _, tc := range cases {
		actualBytes, err := json.Marshal(tc.testCase)
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		actualString := string(actualBytes)
		if actualString != string(tc.expectedMarshaledBytes) {
			t.Errorf("Unexpected unmarshal/marshal round trip. Got '%v', want '%v'", actualString, string(tc.expectedMarshaledBytes))
		}
		var unmarshalled TestType

		err = json.Unmarshal(actualBytes, &unmarshalled)
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		if !unmarshalled.MillisTime.Time().Equal(tc.testCase.MillisTime.Time()) {
			t.Errorf("Unexpected unmarshalled Millis time. Got '%v', want '%v'", unmarshalled.MillisTime, unmarshalled.MillisTime)
		}
		if !unmarshalled.NanoTime.Time().Equal(tc.testCase.NanoTime.Time()) {
			t.Errorf("Unexpected unmarshalled time. Got '%v', want '%v'", unmarshalled.NanoTime, unmarshalled.NanoTime)
		}
		if !unmarshalled.SecondsTime.Time().Equal(tc.testCase.SecondsTime.Time()) {
			t.Errorf("Unexpected unmarshalled time. Got '%v', want '%v'", unmarshalled.SecondsTime, unmarshalled.SecondsTime)
		}
	}
}

func Test_BetweenStartInc(t *testing.T) {
	cases := []struct {
		name       string
		startRange time.Time
		endRange   time.Time
		test       time.Time
		expected   bool
	}{
		{
			name:       "Given a testValue that is equal to the start range, then return true",
			startRange: time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC),
			endRange:   time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC),
			test:       time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC),
			expected:   true,
		},
		{
			name:       "Given a testValue that is equal to the end range, then return false",
			startRange: time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC),
			endRange:   time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC),
			test:       time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC),
			expected:   false,
		},
		{
			name:       "Given a testValue that is equal to the end range, when evaluating nano precision, then return true",
			startRange: time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC),
			endRange:   time.Date(2020, time.January, 1, 14, 0, 0, 10, time.UTC),
			test:       time.Date(2020, time.January, 1, 14, 0, 0, 9, time.UTC),
			expected:   true,
		},
	}

	for _, tc := range cases {
		if dynamocity.BetweenStartInc(tc.test, tc.startRange, tc.endRange) != tc.expected {
			t.Errorf("Given %s (inclusive) and %s, expected %s range check to equal %v", tc.startRange, tc.endRange, tc.test, tc.expected)
		}
	}
}

func Test_BetweenEndInc(t *testing.T) {
	cases := []struct {
		name       string
		startRange time.Time
		endRange   time.Time
		test       time.Time
		expected   bool
	}{
		{
			name:       "Given a testValue that is equal to the end range, then return true",
			startRange: time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC),
			endRange:   time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC),
			test:       time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC),
			expected:   true,
		},
		{
			name:       "Given a testValue that is equal to the start range, then return false",
			startRange: time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC),
			endRange:   time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC),
			test:       time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC),
			expected:   false,
		},
		{
			name:       "Given a testValue that is equal to the end range, when evaluating nano precision, then return true",
			startRange: time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC),
			endRange:   time.Date(2020, time.January, 1, 14, 0, 0, 10, time.UTC),
			test:       time.Date(2020, time.January, 1, 14, 0, 0, 10, time.UTC),
			expected:   true,
		},
	}

	for _, tc := range cases {
		if dynamocity.BetweenEndInc(tc.test, tc.startRange, tc.endRange) != tc.expected {
			t.Errorf("Given %s and %s (inclusive), expected %s range check to equal %v", tc.startRange, tc.endRange, tc.test, tc.expected)
		}
	}
}

func Test_BetweenExclusive(t *testing.T) {
	cases := []struct {
		name       string
		startRange time.Time
		endRange   time.Time
		test       time.Time
		expected   bool
	}{
		{
			name:       "Given a testValue that is in end range (exclusive), then return true",
			startRange: time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC),
			endRange:   time.Date(2020, time.January, 1, 14, 0, 0, 2, time.UTC),
			test:       time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC),
			expected:   true,
		},
		{
			name:       "Given a testValue that is equal to the start range, then return false",
			startRange: time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC),
			endRange:   time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC),
			test:       time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC),
			expected:   false,
		},
		{
			name:       "Given a testValue that is equal to the end range, then return false",
			startRange: time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC),
			endRange:   time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC),
			test:       time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC),
			expected:   false,
		},
	}

	for _, tc := range cases {
		if dynamocity.BetweenExclusive(tc.test, tc.startRange, tc.endRange) != tc.expected {
			t.Errorf("Given %s and %s, expected %s range check to equal %v", tc.startRange, tc.endRange, tc.test, tc.expected)
		}
	}
}

func Test_BetweenInclusive(t *testing.T) {
	cases := []struct {
		name       string
		startRange time.Time
		endRange   time.Time
		test       time.Time
		expected   bool
	}{
		{
			name:       "Given a testValue that equals the end range, then return true",
			startRange: time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC),
			endRange:   time.Date(2020, time.January, 1, 14, 0, 0, 2, time.UTC),
			test:       time.Date(2020, time.January, 1, 14, 0, 0, 2, time.UTC),
			expected:   true,
		},
		{
			name:       "Given a testValue that equals the start range, then return true",
			startRange: time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC),
			endRange:   time.Date(2020, time.January, 1, 14, 0, 0, 2, time.UTC),
			test:       time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC),
			expected:   true,
		},
		{
			name:       "Given a testValue that is between the range values, then return true",
			startRange: time.Date(2020, time.January, 1, 14, 0, 0, 0, time.UTC),
			endRange:   time.Date(2020, time.January, 1, 14, 0, 0, 2, time.UTC),
			test:       time.Date(2020, time.January, 1, 14, 0, 0, 1, time.UTC),
			expected:   true,
		},
	}

	for _, tc := range cases {
		if dynamocity.BetweenInclusive(tc.test, tc.startRange, tc.endRange) != tc.expected {
			t.Errorf("Given %s (inclusive) and %s (inclusive), expected %s range check to equal %v", tc.startRange, tc.endRange, tc.test, tc.expected)
		}
	}
}
