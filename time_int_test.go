package dynamocity_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/endpoints"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/expression"
	"github.com/edwardsmatt/dynamocity"
)

var db *dynamodb.Client
var tableName *string
var itemsSortedOrder []testDynamoItem
var err error

func init() {
	/* load test data */
	db, tableName, itemsSortedOrder, err = setupTestFixtures()
}

type sortKeyTestCase struct {
	name       string
	timestamp  string
	lsiSortKey string
	lsiKeyName string
	keyBuilder func(sortKeyTestCase, *testing.T) interface{}
	verify     func([]testDynamoItem, sortKeyTestCase, *testing.T)
}

func Test_DynamocityTime(t *testing.T) {
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	cases := []sortKeyTestCase{
		{
			name:       "Given a RFC3339 Timestamp, when using a sortkey with dynamocity time, then apply sort key greater Than filters correctly",
			timestamp:  "2019-12-09T06:50:02.53323Z",
			lsiSortKey: "dynamocityTime",
			lsiKeyName: "dynamocity-time-index",
			keyBuilder: func(tc sortKeyTestCase, t *testing.T) interface{} {
				timestamp, err := time.Parse(time.RFC3339Nano, tc.timestamp)
				if err != nil {
					t.Error(err)
					t.FailNow()
				}
				return dynamocity.Time(timestamp)
			},
			verify: func(actualItems []testDynamoItem, tc sortKeyTestCase, t *testing.T) {
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
			name:       "Given a RFC3339Nano Timestamp, when using a default RFC3339Nano timestamp, then sort key filters based on string value",
			timestamp:  "2019-12-09T06:50:02.53323Z",
			lsiSortKey: "nanoTime",
			lsiKeyName: "nano-time-index",
			keyBuilder: func(tc sortKeyTestCase, t *testing.T) interface{} {
				timestamp, err := time.Parse(time.RFC3339Nano, tc.timestamp)
				if err != nil {
					t.Error(err)
					t.FailNow()
				}
				return timestamp
			},
			verify: func(actualItems []testDynamoItem, tc sortKeyTestCase, t *testing.T) {
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
	}

	for _, tc := range cases {

		lsiKeyCondition := expression.KeyAnd(
			expression.Key("pk").Equal(expression.Value("TEST")),
			expression.Key(tc.lsiSortKey).GreaterThanEqual(expression.Value(tc.keyBuilder(tc, t))),
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
			IndexName:                 aws.String(tc.lsiKeyName),
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

		queryResultItems := make([]testDynamoItem, len(allItems))
		if err = dynamodbattribute.UnmarshalListOfMaps(allItems, &queryResultItems); err != nil {
			t.Error(err)
			t.FailNow()
		}
		tc.verify(queryResultItems, tc, t)
	}

}

const (
	dynamoEndpoint = "http://localhost:8000"
)

type testDynamoItem struct {
	PartitionKey   string          `dynamodbav:"pk"`
	SortKey        string          `dynamodbav:"sk"`
	NanoTime       time.Time       `dynamodbav:"nanoTime"`
	DynamocityTime dynamocity.Time `dynamodbav:"dynamocityTime"`
	StringTime     string          `dynamodbav:"timestamp"`
}

func dynamoDB() (*dynamodb.Client, error) {
	awsConfig, err := external.LoadDefaultAWSConfig()
	if err != nil {
		return nil, err
	}

	overrides := make(map[string]string)
	overrides[dynamodb.EndpointsID] = dynamoEndpoint

	awsConfig.Region = endpoints.ApSoutheast2RegionID
	awsConfig.EndpointResolver = dynamocity.MakeEndpointResolver(overrides)

	db := dynamodb.New(awsConfig)

	return db, nil
}

func makeNewTable(db *dynamodb.Client, tableName string, attrs dynamocity.Attributes, keys dynamocity.Keys, gsis dynamocity.GlobalSecondaryIndexes, lsis dynamocity.LocalSecondaryIndexes) error {
	r := dynamodb.ListTablesInput{}
	ltr := db.ListTablesRequest(&r)
	resp1, err := ltr.Send(ltr.Context())
	if err != nil {
		return err
	}

	for _, tn := range resp1.TableNames {
		if tn == tableName {
			return nil
		}
	}

	cti := dynamodb.CreateTableInput{
		TableName:            aws.String(tableName),
		AttributeDefinitions: attrs,
		KeySchema:            keys,
		BillingMode:          dynamodb.BillingModePayPerRequest,
	}

	if len(gsis) > 0 {
		cti.GlobalSecondaryIndexes = gsis
	}

	if len(lsis) > 0 {
		cti.LocalSecondaryIndexes = lsis
	}

	ctr := db.CreateTableRequest(&cti)
	_, err = ctr.Send(ctr.Context())
	if err != nil {
		return err
	}
	return nil
}

func testTable(db *dynamodb.Client) *string {
	newTable := "test_table"
	pk := dynamocity.MakeAttribute("pk", dynamodb.ScalarAttributeTypeS)
	sk := dynamocity.MakeAttribute("sk", dynamodb.ScalarAttributeTypeS)
	dynamocityTime := dynamocity.MakeAttribute("dynamocityTime", dynamodb.ScalarAttributeTypeS)
	nanoTime := dynamocity.MakeAttribute("nanoTime", dynamodb.ScalarAttributeTypeS)

	attrs := []dynamodb.AttributeDefinition{
		pk.AttributeDefinition(), sk.AttributeDefinition(), dynamocityTime.AttributeDefinition(), nanoTime.AttributeDefinition(),
	}

	keys := []dynamodb.KeySchemaElement{
		pk.KeyElement(dynamodb.KeyTypeHash),
		sk.KeyElement(dynamodb.KeyTypeRange),
	}

	lsis := []dynamodb.LocalSecondaryIndex{
		dynamocity.LSI("dynamocity-time-index", *pk, *dynamocityTime, dynamodb.ProjectionTypeAll, nil),
		dynamocity.LSI("nano-time-index", *pk, *nanoTime, dynamodb.ProjectionTypeAll, nil),
	}

	gsis := dynamocity.GlobalSecondaryIndexes{}

	makeNewTable(db, newTable, attrs, keys, gsis, lsis)
	return aws.String(newTable)
}

func setupTestFixtures() (*dynamodb.Client, *string, []testDynamoItem, error) {
	db, err := dynamoDB()
	if err != nil {
		return nil, nil, nil, err
	}
	tableName := testTable(db)

	items := []testDynamoItem{
		{
			PartitionKey: "TEST",
			SortKey:      "72fdbec6-63aa-489c-a126-e928bb6210b3",
			StringTime:   "2019-12-09T06:50:02Z",
		},
		{
			PartitionKey: "TEST",
			SortKey:      "2ffb86c0-9b5e-47c5-b4e4-0f222e4c2990",
			StringTime:   "2019-12-09T06:50:02.5Z",
		},
		{
			PartitionKey: "TEST",
			SortKey:      "6cbf5f88-bf3e-4705-8923-985e048d355c",
			StringTime:   "2019-12-09T06:50:02.53Z",
		},
		{
			PartitionKey: "TEST",
			SortKey:      "7bb99219-46d6-4ba5-8a40-2adc80e58dd0",
			StringTime:   "2019-12-09T06:50:02.533Z",
		},
		{
			PartitionKey: "TEST",
			SortKey:      "d5ba7130-3c9d-43e9-8596-8ce372d5ebe5",
			StringTime:   "2019-12-09T06:50:02.5332Z",
		},
		{
			PartitionKey: "TEST",
			SortKey:      "92edbce8-7271-44fe-9e7b-83adabf406cc",
			StringTime:   "2019-12-09T06:50:02.53323Z",
		},
		{
			PartitionKey: "TEST",
			SortKey:      "883dc7f6-384b-4d17-8bcf-4bf1a310d582",
			StringTime:   "2019-12-09T06:50:02.533237Z",
		},
		{
			PartitionKey: "TEST",
			SortKey:      "9e8a5d44-8a14-4594-b677-85f8e9f22670",
			StringTime:   "2019-12-09T06:50:02.5332373Z",
		},
		{
			PartitionKey: "TEST",
			SortKey:      "2e53bcda-9451-4da3-a1b4-afd165479766",
			StringTime:   "2019-12-09T06:50:02.53323732Z",
		},
		{
			PartitionKey: "TEST",
			SortKey:      "7721ad03-bcca-4e4c-91dc-97c30d0e85ee",
			StringTime:   "2019-12-09T06:50:02.533237329Z",
		},
	}

	for i := 0; i < len(items); i++ {
		item := &items[i]
		nanoTime, err := time.Parse(time.RFC3339Nano, item.StringTime)
		if err != nil {
			return nil, nil, nil, err
		}
		item.NanoTime = nanoTime
		item.DynamocityTime = dynamocity.Time(nanoTime)

		if _, err := dynamocity.PutItem(db, *tableName, item); err != nil {
			return nil, nil, nil, err
		}
	}
	return db, tableName, items, nil
}
