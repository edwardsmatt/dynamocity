package testutils

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/edwardsmatt/dynamocity"
)

const dynamoEndpoint = "http://localhost:8000"

type TestDynamoItem struct {
	PartitionKey string                 `dynamodbav:"pk"`
	SortKey      string                 `dynamodbav:"sk"`
	GoTime       time.Time              `dynamodbav:"goTime"`
	NanoTime     dynamocity.NanoTime    `dynamodbav:"nanoTime"`
	MillisTime   dynamocity.MillisTime  `dynamodbav:"millisTime"`
	SecondsTime  dynamocity.SecondsTime `dynamodbav:"secondsTime"`
	StringTime   string                 `dynamodbav:"timestamp"`
}

type SortKeyTestCase struct {
	Name       string
	Timestamp  string
	SortKey    string
	IndexName  string
	KeyBuilder func(SortKeyTestCase, *testing.T) interface{}
	Verify     func([]map[string]types.AttributeValue, SortKeyTestCase, *testing.T)
}

var GoTimeKeyBuilder = func(tc SortKeyTestCase, t *testing.T) interface{} {
	timestamp, err := time.Parse(time.RFC3339Nano, tc.Timestamp)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	return timestamp
}

var NanoTimeKeyBuilder = func(tc SortKeyTestCase, t *testing.T) interface{} {
	timestamp, err := time.Parse(time.RFC3339Nano, tc.Timestamp)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	return dynamocity.NanoTime(timestamp)
}

var MillisTimeKeyBuilder = func(tc SortKeyTestCase, t *testing.T) interface{} {
	timestamp, err := time.Parse(time.RFC3339Nano, tc.Timestamp)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	return dynamocity.MillisTime(timestamp)
}

var SecondsTimeKeyBuilder = func(tc SortKeyTestCase, t *testing.T) interface{} {
	timestamp, err := time.Parse(time.RFC3339Nano, tc.Timestamp)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	return dynamocity.SecondsTime(timestamp)
}

func DynamoDB() (*dynamodb.Client, error) {
	overrides := make(map[string]string)
	overrides[dynamodb.ServiceID] = dynamoEndpoint
	customResolver := dynamocity.MakeEndpointResolver(overrides)
	awsConfig, err := config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolver(customResolver),
	)
	if err != nil {
		return nil, err
	}

	db := dynamodb.NewFromConfig(awsConfig)

	return db, nil
}

func MakeNewTable(db *dynamodb.Client, tableName string, attrs Attributes, keys Keys, gsis GlobalSecondaryIndexes, lsis LocalSecondaryIndexes) error {
	r := dynamodb.ListTablesInput{}
	resp1, err := db.ListTables(context.TODO(), &r)
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
		BillingMode:          types.BillingModePayPerRequest,
	}

	if len(gsis) > 0 {
		cti.GlobalSecondaryIndexes = gsis
	}

	if len(lsis) > 0 {
		cti.LocalSecondaryIndexes = lsis
	}

	_, err = db.CreateTable(context.TODO(), &cti)
	if err != nil {
		return err
	}
	return nil
}

func MakeTestTable(db *dynamodb.Client) (*string, error) {
	newTable := "test_table"
	pk := MakeAttribute("pk", types.ScalarAttributeTypeS)
	sk := MakeAttribute("sk", types.ScalarAttributeTypeS)
	nanoTime := MakeAttribute("nanoTime", types.ScalarAttributeTypeS)
	goTime := MakeAttribute("goTime", types.ScalarAttributeTypeS)
	millisTime := MakeAttribute("millisTime", types.ScalarAttributeTypeS)
	secondsTime := MakeAttribute("secondsTime", types.ScalarAttributeTypeS)

	attrs := []types.AttributeDefinition{
		pk.AttributeDefinition(),
		sk.AttributeDefinition(),
		nanoTime.AttributeDefinition(),
		goTime.AttributeDefinition(),
		millisTime.AttributeDefinition(),
		secondsTime.AttributeDefinition(),
	}

	keys := []types.KeySchemaElement{
		pk.KeyElement(types.KeyTypeHash),
		sk.KeyElement(types.KeyTypeRange),
	}

	lsis := []types.LocalSecondaryIndex{
		LSI("go-time-index", *pk, *goTime, types.ProjectionTypeAll, nil),
	}

	defaultThroughput := &types.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(1), WriteCapacityUnits: aws.Int64(1)}

	gsis := GlobalSecondaryIndexes{
		GSI("nano-time-index", *pk, *nanoTime, types.ProjectionTypeAll, defaultThroughput, nil),
		GSI("millis-time-index", *pk, *millisTime, types.ProjectionTypeAll, defaultThroughput, nil),
		GSI("seconds-time-index", *pk, *secondsTime, types.ProjectionTypeAll, defaultThroughput, nil),
	}

	if err := MakeNewTable(db, newTable, attrs, keys, gsis, lsis); err != nil {
		return nil, err
	}
	return aws.String(newTable), nil
}

func SetupTestFixtures() (*dynamodb.Client, *string, []TestDynamoItem, error) {
	db, err := DynamoDB()
	if err != nil {
		return nil, nil, nil, err
	}
	tableName, err := MakeTestTable(db)
	if err != nil {
		return nil, nil, nil, err
	}

	items := []TestDynamoItem{
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
		goTime, err := time.Parse(time.RFC3339Nano, item.StringTime)
		if err != nil {
			return nil, nil, nil, err
		}
		item.GoTime = goTime
		item.NanoTime = dynamocity.NanoTime(goTime)
		item.MillisTime = dynamocity.MillisTime(goTime)
		item.SecondsTime = dynamocity.SecondsTime(goTime)

		if _, err := PutItem(db, *tableName, item); err != nil {
			return nil, nil, nil, err
		}
	}
	return db, tableName, items, nil
}
