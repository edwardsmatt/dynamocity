package testutils

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
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
	Verify     func([]map[string]dynamodb.AttributeValue, SortKeyTestCase, *testing.T)
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
	awsConfig, err := external.LoadDefaultAWSConfig()
	if err != nil {
		return nil, err
	}

	overrides := make(map[string]string)
	overrides[dynamodb.EndpointsID] = dynamoEndpoint

	awsConfig.Region = "ap-southeast-2"
	awsConfig.EndpointResolver = dynamocity.MakeEndpointResolver(overrides)

	db := dynamodb.New(awsConfig)

	return db, nil
}

func MakeNewTable(db *dynamodb.Client, tableName string, attrs Attributes, keys Keys, gsis GlobalSecondaryIndexes, lsis LocalSecondaryIndexes) error {
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

func MakeTestTable(db *dynamodb.Client) (*string, error) {
	newTable := "test_table"
	pk := MakeAttribute("pk", dynamodb.ScalarAttributeTypeS)
	sk := MakeAttribute("sk", dynamodb.ScalarAttributeTypeS)
	nanoTime := MakeAttribute("nanoTime", dynamodb.ScalarAttributeTypeS)
	goTime := MakeAttribute("goTime", dynamodb.ScalarAttributeTypeS)
	millisTime := MakeAttribute("millisTime", dynamodb.ScalarAttributeTypeS)
	secondsTime := MakeAttribute("secondsTime", dynamodb.ScalarAttributeTypeS)

	attrs := []dynamodb.AttributeDefinition{
		pk.AttributeDefinition(),
		sk.AttributeDefinition(),
		nanoTime.AttributeDefinition(),
		goTime.AttributeDefinition(),
		millisTime.AttributeDefinition(),
		secondsTime.AttributeDefinition(),
	}

	keys := []dynamodb.KeySchemaElement{
		pk.KeyElement(dynamodb.KeyTypeHash),
		sk.KeyElement(dynamodb.KeyTypeRange),
	}

	lsis := []dynamodb.LocalSecondaryIndex{
		LSI("go-time-index", *pk, *goTime, dynamodb.ProjectionTypeAll, nil),
	}

	defaultThroughput := &dynamodb.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(1), WriteCapacityUnits: aws.Int64(1)}

	gsis := GlobalSecondaryIndexes{
		GSI("nano-time-index", *pk, *nanoTime, dynamodb.ProjectionTypeAll, defaultThroughput, nil),
		GSI("millis-time-index", *pk, *millisTime, dynamodb.ProjectionTypeAll, defaultThroughput, nil),
		GSI("seconds-time-index", *pk, *secondsTime, dynamodb.ProjectionTypeAll, defaultThroughput, nil),
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
