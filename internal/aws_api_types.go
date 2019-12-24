package internal

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
)

// Attributes type alias for a slice of dynamodb.AttributeDefinition
type Attributes []dynamodb.AttributeDefinition

// GlobalSecondaryIndexes type alias for a slice of dynamodb.GlobalSecondaryIndex
type GlobalSecondaryIndexes []dynamodb.GlobalSecondaryIndex

// LocalSecondaryIndexes type alias for a slice of dynamodb.LocalSecondaryIndex
type LocalSecondaryIndexes []dynamodb.LocalSecondaryIndex

// Keys type alias for a slice of dynamodb.KeySchemaElement
type Keys []dynamodb.KeySchemaElement

// AttributeDefinition is a type alias for dynamodb.AttributeDefinition
type AttributeDefinition dynamodb.AttributeDefinition

// AttributeDefinition returns a dynamocity.AttributeDefinition as a dynamodb.AttributeDefinition
func (a AttributeDefinition) AttributeDefinition() dynamodb.AttributeDefinition {
	return dynamodb.AttributeDefinition(a)
}

// MakeAttribute is a factory function for creating an AttributeDefinition for the specified attribute type
func MakeAttribute(attributeName string, attributeType dynamodb.ScalarAttributeType) *AttributeDefinition {

	return &AttributeDefinition{
		AttributeName: aws.String(attributeName),
		AttributeType: attributeType,
	}
}

// KeyElement will return a dynamodb.KeySchemaElement of the specified dynamodb.KeyType for the given AttributeDefinition
func (a AttributeDefinition) KeyElement(k dynamodb.KeyType) dynamodb.KeySchemaElement {
	return dynamodb.KeySchemaElement{
		AttributeName: a.AttributeName,
		KeyType:       k,
	}
}

// LSI is a factory function for creating a dynamodb.LocalSecondaryIndex
func LSI(i string, h AttributeDefinition, s AttributeDefinition, p dynamodb.ProjectionType, nonKeyAttrs []string) dynamodb.LocalSecondaryIndex {
	projection := &dynamodb.Projection{
		ProjectionType: p,
	}
	if p == dynamodb.ProjectionTypeInclude {
		projection.NonKeyAttributes = nonKeyAttrs
	}

	lsi := dynamodb.LocalSecondaryIndex{
		IndexName: aws.String(i),
		KeySchema: Keys{
			h.KeyElement(dynamodb.KeyTypeHash),
			s.KeyElement(dynamodb.KeyTypeRange),
		},
		Projection: projection,
	}
	return lsi
}

// GSI is a factory function for creating a dynamodb.GlobalSecondaryIndex
func GSI(i string, h AttributeDefinition, s AttributeDefinition, p dynamodb.ProjectionType, t *dynamodb.ProvisionedThroughput, nonKeyAttrs []string) dynamodb.GlobalSecondaryIndex {
	projection := &dynamodb.Projection{
		ProjectionType: p,
	}
	if p == dynamodb.ProjectionTypeInclude {
		projection.NonKeyAttributes = nonKeyAttrs
	}

	gsi := dynamodb.GlobalSecondaryIndex{
		IndexName: aws.String(i),
		KeySchema: Keys{
			h.KeyElement(dynamodb.KeyTypeHash),
			s.KeyElement(dynamodb.KeyTypeRange),
		},
		Projection:            projection,
		ProvisionedThroughput: t,
	}

	return gsi
}

// PutItem is a utility function to put an item in the specified table using the provided *dynamodb.Client
func PutItem(db *dynamodb.Client, tableName string, item interface{}) (*dynamodb.PutItemResponse, error) {
	i, err := dynamodbattribute.MarshalMap(item)

	if err != nil {
		return nil, err
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      i,
	}

	req := db.PutItemRequest(input)
	resp, err := req.Send(req.Context())
	return resp, err
}
