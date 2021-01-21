package testutils

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Attributes type alias for a slice of types.AttributeDefinition
type Attributes []types.AttributeDefinition

// GlobalSecondaryIndexes type alias for a slice of types.GlobalSecondaryIndex
type GlobalSecondaryIndexes []types.GlobalSecondaryIndex

// LocalSecondaryIndexes type alias for a slice of types.LocalSecondaryIndex
type LocalSecondaryIndexes []types.LocalSecondaryIndex

// Keys type alias for a slice of types.KeySchemaElement
type Keys []types.KeySchemaElement

// AttributeDefinition is a type alias for types.AttributeDefinition
type AttributeDefinition types.AttributeDefinition

// AttributeDefinition returns a dynamocity.AttributeDefinition as a types.AttributeDefinition
func (a AttributeDefinition) AttributeDefinition() types.AttributeDefinition {
	return types.AttributeDefinition(a)
}

// MakeAttribute is a factory function for creating an AttributeDefinition for the specified attribute type
func MakeAttribute(attributeName string, attributeType types.ScalarAttributeType) *AttributeDefinition {

	return &AttributeDefinition{
		AttributeName: aws.String(attributeName),
		AttributeType: attributeType,
	}
}

// KeyElement will return a types.KeySchemaElement of the specified types.KeyType for the given AttributeDefinition
func (a AttributeDefinition) KeyElement(k types.KeyType) types.KeySchemaElement {
	return types.KeySchemaElement{
		AttributeName: a.AttributeName,
		KeyType:       k,
	}
}

// LSI is a factory function for creating a types.LocalSecondaryIndex
func LSI(i string, h AttributeDefinition, s AttributeDefinition, p types.ProjectionType, nonKeyAttrs []string) types.LocalSecondaryIndex {
	projection := &types.Projection{
		ProjectionType: p,
	}
	if p == types.ProjectionTypeInclude {
		projection.NonKeyAttributes = nonKeyAttrs
	}

	lsi := types.LocalSecondaryIndex{
		IndexName: aws.String(i),
		KeySchema: Keys{
			h.KeyElement(types.KeyTypeHash),
			s.KeyElement(types.KeyTypeRange),
		},
		Projection: projection,
	}
	return lsi
}

// GSI is a factory function for creating a types.GlobalSecondaryIndex
func GSI(i string, h AttributeDefinition, s AttributeDefinition, p types.ProjectionType, t *types.ProvisionedThroughput, nonKeyAttrs []string) types.GlobalSecondaryIndex {
	projection := &types.Projection{
		ProjectionType: p,
	}
	if p == types.ProjectionTypeInclude {
		projection.NonKeyAttributes = nonKeyAttrs
	}

	gsi := types.GlobalSecondaryIndex{
		IndexName: aws.String(i),
		KeySchema: Keys{
			h.KeyElement(types.KeyTypeHash),
			s.KeyElement(types.KeyTypeRange),
		},
		Projection:            projection,
		ProvisionedThroughput: t,
	}

	return gsi
}

// PutItem is a utility function to put an item in the specified table using the provided *types.Client
func PutItem(db *dynamodb.Client, tableName string, item interface{}) (*dynamodb.PutItemOutput, error) {
	i, err := attributevalue.MarshalMap(item)

	if err != nil {
		return nil, err
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      i,
	}

	return db.PutItem(context.TODO(), input)
}
