package dynamocity

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// SecondsTime represents a sortable strict RFC3339 Timestamp with fixed second precision, making it string sortable.
// SecondsTime implements dynamodbattribute.Marshaler, dynamodbattribute.Unmarshaller specifically for the time.RFC3339
// format which does not permit fractional seconds; however, once this format is marshalled it may be sorted correctly in a
// string value
type SecondsTime time.Time

// MarshalDynamoDBAttributeValue implements the dynamodb.Marshaler interface to marshal
// a dynamocity.SecondsTime into a DynamoDB AttributeValue string value with specific second precision
func (t SecondsTime) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	rfcTime := time.Time(t).Format(StrictSecondsFmt)
	return &types.AttributeValueMemberS{
		Value: rfcTime,
	}, nil
}

// UnmarshalDynamoDBAttributeValue implements the dynamodb.Unmarshaler interface to unmarshal
// a dynamodb.AttributeValue into a dynamocity.SecondsTime. This unmarshal is flexible on fractional second precision
func (t *SecondsTime) UnmarshalDynamoDBAttributeValue(av types.AttributeValue) error {
	tv, ok := av.(*types.AttributeValueMemberS)
	if !ok {
		return &attributevalue.UnmarshalTypeError{
			Value: fmt.Sprintf("%T", av),
			Type:  reflect.TypeOf((*SecondsTime)(nil)),
		}
	}
	rfc339Time, err := time.Parse(FlexibleNanoFmt, tv.Value)
	if err != nil {
		return err
	}
	*t = SecondsTime(rfc339Time)
	return nil
}

// Time is a handler func to return an instance of dynamocity.SecondsTime as time.Time
func (t SecondsTime) Time() time.Time {
	return time.Time(t)
}

// String implements the fmt.Stringer interface to supply a native String representation for a value in time.RFC3339
// Format with second precision
func (t SecondsTime) String() string {
	return t.Time().Format(StrictSecondsFmt)
}

// UnmarshalJSON implements the json.Unmarshaler interface to marshal RFC3339 timestamps with second precision
func (t *SecondsTime) UnmarshalJSON(b []byte) error {
	str, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	parsedTime, err := time.Parse(FlexibleNanoFmt, str)
	if err != nil {
		return fmt.Errorf("Timestamp '%s' cannot be unmarshalled as a valid RFC3339 timestamp", str)
	}
	*t = SecondsTime(parsedTime)
	return nil
}

// MarshalJSON implements the json.Marshaler interface to marshal RFC3339 timestamps with second precision
func (t SecondsTime) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(t.String())), nil
}
