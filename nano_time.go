package dynamocity

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// NanoTime represents a sortable strict RFC3339 Timestamp with fixed nanosecond precision, making it string sortable.
// NanoTime implements attributevalue.Marshaler, attributevalue.Unmarshaller
// The standard library time.RFC3339Nano format removes trailing zeros from the fractional seconds field
// and thus may not sort correctly once formatted.
type NanoTime time.Time

// MarshalDynamoDBAttributeValue implements the attributevalue.Marshaler interface to marshal
// a dynamocity.NanoTime into a DynamoDB AttributeValue string value with specific nanosecond precision
func (t NanoTime) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	rfcTime := time.Time(t).Format(StrictNanoFmt)
	return &types.AttributeValueMemberS{
		Value: rfcTime,
	}, nil
}

// UnmarshalDynamoDBAttributeValue implements the dynamodb.Unmarshaler interface to unmarshal
// a types.AttributeValue into a dynamocity.NanoTime. This unmarshal is flexible on nanosecond precision
func (t *NanoTime) UnmarshalDynamoDBAttributeValue(av types.AttributeValue) error {
	tv, ok := av.(*types.AttributeValueMemberS)
	if !ok {
		return &attributevalue.UnmarshalTypeError{
			Value: fmt.Sprintf("%T", av),
			Type:  reflect.TypeOf((*NanoTime)(nil)),
		}
	}
	rfc339Time, err := time.Parse(FlexibleNanoFmt, tv.Value)
	if err != nil {
		return err
	}
	*t = NanoTime(rfc339Time)
	return nil
}

// Time is a handler func to return an instance of dynamocity.NanoTime as time.Time
func (t NanoTime) Time() time.Time {
	return time.Time(t)
}

// String implements the fmt.Stringer interface to supply a native String representation for a value in RFC3339
// Format with nanosecond precision
func (t NanoTime) String() string {
	return t.Time().Format(StrictNanoFmt)
}

// UnmarshalJSON implements the json.Unmarshaler interface to marshal RFC3339 timestamps with nanosecond precision
func (t *NanoTime) UnmarshalJSON(b []byte) error {
	str, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	parsedTime, err := time.Parse(FlexibleNanoFmt, str)
	if err != nil {
		return fmt.Errorf("Timestamp '%s' cannot be unmarshalled as a valid RFC3339 timestamp", str)
	}
	*t = NanoTime(parsedTime)
	return nil
}

// MarshalJSON implements the json.Marshaler interface to marshal RFC3339 timestamps with nanosecond precision
func (t NanoTime) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(t.String())), nil
}
