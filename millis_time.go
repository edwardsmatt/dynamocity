package dynamocity

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// MillisTime represents a sortable strict RFC3339 Timestamp with fixed millisecond precision, making it string sortable.
// MillisTime implements dynamodbattribute.Marshaler, dynamodbattribute.Unmarshaller
// The standard library time.RFC3339Nano format removes trailing zeros from the seconds field
// and thus may not sort correctly once formatted.
type MillisTime time.Time

// MarshalDynamoDBAttributeValue implements the dynamodb.Marshaler interface to marshal
// a dynamocity.MillisTime into a DynamoDB AttributeValue string value with specific millisecond precision
func (t MillisTime) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	rfcTime := time.Time(t).Format(StrictMillisFmt)
	av.S = &rfcTime
	return nil
}

// UnmarshalDynamoDBAttributeValue implements the dynamodb.Unmarshaler interface to unmarshal
// a dynamodb.AttributeValue into a dynamocity.MillisTime. This unmarshal is flexible on millisecond precision
func (t *MillisTime) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	timeString := *av.S
	rfc339Time, err := time.Parse(FlexibleNanoFmt, timeString)
	if err != nil {
		return err
	}
	*t = MillisTime(rfc339Time)
	return nil
}

// Time is a handler func to return an instance of dynamocity.MillisTime as time.Time
func (t MillisTime) Time() time.Time {
	return time.Time(t)
}

// String implements the fmt.Stringer interface to supply a native String representation for a value in RFC3339
// Format with millisecond precision
func (t MillisTime) String() string {
	return t.Time().Format(StrictMillisFmt)
}

// UnmarshalJSON implements the json.Unmarshaler interface to marshal RFC3339 timestamps with millisecond precision
func (t *MillisTime) UnmarshalJSON(b []byte) error {
	str, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	parsedTime, err := time.Parse(FlexibleNanoFmt, str)
	if err != nil {
		return fmt.Errorf("Timestamp '%s' cannot be unmarshalled as a valid RFC3339 timestamp", str)
	}
	*t = MillisTime(parsedTime)
	return nil
}

// MarshalJSON implements the json.Marshaler interface to marshal RFC3339 timestamps with millisecond precision
func (t MillisTime) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(t.String())), nil
}
