package dynamocity

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Date represents a sortable Date with fixed date precision.
//
// Date implements dynamodbattribute.Marshaler, dynamodbattribute.Unmarshaller specifically for "YYYY-MM-DD"
// format which does not permit any timestamp; however, once this format is marshalled it may be sorted correctly in a
// string value
type Date time.Time

// MarshalDynamoDBAttributeValue implements the dynamodb.Marshaler interface to marshal
// a dynamocity.Date into a DynamoDB AttributeValue string value with specific second precision
func (t Date) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	rfcTime := time.Time(t).Format(StrictDateFmt)
	av.S = &rfcTime
	return nil
}

// UnmarshalDynamoDBAttributeValue implements the dynamodb.Unmarshaler interface to unmarshal
// a dynamodb.AttributeValue into a dynamocity.Date. This unmarshal is flexible and supports any timestamp
// with nanosecond precision
func (t *Date) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	timeString := *av.S
	date, err := time.Parse(FlexibleNanoFmt, timeString)
	if err != nil {
		return err
	}
	*t = Date(date)
	return nil
}

// Time is a handler func to return an instance of dynamocity.Date as time.Time
func (t Date) Time() time.Time {
	return time.Time(t)
}

// String implements the fmt.Stringer interface to supply a native String representation for a value in time.RFC3339
// Format with second precision
func (t Date) String() string {
	return t.Time().Format(StrictDateFmt)
}

// UnmarshalJSON implements the json.Unmarshaler interface to marshal RFC3339 timestamps with second precision
func (t *Date) UnmarshalJSON(b []byte) error {
	str, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	parsedTime, err := time.Parse(FlexibleNanoFmt, str)
	if err != nil {
		return fmt.Errorf("Timestamp '%s' cannot be unmarshalled as a valid RFC3339 timestamp", str)
	}
	*t = Date(parsedTime)
	return nil
}

// MarshalJSON implements the json.Marshaler interface to marshal RFC3339 timestamps with second precision
func (t Date) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(t.String())), nil
}
