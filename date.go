package dynamocity

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Date represents a sortable Date with fixed date precision.
//
// Date implements attributevalue.Marshaler specifically for "YYYY-MM-DD"
// format which does not permit any timestamp; however, it can unmarshall from any
// Timestamp with nanosecond precision.
type Date time.Time

// MarshalDynamoDBAttributeValue implements the attributevalue.Marshaler interface to marshal
// a dynamocity.Date into a DynamoDB AttributeValue string value with specific second precision
func (t Date) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	rfcTime := time.Time(t).Format(StrictDateFmt)
	return &types.AttributeValueMemberS{
		Value: rfcTime,
	}, nil
}

// UnmarshalDynamoDBAttributeValue implements the attributevalue.Unmarshaler interface to unmarshal
// a attributevalue.AttributeValue into a dynamocity.Date. This unmarshal is flexible and supports any timestamp
// with nanosecond precision
func (t *Date) UnmarshalDynamoDBAttributeValue(av types.AttributeValue) error {
	tv, ok := av.(*types.AttributeValueMemberS)
	if !ok {
		return &attributevalue.UnmarshalTypeError{
			Value: fmt.Sprintf("%T", av),
			Type:  reflect.TypeOf((*MillisTime)(nil)),
		}
	}
	date, err := parse(tv.Value)
	if err != nil {
		return err
	}
	*t = Date(date)
	return nil
}

// parse is a helper function to assist with parsing a string to a time.Time.
//
// This function will attempt to parse using dynamocity.FlexibleNanoFmt, and if that fails
// dynamocity.StrictDateFmt
func parse(str string) (time.Time, error) {
	parsedTime, err := time.Parse(FlexibleNanoFmt, str)
	if err == nil {
		return parsedTime, nil
	}
	return time.Parse(StrictDateFmt, str)
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

// UnmarshalJSON implements the json.Unmarshaler interface to unmarshal a date or RFC3339 timestamp
func (t *Date) UnmarshalJSON(b []byte) error {
	str, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	parsedTime, err := parse(str)
	if err != nil {
		return fmt.Errorf("Timestamp '%s' cannot be unmarshalled", str)
	}
	*t = Date(parsedTime)
	return nil
}

// MarshalJSON implements the json.Marshaler interface to marshal RFC3339 timestamps with second precision
func (t Date) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(t.String())), nil
}

// ParseDate will attempt to parse any RFC3339 Timestamp or date with format YYYY-MM-DD to a dynamocity.Date
func ParseDate(str string) (Date, error) {
	time, err := parse(str)
	return Date(time), err
}
