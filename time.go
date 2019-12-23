package dynamocity

import (
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// flexibleNanoUnmarshallingFmt applies a backwards compatible nanosecond precision unmarshalling of a dynamocity.Time.
// This ensures that timestamp strings marshalled with time.RFC3339Nano who may have had trailing zeros stripped
// may be unmarshalled safely.
const flexibleNanoUnmarshallingFmt = time.RFC3339Nano

// strictNanoMarshallingFmt applies a strict nanosecond precision marshalling of a dynamocity.Time.
// This ensures that trailing zeros are never stripped. The standard library time.RFC3339Nano format
// removes trailing zeros beyond millisecond precision from the seconds field, and thus may not sort correctly once formatted.
const strictNanoMarshallingFmt = "2006-01-02T15:04:05.000000000Z07:00"

// Time represents a sortable strict RFC3339 Timestamp with fixed nanosecond precision, making it string sortable.
// Time implements dynamodbattribute.Marshaler, dynamodbattribute.Unmarshaller
// The standard library time.RFC3339Nano format removes trailing zeros beyond millis from the seconds field
// and thus may not sort correctly once formatted.
type Time time.Time

// MarshalDynamoDBAttributeValue implements the dynamodb.Marshaler interface to marshal
// a dynamocity.Time into a DynamoDB AttributeValue string value with specific nanosecond precision
func (t Time) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	rfcTime := time.Time(t).Format(strictNanoMarshallingFmt)
	av.S = &rfcTime
	return nil
}

// UnmarshalDynamoDBAttributeValue implements the dynamodb.Unmarshaler interface to unmarshal
// a dynamodb.AttributeValue into a dynamocity.Time. This unmarshal is flexible on nanosecond precision
func (t *Time) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	timeString := *av.S
	rfc339Time, err := time.Parse(flexibleNanoUnmarshallingFmt, timeString)
	if err != nil {
		return err
	}
	*t = Time(rfc339Time)
	return nil
}

// Time is a handler func to return an instance of dynamocity.Time as time.Time
func (t Time) Time() time.Time {
	return time.Time(t)
}

// BetweenStartInc will return true if this dynamocity.Time is after or equal to the start and before the end
func (t Time) BetweenStartInc(startInclusive, endExclusive Time) bool {
	afterOrEqualToStart := t.Time().After(startInclusive.Time()) || t.Time().Equal(startInclusive.Time())
	beforeEnd := t.Time().Before(endExclusive.Time())
	return afterOrEqualToStart && beforeEnd
}

// BetweenEndInc will return true if this dynamocity.Time is after the start and before or equal to the end
func (t Time) BetweenEndInc(startExclusive, endInclusive Time) bool {
	afterStart := t.Time().After(startExclusive.Time())
	beforeOrEqualToEnd := t.Time().Before(endInclusive.Time()) || t.Time().Equal(endInclusive.Time())
	return afterStart && beforeOrEqualToEnd
}

// BetweenExclusive will return true if this dynamocity.Time is after the start and before to the end
func (t Time) BetweenExclusive(startExclusive, endExclusive Time) bool {
	afterStart := t.Time().After(startExclusive.Time())
	beforeEnd := t.Time().Before(endExclusive.Time())
	return afterStart && beforeEnd
}

// BetweenInclusive will return true if this dynamocity.Time is after or equal to the start and before or equal to the end
func (t Time) BetweenInclusive(start, end Time) bool {
	afterOrEqualToStart := t.Time().After(start.Time()) || t.Time().Equal(start.Time())
	beforeOrEqualToEnd := t.Time().Before(end.Time()) || t.Time().Equal(end.Time())
	return afterOrEqualToStart && beforeOrEqualToEnd
}

// String implements the fmt.Stringer interface to supply a native String representation for a value in RFC3339
// Format with millis precision
func (t Time) String() string {
	return t.Time().Format(strictNanoMarshallingFmt)
}

// UnmarshalJSON implements the json.Unmarshaler interface to marshal RFC3339 timestamps with millis precision
func (t *Time) UnmarshalJSON(b []byte) error {
	str, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	parsedTime, err := time.Parse(flexibleNanoUnmarshallingFmt, str)
	if err != nil {
		return err
	}
	*t = Time(parsedTime)
	return nil
}

// MarshalJSON implements the json.Marshaler interface to marshal RFC3339 timestamps with millis precision
func (t Time) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(t.String())), nil
}
