package dynamocity

import "time"

// StrictNanoFmt applies a strict nanosecond precision marshalling of a dynamocity.NanoTime.
// This ensures that trailing zeros are never stripped. The standard library time.RFC3339Nano format
// removes trailing zeros from the seconds field, and thus may not sort correctly once formatted.
//
// Unmarshalling using StrictNanoFmt will result in errors if the string does not strictly match the RFC3339 format with fixed
// nanosecond precision. For example: `2006-01-02T15:04:05.000000000Z07:00`
const StrictNanoFmt = "2006-01-02T15:04:05.000000000Z07:00"

// StrictMillisFmt applies a strict millisecond precision marshalling of a dynamocity.NanoTime.
// This ensures that trailing zeros are never stripped. The standard library time.RFC3339Nano format
// removes trailing zeros from the seconds field, and thus may not sort correctly once formatted.
//
// Unmarshalling using StrictMillisFmt will result in errors if the string does not strictly match the RFC3339 format with millisecond
// precision. For example: `2006-01-02T15:04:05.000Z07:00`
const StrictMillisFmt = "2006-01-02T15:04:05.000Z07:00"

// StrictSecondsFmt is the standard library time.RFC3339, which applies a strict second precision marshalling and unmarshalling capability.
//
// Unmarshalling using StrictSecondsFmt will result in errors if the string does not strictly match the time.RFC3339 format.
// For example: `2006-01-02T15:04:05Z07:00`
const StrictSecondsFmt = time.RFC3339

// FlexibleNanoFmt is the standard library time.RFC3339Nano, which applies a flexible compatible nanosecond precision
// marshalling and unmarshalling capability. However, when marshalling the standard library time.RFC3339Nano format removes trailing
// zeros from the seconds field, and thus may not sort correctly once formatted.
//
// Therefore, this format is unsafe for marshalling to dynamo or JSON if the resultant value is expected to be sortable by string.
const FlexibleNanoFmt = time.RFC3339Nano

// BetweenStartInc will return true if this dynamocity.MillisTime is after or equal to the start and before the end
func BetweenStartInc(t, startInclusive, endExclusive time.Time) bool {
	afterOrEqualToStart := t.After(startInclusive) || t.Equal(startInclusive)
	beforeEnd := t.Before(endExclusive)
	return afterOrEqualToStart && beforeEnd
}

// BetweenEndInc will return true if this dynamocity.MillisTime is after the start and before or equal to the end
func BetweenEndInc(t, startExclusive, endInclusive time.Time) bool {
	afterStart := t.After(startExclusive)
	beforeOrEqualToEnd := t.Before(endInclusive) || t.Equal(endInclusive)
	return afterStart && beforeOrEqualToEnd
}

// BetweenExclusive will return true if this dynamocity.MillisTime is after the start and before to the end
func BetweenExclusive(t, startExclusive, endExclusive time.Time) bool {
	afterStart := t.After(startExclusive)
	beforeEnd := t.Before(endExclusive)
	return afterStart && beforeEnd
}

// BetweenInclusive will return true if this dynamocity.MillisTime is after or equal to the start and before or equal to the end
func BetweenInclusive(t, start, end time.Time) bool {
	afterOrEqualToStart := t.After(start) || t.Equal(start)
	beforeOrEqualToEnd := t.Before(end) || t.Equal(end)
	return afterOrEqualToStart && beforeOrEqualToEnd
}
