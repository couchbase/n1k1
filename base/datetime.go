//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package base

import (
	"math"
	"strings"
	"time"
)

// Date/time helpers. Only the millis-based, non-volatile pieces are native so
// far -- string parsing (cbq's large ordered format list) and named-timezone
// loading stay in the cbq fallback.

// cbq's representable epoch-millis range (expression/func_date.go _MIN/_MAX_MILLIS,
// -9999-01-01 .. 9999-12-31 accounting for max TZ offsets); outside this cbq warns
// and yields NULL.
const (
	DateMinMillis = -377705073600000
	DateMaxMillis = +253402250399999
)

// DatePartMillis extracts a date/time component from epoch millis, mirroring cbq's
// DATE_PART_MILLIS (2-arg form): millisToTime (time.Unix -> the process LOCAL zone,
// exactly as cbq) then datePart. ok=false (caller yields NULL) if millis is outside
// the representable range or part is not a known component. The component set and
// arithmetic are a byte-for-byte port of cbq's datePart, so results are identical
// within a process. The 3-arg named-timezone form is left to the cbq fallback.
func DatePartMillis(millis float64, part []byte) (int, bool) {
	if millis < DateMinMillis || millis > DateMaxMillis {
		return 0, false
	}

	t := time.Unix(int64(millis/1000), int64(math.Mod(millis, 1000)*1000000.0))

	switch strings.ToLower(string(part)) {
	case "millennium":
		return (t.Year() / 1000) + 1, true
	case "century":
		return (t.Year() / 100) + 1, true
	case "decade":
		return t.Year() / 10, true
	case "year":
		return t.Year(), true
	case "quarter":
		return (int(t.Month()) + 2) / 3, true
	case "calendar_month", "month":
		return int(t.Month()), true
	case "day":
		return t.Day(), true
	case "hour":
		return t.Hour(), true
	case "minute":
		return t.Minute(), true
	case "second":
		return t.Second(), true
	case "millisecond":
		return t.Nanosecond() / int(time.Millisecond), true
	case "week":
		return int(math.Ceil(float64(t.YearDay()) / 7.0)), true
	case "day_of_year", "doy":
		return t.YearDay(), true
	case "day_of_week", "dow":
		return int(t.Weekday()), true
	case "iso_week":
		_, w := t.ISOWeek()
		return w, true
	case "iso_year":
		y, _ := t.ISOWeek()
		return y, true
	case "iso_dow":
		d := int(t.Weekday())
		if d == 0 {
			d = 7
		}
		return d, true
	case "timezone":
		_, z := t.Zone()
		return z, true
	case "timezone_hour":
		_, z := t.Zone()
		return z / (60 * 60), true
	case "timezone_minute":
		_, z := t.Zone()
		zh := z / (60 * 60)
		z = z - (zh * (60 * 60))
		return z / 60, true
	default:
		return 0, false
	}
}
