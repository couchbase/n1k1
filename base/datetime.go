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

// timeToMillis mirrors cbq's timeToMillis (expression/func_date.go): rounds t to
// the millisecond then returns epoch millis as a float64 (always integer-valued).
func timeToMillis(t time.Time) float64 {
	rounded := t.Round(time.Millisecond)
	return float64(rounded.Unix()*1000) + float64(rounded.Nanosecond())/1000000.0
}

// isLeapYear mirrors cbq's isLeapYear.
func isLeapYear(year int) bool {
	return year%400 == 0 || (year%4 == 0 && year%100 != 0)
}

// DateAddMillis adds n intervals of the named part to the epoch-millis, mirroring
// cbq's DATE_ADD_MILLIS: millisToTime (time.Unix -> the process LOCAL zone) then
// dateAdd then timeToMillis. It is a byte-for-byte port of cbq's dateAdd, so
// results match within a process. ok=false (caller yields NULL) when: millis is
// out of the representable range; n is not integral; part is not a known
// component; or the arithmetic overflows the representable range. No string
// parsing or named-timezone loading is involved, so this stays fully native.
func DateAddMillis(millis float64, n float64, part []byte) (float64, bool) {
	if millis < DateMinMillis || millis > DateMaxMillis {
		return 0, false // W_DATE_OVERFLOW
	}
	if n != math.Trunc(n) {
		return 0, false // W_DATE_NON_INT_VALUE
	}

	t := time.Unix(int64(millis/1000), int64(math.Mod(millis, 1000)*1000000.0))

	res, ok := TimeAdd(t, int(n), part)
	if !ok {
		return 0, false
	}

	return timeToMillis(res), true
}

// TimeAdd is a byte-for-byte port of cbq's dateAdd (expression/func_date.go): add
// n of the named part (case-insensitive) to t. ok=false on an unknown component or
// on an overflow of the representable range (matching cbq's W_DATE_* warnings,
// which the boxed lane surfaces as NULL). The calendar_month case reproduces cbq's
// last-day-of-month rounding exactly.
func TimeAdd(t time.Time, n int, part []byte) (time.Time, bool) {
	p := strings.ToLower(string(part))

	if n == 0 {
		return t, true
	}

	var res time.Time

	switch p {
	case "millennium":
		res = t.AddDate(n*1000, 0, 0)
	case "century":
		res = t.AddDate(n*100, 0, 0)
	case "decade":
		res = t.AddDate(n*10, 0, 0)
	case "year":
		res = t.AddDate(n, 0, 0)
	case "quarter":
		res = t.AddDate(0, n*3, 0)
	case "month":
		res = t.AddDate(0, n, 0)
	case "calendar_month":
		// adds months but if the original was the last day of the start month, the
		// result is the last day of the new month; if the new day would be beyond
		// the end of the new month, round it down to the end of the new month.
		om := t.Month()
		od := t.Day()
		last := false
		switch {
		case om == time.January || om == time.March || om == time.May || om == time.July ||
			om == time.August || om == time.October || om == time.December:
			if od == 31 {
				last = true
			}
		case om == time.February:
			ly := isLeapYear(t.Year())
			if ly && od == 29 {
				last = true
			} else if !ly && od == 28 {
				last = true
			}
		default:
			if od == 30 {
				last = true
			}
		}
		ny := t.Year() + (n / 12)
		nm := time.January
		if n > 0 {
			tt := int(om-1) + (n % 12)
			if tt >= 12 {
				tt -= 12
				ny++
			}
			nm = time.Month(tt + 1)
		} else {
			tt := int(om-1) + (n % 12)
			if tt < 0 {
				tt += 12
				ny--
			}
			nm = time.Month(tt + 1)
		}
		nd := od
		if last {
			switch {
			case nm == time.January || nm == time.March || nm == time.May || nm == time.July ||
				nm == time.August || nm == time.October || nm == time.December:
				nd = 31
			case nm == time.February:
				nd = 28
				if isLeapYear(ny) {
					nd = 29
				}
			default:
				nd = 30
			}
		} else {
			switch {
			case nm == time.January || nm == time.March || nm == time.May || nm == time.July ||
				nm == time.August || nm == time.October || nm == time.December:
				if nd > 31 {
					nd = 31
				}
			case nm == time.February:
				max := 28
				if isLeapYear(ny) {
					max = 29
				}
				if nd > max {
					nd = max
				}
			default:
				if nd > 30 {
					nd = 30
				}
			}
		}
		res = time.Date(ny, nm, nd, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
	case "week":
		res = t.AddDate(0, 0, n*7)
	case "day":
		res = t.AddDate(0, 0, n)
	case "hour":
		res = t.Add(time.Duration(n) * time.Hour)
	case "minute":
		res = t.Add(time.Duration(n) * time.Minute)
	case "second":
		res = t.Add(time.Duration(n) * time.Second)
	case "millisecond":
		res = t.Add(time.Duration(n) * time.Millisecond)
	default:
		return t, false // W_DATE_INVALID_COMPONENT
	}

	t1 := timeToMillis(t)
	t2 := timeToMillis(res)
	if (n > 0 && t1 > t2) || (n < 0 && t1 < t2) || t2 < DateMinMillis || t2 > DateMaxMillis {
		return t, false // W_DATE_OVERFLOW
	}

	return res, true
}
