// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::LazyLock;

use databend_common_exception::Result;
use jiff::civil::date;
use jiff::civil::datetime;
use jiff::civil::Date;
use jiff::civil::Time;
use jiff::civil::Weekday;
use jiff::tz::TimeZone;
use jiff::SignedDuration;
use jiff::SpanRelativeTo;
use jiff::Timestamp;
use jiff::Unit;
use jiff::Zoned;
use num_traits::AsPrimitive;

use crate::types::date::clamp_date;
use crate::types::timestamp::clamp_timestamp;
use crate::types::timestamp::MICROS_PER_SEC;

pub trait DateConverter {
    fn to_date(&self, tz: TimeZone) -> Date;
    fn to_timestamp(&self, tz: TimeZone) -> Zoned;
}

impl<T> DateConverter for T
where T: AsPrimitive<i64>
{
    fn to_date(&self, _tz: TimeZone) -> Date {
        let dur = SignedDuration::from_hours(self.as_() * 24);
        date(1970, 1, 1).checked_add(dur).unwrap()
    }

    fn to_timestamp(&self, tz: TimeZone) -> Zoned {
        // Can't use `tz.timestamp_nanos(self.as_() * 1000)` directly, is may cause multiply with overflow.
        let micros = self.as_();
        let (mut secs, mut nanos) = (micros / MICROS_PER_SEC, (micros % MICROS_PER_SEC) * 1_000);
        if nanos < 0 {
            secs -= 1;
            nanos += 1_000_000_000;
        }

        if secs > 253402207200 {
            secs = 253402207200;
            nanos = 0;
        } else if secs < -377705023201 {
            secs = -377705023201;
            nanos = 0;
        }
        let ts = Timestamp::new(secs, nanos as i32).unwrap();
        ts.to_zoned(tz)
    }
}

pub const MICROSECS_PER_DAY: i64 = 86_400_000_000;

// Timestamp arithmetic factors.
pub const FACTOR_HOUR: i64 = 3600;
pub const FACTOR_MINUTE: i64 = 60;
pub const FACTOR_SECOND: i64 = 1;
const LAST_DAY_LUT: [i8; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

fn eval_years_base(
    year: i16,
    month: i8,
    day: i8,
    delta: i64,
    _add_months: bool,
) -> std::result::Result<Date, String> {
    let new_year = year as i64 + delta;
    let mut new_day = day;
    if std::intrinsics::unlikely(month == 2 && day == 29) {
        new_day = last_day_of_year_month(new_year as i16, month);
    }
    match Date::new(new_year as i16, month, new_day) {
        Ok(d) => Ok(d),
        Err(e) => Err(format!("Invalid date: {}", e)),
    }
}

fn eval_months_base(
    year: i16,
    month: i8,
    day: i8,
    delta: i64,
    add_months: bool,
) -> std::result::Result<Date, String> {
    let total_months = (month as i64 + delta - 1) as i16;
    let mut new_year = year + (total_months / 12);
    let mut new_month0 = total_months % 12;
    if new_month0 < 0 {
        new_year -= 1;
        new_month0 += 12;
    }

    // Handle month last day overflow, "2020-2-29" + "1 year" should be "2021-2-28", or "1990-1-31" + "3 month" should be "1990-4-30".
    // For ADD_MONTHS only, if the original day is the last day of the month, the result day of month will be the last day of the result month.
    let new_month = (new_month0 + 1) as i8;
    // Determine the correct day
    let max_day = last_day_of_year_month(new_year, new_month);
    let new_day = if add_months && day == last_day_of_year_month(year, month) {
        max_day
    } else {
        day.min(max_day)
    };

    match Date::new(new_year, (new_month0 + 1) as i8, new_day) {
        Ok(d) => Ok(d),
        Err(e) => Err(format!("Invalid date: {}", e)),
    }
}

// Get the last day of the year month, could be 28(non leap Feb), 29(leap year Feb), 30 or 31
fn last_day_of_year_month(year: i16, month: i8) -> i8 {
    let is_leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if std::intrinsics::unlikely(month == 2 && is_leap_year) {
        return 29;
    }
    LAST_DAY_LUT[month as usize]
}

macro_rules! impl_interval_year_month {
    ($name: ident, $op: expr) => {
        #[derive(Clone)]
        pub struct $name;

        impl $name {
            pub fn eval_date(
                date: i32,
                tz: TimeZone,
                delta: impl AsPrimitive<i64>,
                add_months: bool,
            ) -> std::result::Result<i32, String> {
                let date = date.to_date(tz);
                let new_date = $op(
                    date.year(),
                    date.month(),
                    date.day(),
                    delta.as_(),
                    add_months,
                )?;

                Ok(clamp_date(
                    new_date
                        .since((Unit::Day, Date::new(1970, 1, 1).unwrap()))
                        .unwrap()
                        .get_days() as i64,
                ))
            }

            pub fn eval_timestamp(
                us: i64,
                tz: TimeZone,
                delta: impl AsPrimitive<i64>,
                add_months: bool,
            ) -> std::result::Result<i64, String> {
                let ts = us.to_timestamp(tz.clone());
                let new_date = $op(ts.year(), ts.month(), ts.day(), delta.as_(), add_months)?;

                let mut ts = new_date
                    .at(ts.hour(), ts.minute(), ts.second(), ts.subsec_nanosecond())
                    .to_zoned(tz)
                    .map_err(|e| format!("{}", e))?
                    .timestamp()
                    .as_microsecond();
                clamp_timestamp(&mut ts);
                Ok(ts)
            }
        }
    };
}

impl_interval_year_month!(EvalYearsImpl, eval_years_base);
impl_interval_year_month!(EvalMonthsImpl, eval_months_base);

impl EvalYearsImpl {
    pub fn eval_date_diff(date_start: i32, date_end: i32, tz: TimeZone) -> i32 {
        let date_start = date_start.to_date(tz.clone());
        let date_end = date_end.to_date(tz);
        (date_end.year() - date_start.year()) as i32
    }

    pub fn eval_date_between(date_start: i32, date_end: i32, tz: TimeZone) -> i32 {
        if date_start == date_end {
            return 0;
        }
        if date_start > date_end {
            return -Self::eval_date_between(date_end, date_start, tz);
        }

        let date_start = date_start.to_date(tz.clone());
        let date_end = date_end.to_date(tz);

        let mut years = date_end.year() - date_start.year();

        // If the end month is less than the start month,
        // or the months are equal but the end day is less than the start day,
        // the last year is incomplete, minus 1
        if (date_end.month() < date_start.month())
            || (date_end.month() == date_start.month() && date_end.day() < date_start.day())
        {
            years -= 1;
        }

        years as i32
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: TimeZone) -> i64 {
        let date_start = date_start.to_timestamp(tz.clone());
        let date_end = date_end.to_timestamp(tz);
        date_end.year() as i64 - date_start.year() as i64
    }

    pub fn eval_timestamp_between(date_start: i64, date_end: i64, tz: TimeZone) -> i64 {
        if date_start == date_end {
            return 0;
        }
        if date_start > date_end {
            return -Self::eval_timestamp_between(date_end, date_start, tz);
        }
        let start = date_start.to_timestamp(tz.clone());
        let end = date_end.to_timestamp(tz);

        let mut years = end.year() - start.year();

        // Handle special cases on February 29 in leap years:
        // If the start date is February 29 and the end date is February 28, it is considered a full year (leap year to regular year).
        // Otherwise, the end date, month day, must be >= the start date, month day, and the time must be reached
        let start_month = start.month();
        let start_day = start.day();

        let end_month = end.month();
        let end_day = end.day();

        let start_is_feb_29 = start_month == 2 && start_day == 29;
        let end_is_feb_28 = end_month == 2 && end_day == 28;

        let end_before_start_date = (end_month < start_month)
            || (end_month == start_month && end_day < start_day)
            || (end_month == start_month && end_day == start_day && end.time() < start.time());

        if start_is_feb_29 && end_is_feb_28 {
        } else if end_before_start_date {
            years -= 1;
        }

        years as i64
    }
}

pub struct EvalISOYearsImpl;
impl EvalISOYearsImpl {
    pub fn eval_date_diff(date_start: i32, date_end: i32, tz: TimeZone) -> i32 {
        let date_start = date_start.to_date(tz.clone());
        let date_end = date_end.to_date(tz);
        date_end.iso_week_date().year() as i32 - date_start.iso_week_date().year() as i32
    }

    pub fn eval_date_between(date_start: i32, date_end: i32, tz: TimeZone) -> i32 {
        if date_start == date_end {
            return 0;
        }
        if date_start > date_end {
            return -Self::eval_date_between(date_end, date_start, tz);
        }
        let date_start = date_start.to_date(tz.clone());
        let date_end = date_end.to_date(tz);
        let mut years = date_end.iso_week_date().year() - date_start.iso_week_date().year();
        if (date_end.month() < date_start.month())
            || (date_end.month() == date_start.month() && date_end.day() < date_start.day())
        {
            years -= 1;
        }

        years as i32
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: TimeZone) -> i64 {
        let date_start = date_start.to_timestamp(tz.clone());
        let date_end = date_end.to_timestamp(tz);
        date_end.date().iso_week_date().year() as i64 - date_start.iso_week_date().year() as i64
    }

    pub fn eval_timestamp_between(date_start: i64, date_end: i64, tz: TimeZone) -> i64 {
        if date_start == date_end {
            return 0;
        }
        if date_start > date_end {
            return -Self::eval_timestamp_between(date_end, date_start, tz);
        }

        let start = date_start.to_timestamp(tz.clone());
        let end = date_end.to_timestamp(tz);
        let mut years =
            end.date().iso_week_date().year() as i64 - start.date().iso_week_date().year() as i64;
        let start_month = start.month();
        let start_day = start.day();

        let end_month = end.month();
        let end_day = end.day();

        let start_is_feb_29 = start_month == 2 && start_day == 29;
        let end_is_feb_28 = end_month == 2 && end_day == 28;

        let end_before_start_date = (end_month < start_month)
            || (end_month == start_month && end_day < start_day)
            || (end_month == start_month && end_day == start_day && end.time() < start.time());

        if start_is_feb_29 && end_is_feb_28 {
        } else if end_before_start_date {
            years -= 1;
        }

        years
    }
}

pub struct EvalYearWeeksImpl;
impl EvalYearWeeksImpl {
    fn yearweek(date: Date) -> i32 {
        let iso_week = date.iso_week_date();
        (iso_week.year() as i32 * 100) + iso_week.week() as i32
    }

    pub fn eval_date_diff(date_start: i32, date_end: i32, tz: TimeZone) -> i32 {
        let date_start = date_start.to_date(tz.clone());
        let date_end = date_end.to_date(tz);
        let end = Self::yearweek(date_end);
        let start = Self::yearweek(date_start);

        end - start
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: TimeZone) -> i64 {
        let date_start = date_start.to_timestamp(tz.clone());
        let date_end = date_end.to_timestamp(tz);
        let end = Self::yearweek(date_end.date()) as i64;
        let start = Self::yearweek(date_start.date()) as i64;

        end - start
    }

    // In duckdb datesub(yearweek, ) is same as datesub(week, ) But we can contain these logic
    // fn week_end(date: Date) -> Date {
    // let weekday = date.weekday();
    //
    // let days_to_sunday = 7 - weekday.to_monday_one_offset(); // monday=1, sunday=7
    // let dur = SignedDuration::from_hours(days_to_sunday as i64 * 24);
    // date.checked_add(dur).unwrap()
    // }
    // pub fn eval_date_between(start: i32, end: i32, tz: TimeZone) -> i32 {
    // if start == end {
    // return 0;
    // }
    //
    // let (earlier, later, sign) = if start <= end {
    // (start, end, 1)
    // } else {
    // (end, start, -1)
    // };
    //
    // let earlier = earlier.to_date(tz.clone());
    // let later = later.to_date(tz);
    //
    // let start_yw = Self::yearweek(earlier);
    // let end_yw = Self::yearweek(later);
    //
    // let mut diff = end_yw - start_yw;
    //
    // If the end week is incomplete, subtract 1
    // if later < Self::week_end(later) {
    // diff -= 1;
    // }
    //
    // diff * sign
    // }
    // pub fn eval_timestamp_between(start: i64, end: i64, tz: TimeZone) -> i64 {
    // if start == end {
    // return 0;
    // }
    //
    // let (earlier, later, sign) = if start <= end {
    // (start, end, 1)
    // } else {
    // (end, start, -1)
    // };
    //
    // let earlier = earlier.to_timestamp(tz.clone());
    // let later = later.to_timestamp(tz);
    //
    // let start_yw = Self::yearweek(earlier.date());
    // let end_yw = Self::yearweek(later.date());
    //
    // let mut diff = end_yw - start_yw;
    //
    // let week_end = EvalYearWeeksImpl::week_end(later.date());
    // if later.datetime() < week_end.at(23, 59, 59, 999_999_999) {
    // diff -= 1;
    // }
    //
    // diff as i64 * sign
    // }
}

pub struct EvalQuartersImpl;

impl EvalQuartersImpl {
    pub fn eval_date_diff(date_start: i32, date_end: i32, tz: TimeZone) -> i32 {
        EvalQuartersImpl::eval_timestamp_diff(
            date_start as i64 * MICROSECS_PER_DAY,
            date_end as i64 * MICROSECS_PER_DAY,
            tz,
        ) as i32
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: TimeZone) -> i64 {
        let date_start = date_start.to_timestamp(tz.clone());
        let date_end = date_end.to_timestamp(tz);
        (date_end.year() - date_start.year()) as i64 * 4 + ToQuarter::to_number(&date_end) as i64
            - ToQuarter::to_number(&date_start) as i64
    }

    // Return date corresponding to quarter number (1~4)
    // fn quarter(month: i8) -> i32 {
    // ((month - 1) / 3 + 1) as i32
    // }
    //
    //
    // fn quarter_start(year: i16, month: i8) -> (i16, i8) {
    // let q = ((month - 1) / 3) + 1;
    // let start_month = (q - 1) * 3 + 1;
    // (year, start_month)
    // }
    //
    // DuckDB directly calc month/3
    // pub fn eval_date_between(start: i32, end: i32, tz: TimeZone) -> i32 {
    // if start == end {
    // return 0;
    // }
    // let (earlier, later, sign) = if start <= end {
    // (start, end, 1)
    // } else {
    // (end, start, -1)
    // };
    //
    // let earlier = earlier.to_date(tz.clone());
    // let later = later.to_date(tz);
    //
    // let start_year = earlier.year();
    // let start_quarter = Self::quarter(earlier.month());
    // let end_year = later.year();
    // let end_quarter = Self::quarter(later.month());
    //
    // let mut diff =
    // (end_year - start_year) as i64 * 4 + (end_quarter as i64 - start_quarter as i64);
    //
    // let (last_quarter_start_year, last_quarter_start_month) =
    // Self::quarter_start(end_year, later.month());
    // let last_quarter_start_date = date(last_quarter_start_year, last_quarter_start_month, 1);
    //
    //
    // if later < last_quarter_start_date {
    // diff -= 1;
    // }
    //
    // (diff * sign) as i32
    // }
    // pub fn eval_timestamp_between(start: i64, end: i64, tz: TimeZone) -> i64 {
    // if start == end {
    // return 0;
    // }
    //
    // let (earlier, later, sign) = if start <= end {
    // (start, end, 1)
    // } else {
    // (end, start, -1)
    // };
    //
    // let earlier = earlier.to_timestamp(tz.clone());
    // let later = later.to_timestamp(tz);
    //
    // let start_year = earlier.year();
    // let start_quarter = Self::quarter(earlier.month());
    // let end_year = later.year();
    // let end_quarter = Self::quarter(later.month());
    //
    // let mut diff =
    // (end_year - start_year) as i64 * 4 + (end_quarter as i64 - start_quarter as i64);
    //
    // let (last_quarter_start_year, last_quarter_start_month) =
    // Self::quarter_start(later.year(), later.month());
    // let last_quarter_start_date = date(last_quarter_start_year, last_quarter_start_month, 1);
    // let last_quarter_start_datetime = last_quarter_start_date.to_datetime(earlier.time());
    //
    // if later.datetime() < last_quarter_start_datetime {
    // diff -= 1;
    // }
    // diff * sign
    // }
}

impl EvalMonthsImpl {
    pub fn eval_date_diff(date_start: i32, date_end: i32, tz: TimeZone) -> i32 {
        let date_start = date_start.to_date(tz.clone());
        let date_end = date_end.to_date(tz);
        (date_end.year() - date_start.year()) as i32 * 12 + date_end.month() as i32
            - date_start.month() as i32
    }

    pub fn eval_date_between(start: i32, end: i32, tz: TimeZone) -> i32 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_date_between(end, start, tz);
        }

        let start = start.to_date(tz.clone());
        let end = end.to_date(tz);

        let year_diff = end.year() - start.year();
        let month_diff = end.month() as i32 - start.month() as i32;
        let mut months = year_diff as i32 * 12 + month_diff;

        if end.day() < start.day() {
            months -= 1;
        }

        months
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, tz: TimeZone) -> i64 {
        EvalMonthsImpl::eval_date_diff(
            (date_start / MICROSECS_PER_DAY) as i32,
            (date_end / MICROSECS_PER_DAY) as i32,
            tz,
        ) as i64
    }

    pub fn eval_timestamp_between(start: i64, end: i64, tz: TimeZone) -> i64 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_timestamp_between(end, start, tz);
        }

        let start = start.to_timestamp(tz.clone());
        let end = end.to_timestamp(tz);
        let year_diff = end.year() - start.year();
        let month_diff = end.month() as i64 - start.month() as i64;
        let mut months = year_diff as i64 * 12 + month_diff;

        // Determine the time sequence. If the end time is less than the start time, it is incomplete
        if (end.day() < start.day()) || (end.day() == start.day() && end.time() < start.time()) {
            months -= 1;
        }

        months
    }

    // current we don't consider tz here
    pub fn months_between_ts(ts_a: i64, ts_b: i64) -> f64 {
        EvalMonthsImpl::months_between(
            (ts_a / 86_400_000_000) as i32,
            (ts_b / 86_400_000_000) as i32,
        )
    }

    pub fn months_between(date_a: i32, date_b: i32) -> f64 {
        let date_a = Date::new(1970, 1, 1)
            .unwrap()
            .checked_add(SignedDuration::from_hours(date_a as i64 * 24))
            .unwrap();
        let date_b = Date::new(1970, 1, 1)
            .unwrap()
            .checked_add(SignedDuration::from_hours(date_b as i64 * 24))
            .unwrap();

        let year_diff = (date_a.year() - date_b.year()) as i64;
        let month_diff = date_a.month() as i64 - date_b.month() as i64;

        // Calculate total months difference
        let total_months_diff = year_diff * 12 + month_diff;

        // Determine if special case for fractional part applies
        let is_same_day_of_month = date_a.day() == date_b.day();

        let are_both_end_of_month =
            date_a.last_of_month() == date_a && date_b.last_of_month() == date_b;
        let day_fraction = if is_same_day_of_month || are_both_end_of_month {
            0.0
        } else {
            let day_diff = date_a.day() as i32 - date_b.day() as i32;
            day_diff as f64 / 31.0 // Using 31-day month for fractional part
        };

        // Total difference including fractional part
        total_months_diff as f64 + day_fraction
    }
}

pub struct EvalWeeksImpl;

impl EvalWeeksImpl {
    pub fn eval_date_diff(date_start: i32, date_end: i32) -> i32 {
        // 1970-01-01 is ThursDay
        let date_start = date_start / 7 + (date_start % 7 >= 4) as i32;
        let date_end = date_end / 7 + (date_end % 7 >= 4) as i32;
        date_end - date_start
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64) -> i64 {
        EvalWeeksImpl::eval_date_diff(
            (date_start / MICROSECS_PER_DAY) as i32,
            (date_end / MICROSECS_PER_DAY) as i32,
        ) as i64
    }

    fn calculate_weeks_between_years(
        start_year: i32,
        end_year: i32,
        start_week: u32,
        end_week: u32,
    ) -> i32 {
        let mut weeks = 0;
        let mut current_year = start_year + 1;

        fn iso_weeks(year: i32) -> i32 {
            // Get the first day of the year
            let first_day = date(year as i16, 1, 1);

            // Determine the weekday of the first day
            let weekday = first_day.weekday();

            // Check if the year starts on a Thursday.
            if weekday == Weekday::Thursday {
                return 53;
            }

            // Check if the year starts on a Wednesday and is a leap year.
            if weekday == Weekday::Wednesday
                && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))
            {
                return 53;
            }
            52
        }
        while current_year < end_year {
            weeks += iso_weeks(current_year);
            current_year += 1;
        }

        // add start_year weeks and end_year weeks
        weeks += iso_weeks(start_year) - start_week as i32 + end_week as i32;
        weeks
    }

    pub fn eval_date_between(start: i32, end: i32, tz: TimeZone) -> i32 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_date_between(end, start, tz);
        }

        let earlier = start.to_date(tz.clone());
        let later = end.to_date(tz);
        let mut weeks = Self::calculate_weeks_between_years(
            earlier.year() as i32,
            later.year() as i32,
            earlier.iso_week_date().week() as u32,
            later.iso_week_date().week() as u32,
        );
        // Judge whether it is complete after the last week
        let end_weekday = later.weekday();
        let days_since_monday = end_weekday.to_monday_one_offset() - 1;
        let dur = SignedDuration::from_hours(days_since_monday as i64 * 24);
        let monday_of_end_week = later.checked_sub(dur).unwrap();

        if later < monday_of_end_week {
            weeks -= 1;
        }

        weeks
    }

    pub fn eval_timestamp_between(start: i64, end: i64, tz: TimeZone) -> i64 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_timestamp_between(end, start, tz);
        }

        let earlier = start.to_timestamp(tz.clone());
        let later = end.to_timestamp(tz);

        let mut weeks = Self::calculate_weeks_between_years(
            earlier.year() as i32,
            later.year() as i32,
            earlier.date().iso_week_date().week() as u32,
            later.date().iso_week_date().week() as u32,
        ) as i64;
        // Judge whether it is complete after the last week
        let end_date = later.date();
        let end_weekday = end_date.weekday();
        let days_since_monday = end_weekday.to_monday_one_offset() - 1;
        let dur = SignedDuration::from_hours(days_since_monday as i64 * 24);
        let monday_of_end_week = end_date.checked_sub(dur).unwrap();
        let monday_of_end_week_datetime = monday_of_end_week.at(0, 0, 0, 0);

        if later.datetime() < monday_of_end_week_datetime {
            weeks -= 1;
        }
        weeks
    }
}

pub struct EvalDaysImpl;

impl EvalDaysImpl {
    pub fn eval_date(date: i32, delta: impl AsPrimitive<i64>) -> i32 {
        clamp_date((date as i64).wrapping_add(delta.as_()))
    }

    pub fn eval_date_diff(date_start: i32, date_end: i32) -> i32 {
        date_end - date_start
    }

    pub fn eval_timestamp(date: i64, delta: impl AsPrimitive<i64>) -> i64 {
        let mut value = date.wrapping_add(delta.as_().wrapping_mul(MICROSECS_PER_DAY));
        clamp_timestamp(&mut value);
        value
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64) -> i64 {
        EvalDaysImpl::eval_date_diff(
            (date_start / MICROSECS_PER_DAY) as i32,
            (date_end / MICROSECS_PER_DAY) as i32,
        ) as i64
    }

    pub fn eval_timestamp_between(start: i64, end: i64, tz: TimeZone) -> i64 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_timestamp_between(end, start, tz);
        }

        let start = start.to_timestamp(tz.clone());
        let end = end.to_timestamp(tz);
        let mut full_days = (end.date() - start.date())
            .to_duration(SpanRelativeTo::days_are_24_hours())
            .unwrap()
            .as_hours()
            / 24;
        let end_time = end.time();
        let start_time = start.time();
        if end_time < start_time {
            full_days -= 1;
        }
        full_days
    }
}

pub struct EvalTimesImpl;

impl EvalTimesImpl {
    pub fn eval_date(date: i32, delta: impl AsPrimitive<i64>, factor: i64) -> i32 {
        clamp_date(
            (date as i64 * MICROSECS_PER_DAY)
                .wrapping_add(delta.as_().wrapping_mul(factor * MICROS_PER_SEC)),
        )
    }

    pub fn eval_timestamp(us: i64, delta: impl AsPrimitive<i64>, factor: i64) -> i64 {
        let mut ts = us.wrapping_add(delta.as_().wrapping_mul(factor * MICROS_PER_SEC));
        clamp_timestamp(&mut ts);
        ts
    }

    pub fn eval_timestamp_diff(date_start: i64, date_end: i64, factor: i64) -> i64 {
        let date_start = date_start / (MICROS_PER_SEC * factor);
        let date_end = date_end / (MICROS_PER_SEC * factor);
        date_end - date_start
    }

    pub fn eval_timestamp_between(unit: &str, start: i64, end: i64) -> i64 {
        if start == end {
            return 0;
        }
        if start > end {
            return -Self::eval_timestamp_between(unit, end, start);
        }

        let duration = SignedDuration::from_micros(end - start);

        match unit {
            "hours" => duration.as_hours(),
            "minutes" => duration.as_mins(),
            "seconds" => duration.as_secs(),
            _ => unreachable!("Unsupported unit: {}", unit),
        }
    }
}

#[inline]
pub fn today_date(now: &Zoned, tz: &TimeZone) -> i32 {
    let now = now.with_time_zone(tz.clone());
    now.date()
        .since((Unit::Day, Date::new(1970, 1, 1).unwrap()))
        .unwrap()
        .get_days()
}

// Summer Time in 1990 began at 2 a.m. (Beijing time) on Sunday, April 15th and ended at 2 a.m. (Beijing Daylight Saving Time) on Sunday, September 16th.
// During this period, the summer working hours will be implemented, namely from April 15th to September 16th.
// The working hours of all departments of The State Council are from 8 a.m. to 12 p.m. and from 1:30 p.m. to 5:30 p.m. The winter working hours will be implemented after September 17th.
pub fn calc_date_to_timestamp(val: i32, tz: TimeZone) -> std::result::Result<i64, String> {
    let ts = (val as i64) * 24 * 3600 * MICROS_PER_SEC;
    let z = ts.to_timestamp(tz.clone());

    let tomorrow = z.date().tomorrow();
    let yesterday = z.date().yesterday();

    // If there were no yesterday or tomorrow, it might be the limit value.
    // e.g. 9999-12-31
    if tomorrow.is_err() || yesterday.is_err() {
        let tz_offset_micros = tz
            .to_timestamp(date(1970, 1, 1).at(0, 0, 0, 0))
            .unwrap()
            .as_microsecond();
        return Ok(ts + tz_offset_micros);
    }

    // tomorrow midnight
    let tomorrow_date = tomorrow.map_err(|e| format!("Calc tomorrow midnight with error {}", e))?;

    let tomorrow_zoned = tomorrow_date.to_zoned(tz.clone()).unwrap_or(z.clone());
    let tomorrow_is_dst = tz.to_offset_info(tomorrow_zoned.timestamp()).dst().is_dst();

    // yesterday midnight
    let yesterday_date =
        yesterday.map_err(|e| format!("Calc yesterday midnight with error {}", e))?;
    let yesterday_zoned = yesterday_date.to_zoned(tz.clone()).unwrap_or(z.clone());
    let yesterday_is_std = tz
        .to_offset_info(yesterday_zoned.timestamp())
        .dst()
        .is_std();

    // today midnight
    let today_datetime_midnight = z.date().to_datetime(Time::midnight());
    let today_zoned = if let Some(tz) = tz.iana_name() {
        today_datetime_midnight
            .in_tz(tz)
            .map_err(|e| format!("Calc today midnight with error {}", e))?
    } else {
        return Err("Timezone can not decode as IANA time zone".to_string());
    };
    let today_is_dst = tz.to_offset_info(today_zoned.timestamp()).dst().is_dst();

    let tz_offset_micros = tz
        .to_timestamp(date(1970, 1, 1).at(0, 0, 0, 0))
        .unwrap()
        .as_microsecond();

    let base_res = ts + tz_offset_micros;

    // Origin：(today_is_dst && tomorrow_is_dst && !yesterday_is_std) || (today_is_dst && !tomorrow_is_dst && yesterday_is_std)
    if today_is_dst && (tomorrow_is_dst != yesterday_is_std) {
        Ok(base_res - 3600 * MICROS_PER_SEC)
    } else {
        Ok(base_res)
    }
}

pub trait ToNumber<N> {
    fn to_number(dt: &Zoned) -> N;
}

pub struct ToNumberImpl;

impl ToNumberImpl {
    pub fn eval_timestamp<T: ToNumber<R>, R>(us: i64, tz: TimeZone) -> R {
        let dt = us.to_timestamp(tz);
        T::to_number(&dt)
    }

    pub fn eval_date<T: ToNumber<R>, R>(date: i32, tz: TimeZone) -> Result<R> {
        let dt = date
            .to_date(tz.clone())
            .at(0, 0, 0, 0)
            .to_zoned(tz)
            .unwrap();
        Ok(T::to_number(&dt))
    }
}

pub struct ToYYYYMM;
pub struct ToYYYYWW;
pub struct ToYYYYMMDD;
pub struct ToYYYYMMDDHH;
pub struct ToYYYYMMDDHHMMSS;
pub struct ToYear;
pub struct ToTimezoneHour;
pub struct ToTimezoneMinute;
pub struct ToMillennium;
pub struct ToISOYear;
pub struct ToQuarter;
pub struct ToMonth;
pub struct ToDayOfYear;
pub struct ToDayOfMonth;
pub struct ToDayOfWeek;
pub struct DayOfWeek;
pub struct ToHour;
pub struct ToMinute;
pub struct ToSecond;
pub struct ToUnixTimestamp;

pub struct ToWeekOfYear;

impl ToNumber<u32> for ToYYYYMM {
    fn to_number(dt: &Zoned) -> u32 {
        dt.year() as u32 * 100 + dt.month() as u32
    }
}

impl ToNumber<u16> for ToMillennium {
    fn to_number(dt: &Zoned) -> u16 {
        dt.year() as u16 / 1000 + 1
    }
}

impl ToNumber<u32> for ToWeekOfYear {
    fn to_number(dt: &Zoned) -> u32 {
        dt.date().iso_week_date().week() as u32
    }
}

impl ToNumber<u32> for ToYYYYMMDD {
    fn to_number(dt: &Zoned) -> u32 {
        dt.year() as u32 * 10_000 + dt.month() as u32 * 100 + dt.day() as u32
    }
}

impl ToNumber<u64> for ToYYYYMMDDHH {
    fn to_number(dt: &Zoned) -> u64 {
        dt.year() as u64 * 1_000_000
            + dt.month() as u64 * 10_000
            + dt.day() as u64 * 100
            + dt.hour() as u64
    }
}

impl ToNumber<u64> for ToYYYYMMDDHHMMSS {
    fn to_number(dt: &Zoned) -> u64 {
        dt.year() as u64 * 10_000_000_000
            + dt.month() as u64 * 100_000_000
            + dt.day() as u64 * 1_000_000
            + dt.hour() as u64 * 10_000
            + dt.minute() as u64 * 100
            + dt.second() as u64
    }
}

impl ToNumber<u16> for ToYear {
    fn to_number(dt: &Zoned) -> u16 {
        dt.year() as u16
    }
}

impl ToNumber<i16> for ToTimezoneHour {
    fn to_number(dt: &Zoned) -> i16 {
        dt.offset().seconds().div_ceil(3600) as i16
    }
}

impl ToNumber<i16> for ToTimezoneMinute {
    fn to_number(dt: &Zoned) -> i16 {
        (dt.offset().seconds() % 3600).div_ceil(60) as i16
    }
}

impl ToNumber<u16> for ToISOYear {
    fn to_number(dt: &Zoned) -> u16 {
        dt.date().iso_week_date().year() as _
    }
}

impl ToNumber<u32> for ToYYYYWW {
    fn to_number(dt: &Zoned) -> u32 {
        let week_date = dt.date().iso_week_date();
        let year = week_date.year() as u32 * 100;
        year + dt.date().iso_week_date().week() as u32
    }
}

impl ToNumber<u8> for ToQuarter {
    fn to_number(dt: &Zoned) -> u8 {
        // begin with 0
        ((dt.month() - 1) / 3 + 1) as u8
    }
}

impl ToNumber<u8> for ToMonth {
    fn to_number(dt: &Zoned) -> u8 {
        dt.month() as u8
    }
}

impl ToNumber<u16> for ToDayOfYear {
    fn to_number(dt: &Zoned) -> u16 {
        dt.day_of_year() as u16
    }
}

impl ToNumber<u8> for ToDayOfMonth {
    fn to_number(dt: &Zoned) -> u8 {
        dt.day() as u8
    }
}

impl ToNumber<u8> for ToDayOfWeek {
    fn to_number(dt: &Zoned) -> u8 {
        dt.weekday().to_monday_one_offset() as u8
    }
}

impl ToNumber<u8> for DayOfWeek {
    fn to_number(dt: &Zoned) -> u8 {
        dt.weekday().to_sunday_zero_offset() as u8
    }
}

impl ToNumber<i64> for ToUnixTimestamp {
    fn to_number(dt: &Zoned) -> i64 {
        dt.with_time_zone(TimeZone::UTC).timestamp().as_second()
    }
}

#[derive(Clone, Copy)]
pub enum Round {
    Second,
    Minute,
    FiveMinutes,
    TenMinutes,
    FifteenMinutes,
    TimeSlot,
    Hour,
    Day,
}

pub fn round_timestamp(ts: i64, tz: &TimeZone, round: Round) -> i64 {
    let dtz = ts.to_timestamp(tz.clone());
    let res = match round {
        Round::Second => tz
            .to_zoned(datetime(
                dtz.year(),
                dtz.month(),
                dtz.day(),
                dtz.hour(),
                dtz.minute(),
                dtz.second(),
                0,
            ))
            .unwrap(),
        Round::Minute => tz
            .to_zoned(datetime(
                dtz.year(),
                dtz.month(),
                dtz.day(),
                dtz.hour(),
                dtz.minute(),
                0,
                0,
            ))
            .unwrap(),
        Round::FiveMinutes => tz
            .to_zoned(datetime(
                dtz.year(),
                dtz.month(),
                dtz.day(),
                dtz.hour(),
                dtz.minute() / 5 * 5,
                0,
                0,
            ))
            .unwrap(),
        Round::TenMinutes => tz
            .to_zoned(datetime(
                dtz.year(),
                dtz.month(),
                dtz.day(),
                dtz.hour(),
                dtz.minute() / 10 * 10,
                0,
                0,
            ))
            .unwrap(),
        Round::FifteenMinutes => tz
            .to_zoned(datetime(
                dtz.year(),
                dtz.month(),
                dtz.day(),
                dtz.hour(),
                dtz.minute() / 15 * 15,
                0,
                0,
            ))
            .unwrap(),
        Round::TimeSlot => tz
            .to_zoned(datetime(
                dtz.year(),
                dtz.month(),
                dtz.day(),
                dtz.hour(),
                dtz.minute() / 30 * 30,
                0,
                0,
            ))
            .unwrap(),
        Round::Hour => tz
            .to_zoned(datetime(
                dtz.year(),
                dtz.month(),
                dtz.day(),
                dtz.hour(),
                0,
                0,
                0,
            ))
            .unwrap(),
        Round::Day => tz
            .to_zoned(datetime(dtz.year(), dtz.month(), dtz.day(), 0, 0, 0, 0))
            .unwrap(),
    };
    res.timestamp().as_microsecond()
}

pub struct DateRounder;

impl DateRounder {
    pub fn eval_timestamp<T: ToNumber<i32>>(us: i64, tz: TimeZone) -> i32 {
        let dt = us.to_timestamp(tz);
        T::to_number(&dt)
    }

    pub fn eval_date<T: ToNumber<i32>>(date: i32, tz: TimeZone) -> Result<i32> {
        let naive_dt = date
            .to_date(tz.clone())
            .at(0, 0, 0, 0)
            .to_zoned(tz)
            .unwrap();
        Ok(T::to_number(&naive_dt))
    }
}

/// Convert `jiff::Zoned` to `i32` in `Scalar::Date(i32)` for `DateType`.
///
/// It's the days since 1970-01-01.
#[inline]
fn datetime_to_date_inner_number(date: &Zoned) -> i32 {
    date.date()
        .since((Unit::Day, Date::new(1970, 1, 1).unwrap()))
        .unwrap()
        .get_days()
}

pub struct ToLastMonday;
pub struct ToLastSunday;
pub struct ToStartOfMonth;
pub struct ToStartOfQuarter;
pub struct ToStartOfYear;
pub struct ToStartOfISOYear;

pub struct ToLastOfYear;
pub struct ToLastOfWeek;
pub struct ToLastOfMonth;
pub struct ToLastOfQuarter;
pub struct ToPreviousMonday;
pub struct ToPreviousTuesday;
pub struct ToPreviousWednesday;
pub struct ToPreviousThursday;
pub struct ToPreviousFriday;
pub struct ToPreviousSaturday;
pub struct ToPreviousSunday;
pub struct ToNextMonday;
pub struct ToNextTuesday;
pub struct ToNextWednesday;
pub struct ToNextThursday;
pub struct ToNextFriday;
pub struct ToNextSaturday;
pub struct ToNextSunday;

impl ToNumber<i32> for ToLastMonday {
    fn to_number(dt: &Zoned) -> i32 {
        // datetime_to_date_inner_number just calc naive_date, so weekday also need only calc naive_date
        datetime_to_date_inner_number(dt) - dt.date().weekday().to_monday_zero_offset() as i32
    }
}

impl ToNumber<i32> for ToLastSunday {
    fn to_number(dt: &Zoned) -> i32 {
        // datetime_to_date_inner_number just calc naive_date, so weekday also need only calc naive_date
        datetime_to_date_inner_number(dt) - dt.date().weekday().to_sunday_zero_offset() as i32
    }
}

impl ToNumber<i32> for ToStartOfMonth {
    fn to_number(dt: &Zoned) -> i32 {
        datetime_to_date_inner_number(&dt.first_of_month().unwrap())
    }
}

impl ToNumber<i32> for ToStartOfQuarter {
    fn to_number(dt: &Zoned) -> i32 {
        let new_month = (dt.month() - 1) / 3 * 3 + 1;
        let new_day = date(dt.year(), new_month, 1)
            .at(0, 0, 0, 0)
            .to_zoned(dt.time_zone().clone())
            .unwrap();
        datetime_to_date_inner_number(&new_day)
    }
}

impl ToNumber<i32> for ToStartOfYear {
    fn to_number(dt: &Zoned) -> i32 {
        datetime_to_date_inner_number(&dt.first_of_year().unwrap())
    }
}

impl ToNumber<i32> for ToStartOfISOYear {
    fn to_number(dt: &Zoned) -> i32 {
        let iso_year = dt.date().iso_week_date().year();
        for i in 1..=7 {
            let new_dt = date(iso_year, 1, i)
                .at(0, 0, 0, 0)
                .to_zoned(dt.time_zone().clone())
                .unwrap();
            if new_dt.date().iso_week_date().weekday() == Weekday::Monday {
                return datetime_to_date_inner_number(&new_dt);
            }
        }
        // Never return 0
        0
    }
}

impl ToNumber<i32> for ToLastOfWeek {
    fn to_number(dt: &Zoned) -> i32 {
        datetime_to_date_inner_number(dt) - dt.date().weekday().to_monday_zero_offset() as i32 + 6
    }
}

impl ToNumber<i32> for ToLastOfMonth {
    fn to_number(dt: &Zoned) -> i32 {
        let day = last_day_of_year_month(dt.year(), dt.month());
        let dt = date(dt.year(), dt.month(), day)
            .at(dt.hour(), dt.minute(), dt.second(), dt.subsec_nanosecond())
            .to_zoned(dt.time_zone().clone())
            .unwrap();
        datetime_to_date_inner_number(&dt)
    }
}

impl ToNumber<i32> for ToLastOfQuarter {
    fn to_number(dt: &Zoned) -> i32 {
        let new_month = (dt.month() - 1) / 3 * 3 + 3;
        let day = last_day_of_year_month(dt.year(), new_month);
        let dt = date(dt.year(), new_month, day)
            .at(dt.hour(), dt.minute(), dt.second(), dt.subsec_nanosecond())
            .to_zoned(dt.time_zone().clone())
            .unwrap();
        datetime_to_date_inner_number(&dt)
    }
}

impl ToNumber<i32> for ToLastOfYear {
    fn to_number(dt: &Zoned) -> i32 {
        let day = last_day_of_year_month(dt.year(), 12);
        let dt = date(dt.year(), 12, day)
            .at(dt.hour(), dt.minute(), dt.second(), dt.subsec_nanosecond())
            .to_zoned(dt.time_zone().clone())
            .unwrap();
        datetime_to_date_inner_number(&dt)
    }
}

impl ToNumber<i32> for ToPreviousMonday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Monday, true)
    }
}

impl ToNumber<i32> for ToPreviousTuesday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Tuesday, true)
    }
}

impl ToNumber<i32> for ToPreviousWednesday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Wednesday, true)
    }
}

impl ToNumber<i32> for ToPreviousThursday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Thursday, true)
    }
}

impl ToNumber<i32> for ToPreviousFriday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Friday, true)
    }
}

impl ToNumber<i32> for ToPreviousSaturday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Saturday, true)
    }
}

impl ToNumber<i32> for ToPreviousSunday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Sunday, true)
    }
}

impl ToNumber<i32> for ToNextMonday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Monday, false)
    }
}

impl ToNumber<i32> for ToNextTuesday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Tuesday, false)
    }
}

impl ToNumber<i32> for ToNextWednesday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Wednesday, false)
    }
}

impl ToNumber<i32> for ToNextThursday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Thursday, false)
    }
}

impl ToNumber<i32> for ToNextFriday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Friday, false)
    }
}

impl ToNumber<i32> for ToNextSaturday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Saturday, false)
    }
}

impl ToNumber<i32> for ToNextSunday {
    fn to_number(dt: &Zoned) -> i32 {
        previous_or_next_day(dt, Weekday::Sunday, false)
    }
}

pub fn previous_or_next_day(dt: &Zoned, target: Weekday, is_previous: bool) -> i32 {
    let dir = if is_previous { -1 } else { 1 };
    let mut days_diff = (dir
        * (target.to_monday_zero_offset() as i32
            - dt.date().weekday().to_monday_zero_offset() as i32)
        + 7)
        % 7;

    days_diff = if days_diff == 0 { 7 } else { days_diff };

    datetime_to_date_inner_number(dt) + dir * days_diff
}

/// PostgreSQL to strftime format specifier mappings
///
/// The vector contains tuples of (postgres_format, strftime_format):
/// - For case-insensitive PostgreSQL formats (e.g., "YYYY"), any case variation will match
/// - For case-sensitive strftime formats (prefixed with '%'), exact case matching is required
///
/// Note: The sort order (by descending key length) is critical for correct pattern matching
static PG_STRFTIME_MAPPINGS: LazyLock<Vec<(&'static str, &'static str)>> = LazyLock::new(|| {
    let mut mappings = vec![
        // ==============================================
        // Case-insensitive PostgreSQL format specifiers
        // (will match regardless of letter case)
        // ==============================================
        // Date components
        ("YYYY", "%Y"), // 4-digit year
        ("YY", "%y"),   // 2-digit year
        ("MMMM", "%B"), // Full month name
        ("MON", "%b"),  // Abbreviated month name (special word boundary handling)
        ("MM", "%m"),   // Month number (01-12)
        ("DD", "%d"),   // Day of month (01-31)
        ("DY", "%a"),   // Abbreviated weekday name
        // Time components
        ("HH24", "%H"), // 24-hour format (00-23)
        ("HH12", "%I"), // 12-hour format (01-12)
        ("AM", "%p"),   // AM/PM indicator (matches both AM/PM)
        ("PM", "%p"),   // AM/PM indicator (matches both AM/PM)
        ("MI", "%M"),   // Minutes (00-59)
        ("SS", "%S"),   // Seconds (00-59)
        ("FF", "%f"),   // Fractional seconds
        // Special cases
        ("UUUU", "%G"),    // ISO week-numbering year
        ("TZHTZM", "%z"),  // Timezone as ±HHMM
        ("TZH:TZM", "%z"), // Timezone as ±HH:MM
        ("TZH", "%:::z"),  // Timezone hour only
        // ==============================================
        // Case-sensitive strftime format specifiers
        // (must match exactly including case)
        // ==============================================
        ("%Y", "%Y"), // Year aliases
        ("%y", "%y"),
        ("%B", "%B"), // Month aliases
        ("%b", "%b"),
        ("%m", "%m"),
        ("%d", "%d"), // Day aliases
        ("%a", "%a"), // Weekday alias
        ("%H", "%H"), // Hour aliases
        ("%I", "%I"),
        ("%p", "%p"),       // AM/PM indicator
        ("%M", "%M"),       // Minute alias
        ("%S", "%S"),       // Second alias
        ("%f", "%f"),       // Fractional second alias
        ("%G", "%G"),       // ISO year alias
        ("%z", "%z"),       // Timezone aliases
        ("%:::z", "%:::z"), // Timezone hour alias
    ];

    // Critical: Sort by descending key length to ensure longest possible matches are found first
    // This prevents shorter patterns from incorrectly matching parts of longer patterns
    mappings.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
    mappings
});

static PG_KEY_LENGTHS: LazyLock<Vec<usize>> =
    LazyLock::new(|| PG_STRFTIME_MAPPINGS.iter().map(|(k, _)| k.len()).collect());

fn starts_with_ignore_case(text: &str, prefix: &str) -> bool {
    if text.len() < prefix.len() {
        return false;
    }
    text.chars()
        .zip(prefix.chars())
        .all(|(c1, c2)| c1.to_lowercase().eq(c2.to_lowercase()))
}

fn is_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

#[inline]
pub fn pg_format_to_strftime(pg_format_string: &str) -> String {
    let mut result = String::with_capacity(pg_format_string.len() + 16);
    let mut current_byte_idx = 0;
    let format_len = pg_format_string.len();

    while current_byte_idx < format_len {
        let remaining_slice = &pg_format_string[current_byte_idx..];
        let mut matched = false;
        let first_char = remaining_slice.chars().next().unwrap_or('\0');

        for ((key, value), &key_len) in PG_STRFTIME_MAPPINGS.iter().zip(PG_KEY_LENGTHS.iter()) {
            if !key.is_empty() && !first_char.eq_ignore_ascii_case(&key.chars().next().unwrap()) {
                continue;
            }

            let is_case_sensitive_key = key.starts_with('%');
            let is_current_match = if is_case_sensitive_key {
                remaining_slice.starts_with(key)
            } else {
                starts_with_ignore_case(remaining_slice, key)
            };

            if is_current_match {
                let mut is_valid_match = true;
                if !is_case_sensitive_key && key.eq_ignore_ascii_case("MON") {
                    let next_byte_idx = current_byte_idx + key_len;

                    if current_byte_idx > 0 {
                        if let Some(prev_char) =
                            pg_format_string[..current_byte_idx].chars().next_back()
                        {
                            if is_word_char(prev_char) {
                                is_valid_match = false;
                            }
                        }
                    }

                    if is_valid_match && next_byte_idx < format_len {
                        if let Some(next_char) = pg_format_string[next_byte_idx..].chars().next() {
                            if is_word_char(next_char) {
                                is_valid_match = false;
                            }
                        }
                    }
                }

                if is_valid_match {
                    result.push_str(value);
                    current_byte_idx += key_len;
                    matched = true;
                    break;
                }
            }
        }

        if !matched {
            let c = first_char;
            result.push(c);
            current_byte_idx += c.len_utf8();
        }
    }

    result
}
