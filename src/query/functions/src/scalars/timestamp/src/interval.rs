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

use std::io::Write;

use databend_common_column::types::months_days_micros;
use databend_common_expression::date_helper::calc_date_to_timestamp;
use databend_common_expression::date_helper::today_date;
use databend_common_expression::date_helper::DateConverter;
use databend_common_expression::date_helper::EvalMonthsImpl;
use databend_common_expression::error_to_null;
use databend_common_expression::types::interval::interval_to_string;
use databend_common_expression::types::interval::string_to_interval;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::IntervalType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use jiff::Zoned;

pub fn register(registry: &mut FunctionRegistry) {
    // cast(xx AS interval)
    // to_interval(xx)
    register_string_to_interval(registry);
    register_interval_to_string(registry);
    // data/timestamp/interval +/- interval
    register_interval_add_sub_mul(registry);
    register_number_to_interval(registry);
}

fn register_string_to_interval(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, IntervalType, _, _>(
        "to_interval",
        |_, _| FunctionDomain::MayThrow,
        eval_string_to_interval,
    );
    registry.register_combine_nullable_1_arg::<StringType, IntervalType, _, _>(
        "try_to_interval",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_string_to_interval),
    );

    fn eval_string_to_interval(
        val: Value<StringType>,
        ctx: &mut EvalContext,
    ) -> Value<IntervalType> {
        vectorize_with_builder_1_arg::<StringType, IntervalType>(|val, output, ctx| {
            match string_to_interval(val) {
                Ok(interval) => output.push(months_days_micros::new(
                    interval.months,
                    interval.days,
                    interval.micros,
                )),
                Err(e) => {
                    ctx.set_error(
                        output.len(),
                        format!("cannot parse to type `INTERVAL`. {}", e),
                    );
                    output.push(months_days_micros::new(0, 0, 0));
                }
            }
        })(val, ctx)
    }
}

fn register_interval_to_string(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<IntervalType, StringType, _, _>(
        "to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<IntervalType, StringType>(|interval, output, _| {
            write!(output.row_buffer, "{}", interval_to_string(&interval)).unwrap();
            output.commit_row();
        }),
    );
}

fn register_interval_add_sub_mul(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg::<IntervalType, IntervalType, IntervalType, _, _>(
        "plus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<IntervalType, IntervalType, IntervalType>(
            |a, b, output, _| {
                output.push(months_days_micros::new(
                    a.months() + b.months(),
                    a.days() + b.days(),
                    a.microseconds() + b.microseconds(),
                ))
            },
        ),
    );

    registry
        .register_passthrough_nullable_2_arg::<TimestampType, IntervalType, TimestampType, _, _>(
            "plus",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<TimestampType, IntervalType, TimestampType>(
                |a, b, output, ctx| {
                    // plus microseconds and days
                    let ts = a
                        .wrapping_add(b.microseconds())
                        .wrapping_add((b.days() as i64).wrapping_mul(86_400_000_000));
                    match EvalMonthsImpl::eval_timestamp(
                        ts,
                        ctx.func_ctx.tz.clone(),
                        b.months(),
                        false,
                    ) {
                        Ok(t) => output.push(t),
                        Err(e) => {
                            ctx.set_error(output.len(), e);
                            output.push(0);
                        }
                    }
                },
            ),
        );

    registry
        .register_passthrough_nullable_2_arg::<IntervalType, TimestampType, TimestampType, _, _>(
            "plus",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<IntervalType, TimestampType, TimestampType>(
                |b, a, output, ctx| {
                    // plus microseconds and days
                    let ts = a
                        .wrapping_add(b.microseconds())
                        .wrapping_add((b.days() as i64).wrapping_mul(86_400_000_000));
                    match EvalMonthsImpl::eval_timestamp(
                        ts,
                        ctx.func_ctx.tz.clone(),
                        b.months(),
                        false,
                    ) {
                        Ok(t) => output.push(t),
                        Err(e) => {
                            ctx.set_error(output.len(), e);
                            output.push(0);
                        }
                    }
                },
            ),
        );

    registry.register_passthrough_nullable_2_arg::<IntervalType, IntervalType, IntervalType, _, _>(
        "minus",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<IntervalType, IntervalType, IntervalType>(
            |a, b, output, _| {
                output.push(months_days_micros::new(
                    a.months() - b.months(),
                    a.days() - b.days(),
                    a.microseconds() - b.microseconds(),
                ));
            },
        ),
    );

    registry
        .register_passthrough_nullable_2_arg::<TimestampType, IntervalType, TimestampType, _, _>(
            "minus",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<TimestampType, IntervalType, TimestampType>(
                |a, b, output, ctx| {
                    // plus microseconds and days
                    let ts = a
                        .wrapping_sub(b.microseconds())
                        .wrapping_sub((b.days() as i64).wrapping_mul(86_400_000_000));
                    match EvalMonthsImpl::eval_timestamp(
                        ts,
                        ctx.func_ctx.tz.clone(),
                        -b.months(),
                        false,
                    ) {
                        Ok(t) => output.push(t),
                        Err(e) => {
                            ctx.set_error(output.len(), e);
                            output.push(0);
                        }
                    }
                },
            ),
        );

    registry
        .register_passthrough_nullable_2_arg::<TimestampType, TimestampType, IntervalType, _, _>(
            "age",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<TimestampType, TimestampType, IntervalType>(
                |t1, t2, output, ctx| {
                    let mut is_negative = false;
                    let mut t1 = t1;
                    let mut t2 = t2;
                    if t1 < t2 {
                        std::mem::swap(&mut t1, &mut t2);
                        is_negative = true;
                    }
                    let tz = &ctx.func_ctx.tz;
                    let t1 = t1.to_timestamp(tz.clone());
                    let t2 = t2.to_timestamp(tz.clone());
                    output.push(calc_age(t1, t2, is_negative));
                },
            ),
        );

    // age(ts) == age(now() at midnight, ts);
    registry.register_passthrough_nullable_1_arg::<TimestampType, IntervalType, _, _>(
        "age",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<TimestampType, IntervalType>(|t2, output, ctx| {
            let mut is_negative = false;
            let tz = &ctx.func_ctx.tz;

            let today_date = today_date(&ctx.func_ctx.now, &ctx.func_ctx.tz);
            match calc_date_to_timestamp(today_date, tz.clone()) {
                Ok(t) => {
                    let mut t1 = t.to_timestamp(tz.clone());
                    let mut t2 = t2.to_timestamp(tz.clone());

                    if t1 < t2 {
                        std::mem::swap(&mut t1, &mut t2);
                        is_negative = true;
                    }
                    output.push(calc_age(t1, t2, is_negative));
                }
                Err(e) => {
                    ctx.set_error(output.len(), e);
                    output.push(months_days_micros::new(0, 0, 0));
                }
            }
        }),
    );

    registry.register_passthrough_nullable_2_arg::<Int64Type, IntervalType, IntervalType, _, _>(
        "multiply",
        |_, _, _| FunctionDomain::Full,
        vectorize_2_arg::<Int64Type, IntervalType, IntervalType>(|a, b, _ctx| {
            months_days_micros::new(
                b.months() * (a as i32),
                b.days() * (a as i32),
                b.microseconds() * a,
            )
        }),
    );

    registry.register_passthrough_nullable_2_arg::<IntervalType, Int64Type, IntervalType, _, _>(
        "multiply",
        |_, _, _| FunctionDomain::Full,
        vectorize_2_arg::<IntervalType, Int64Type, IntervalType>(|b, a, _ctx| {
            months_days_micros::new(
                b.months() * (a as i32),
                b.days() * (a as i32),
                b.microseconds() * a,
            )
        }),
    );
}

fn register_number_to_interval(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_centuries",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new((val * 100 * 12) as i32, 0, 0);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_days",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(0, val as i32, 0);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_weeks",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(0, (val * 7) as i32, 0);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_decades",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new((val * 10 * 12) as i32, 0, 0);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_hours",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(0, 0, val * 3600 * 1_000_000);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_microseconds",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(0, 0, val);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_millennia",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new((val * 1000 * 12) as i32, 0, 0);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_milliseconds",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(0, 0, val * 1000);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_minutes",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(0, 0, val * 60 * 1_000_000);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_months",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(val as i32, 0, 0);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_seconds",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new(0, 0, val * 1_000_000);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, IntervalType, _, _>(
        "to_years",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, IntervalType>(|val, output, _| {
            let res = months_days_micros::new((val * 12) as i32, 0, 0);
            output.push(res);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<IntervalType, Int64Type, _, _>(
        "to_year",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<IntervalType, Int64Type>(|val, output, _| {
            output.push(val.months() as i64 / 12);
        }),
    );
    registry.register_passthrough_nullable_1_arg::<IntervalType, Int64Type, _, _>(
        "to_month",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<IntervalType, Int64Type>(|val, output, _| {
            output.push(val.months() as i64 % 12);
        }),
    );
    // Directly return interval days. Extract need named to_day_of_month
    registry.register_passthrough_nullable_1_arg::<IntervalType, Int64Type, _, _>(
        "to_day_of_month",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<IntervalType, Int64Type>(|val, output, _| {
            output.push(val.days() as i64);
        }),
    );
    registry.register_passthrough_nullable_1_arg::<IntervalType, Int64Type, _, _>(
        "to_hour",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<IntervalType, Int64Type>(|val, output, _| {
            let total_seconds = (val.microseconds() as f64) / 1_000_000.0;
            let hours = (total_seconds / 3600.0) as i64;
            output.push(hours);
        }),
    );
    registry.register_passthrough_nullable_1_arg::<IntervalType, Int64Type, _, _>(
        "to_minute",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<IntervalType, Int64Type>(|val, output, _| {
            let total_seconds = (val.microseconds() as f64) / 1_000_000.0;
            let minutes = ((total_seconds % 3600.0) / 60.0) as i64;
            output.push(minutes);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<IntervalType, Float64Type, _, _>(
        "to_second",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<IntervalType, Float64Type>(|val, output, _| {
            let microseconds = val.microseconds() % 60_000_000;
            let seconds = microseconds as f64 / 1_000_000.0;
            output.push(seconds.into());
        }),
    );

    registry.register_passthrough_nullable_1_arg::<IntervalType, Int64Type, _, _>(
        "to_microsecond",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<IntervalType, Int64Type>(|val, output, _| {
            let microseconds = val.microseconds() % 60_000_000;
            output.push(microseconds);
        }),
    );
    registry.register_passthrough_nullable_1_arg::<IntervalType, Float64Type, _, _>(
        "epoch",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<IntervalType, Float64Type>(|val, output, _| {
            let total_seconds = (val.total_micros() as f64) / 1_000_000.0;
            output.push(total_seconds.into());
        }),
    );
}

fn calc_age(t1: Zoned, t2: Zoned, is_negative: bool) -> months_days_micros {
    let mut years = t1.year() - t2.year();
    let mut months = t1.month() - t2.month();
    let mut days = t1.day() - t2.day();

    let t1_total_nanos = (t1.hour() as i64 * 3600 + t1.minute() as i64 * 60 + t1.second() as i64)
        * 1_000_000_000
        + t1.subsec_nanosecond() as i64;
    let t2_total_nanos = (t2.hour() as i64 * 3600 + t2.minute() as i64 * 60 + t2.second() as i64)
        * 1_000_000_000
        + t2.subsec_nanosecond() as i64;
    let mut total_nanoseconds_diff = t1_total_nanos - t2_total_nanos;

    if total_nanoseconds_diff < 0 {
        total_nanoseconds_diff += 24 * 3600 * 1_000_000_000;
        days -= 1;
    }

    if days < 0 {
        let days_in_month_of_t2 = t2.date().days_in_month();
        days += days_in_month_of_t2;
        months -= 1;
    }

    if months < 0 {
        months += 12;
        years -= 1;
    }

    let total_months = months as i32 + (years as i32 * 12);

    if is_negative {
        months_days_micros::new(-total_months, -days as i32, -total_nanoseconds_diff / 1000)
    } else {
        months_days_micros::new(total_months, days as i32, total_nanoseconds_diff / 1000)
    }
}
