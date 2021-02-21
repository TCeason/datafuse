// Copyright 2020 The VectorQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use criterion::{criterion_group, criterion_main, Criterion};
use futures::stream::StreamExt;

use fuse_query::error::FuseQueryResult;
use fuse_query::interpreters::SelectInterpreter;
use fuse_query::planners::PlanNode;
use fuse_query::sessions::FuseQueryContext;
use fuse_query::sql::PlanParser;

async fn pipeline_executor(sql: &str) -> FuseQueryResult<()> {
    let ctx = FuseQueryContext::try_create()?;

    if let PlanNode::Select(plan) = PlanParser::create(ctx.clone()).build_from_sql(sql)? {
        let executor = SelectInterpreter::try_create(ctx, plan)?;
        let mut stream = executor.execute().await?;
        while let Some(_block) = stream.next().await {}
    } else {
        assert!(false)
    }
    Ok(())
}

fn criterion_benchmark_suite(c: &mut Criterion, sql: &str) {
    c.bench_function(format!("{}", sql).as_str(), |b| {
        b.iter(|| {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(pipeline_executor(sql))
        })
    });
}

fn criterion_benchmark_memory_table_processor(c: &mut Criterion) {
    criterion_benchmark_suite(
        c,
        "select number from system.numbers_mt(1000000) where number < 4 limit 10",
    );
    criterion_benchmark_suite(c, "select number as a, number/2 as b, number+1 as c from system.numbers_mt(1000000) where number < 4 limit 10");
    criterion_benchmark_suite(
        c,
        "select sum(number), max(number) from system.numbers_mt(1000000)",
    );
    criterion_benchmark_suite(c, "select sum(number+1) from system.numbers_mt(10000000)");
}

criterion_group!(benches, criterion_benchmark_memory_table_processor,);
criterion_main!(benches);
