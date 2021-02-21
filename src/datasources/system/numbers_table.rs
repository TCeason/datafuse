// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::mem::size_of;
use std::sync::Arc;

use async_trait::async_trait;

use crate::datasources::{system::NumbersStream, ITable, Partition, Partitions, Statistics};
use crate::datastreams::SendableDataBlockStream;
use crate::datavalues::{DataField, DataSchema, DataSchemaRef, DataType, DataValue};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::{ExpressionPlan, PlanNode, ReadDataSourcePlan, ScanPlan};
use crate::sessions::FuseQueryContextRef;

pub struct NumbersTable {
    table: &'static str,
    schema: DataSchemaRef,
}

impl NumbersTable {
    pub fn create(table: &'static str) -> Self {
        NumbersTable {
            table,
            schema: Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                DataType::UInt64,
                false,
            )])),
        }
    }

    pub fn generate_parts(&self, workers: u64, total: u64) -> Partitions {
        let part_size = total / workers;
        let part_remain = total % workers;

        let mut partitions = Vec::with_capacity(workers as usize);
        if part_size == 0 {
            partitions.push(Partition {
                name: format!("{}-{}-{}", total, 0, total,),
                version: 0,
            })
        } else {
            for part in 0..workers {
                let part_begin = part * part_size;
                let mut part_end = (part + 1) * part_size;
                if part == (workers - 1) && part_remain > 0 {
                    part_end += part_remain;
                }
                partitions.push(Partition {
                    name: format!("{}-{}-{}", total, part_begin, part_end,),
                    version: 0,
                })
            }
        }
        partitions
    }
}

#[async_trait]
impl ITable for NumbersTable {
    fn name(&self) -> &str {
        self.table
    }

    fn schema(&self) -> FuseQueryResult<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn read_plan(
        &self,
        ctx: FuseQueryContextRef,
        push_down_plan: PlanNode,
    ) -> FuseQueryResult<ReadDataSourcePlan> {
        let mut total = ctx.get_max_block_size()? as u64;

        if let PlanNode::Scan(plan) = push_down_plan {
            let ScanPlan { table_args, .. } = plan;
            if let Some(args) = table_args {
                if let ExpressionPlan::Constant(DataValue::UInt64(Some(v))) = args {
                    total = v;
                }

                if let ExpressionPlan::Constant(DataValue::Int64(Some(v))) = args {
                    total = v as u64;
                }
            } else {
                return Err(FuseQueryError::Internal(format!(
                    "Must have one argument for table: system.{}",
                    self.name()
                )));
            }
        }

        let statistics = Statistics {
            read_rows: total as usize,
            read_bytes: (total) * size_of::<u64>() as u64,
        };
        ctx.try_set_statistics(&statistics)?;

        Ok(ReadDataSourcePlan {
            db: "system".to_string(),
            table: self.name().to_string(),
            schema: self.schema.clone(),
            partitions: self.generate_parts(ctx.get_max_threads()?, total),
            statistics: statistics.clone(),
            description: format!(
                "(Read from system.{} table, Read Rows:{}, Read Bytes:{})",
                self.table, statistics.read_rows, statistics.read_bytes
            ),
        })
    }

    async fn read(&self, ctx: FuseQueryContextRef) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(Box::pin(NumbersStream::create(ctx, self.schema.clone())))
    }
}
