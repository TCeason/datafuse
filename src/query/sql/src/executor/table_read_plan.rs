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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_ast::ast;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Identifier;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_base::base::ProgressValues;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::StreamTablePart;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::FieldIndex;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_license::license::Feature::DataMask;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_users::UserApiProvider;
use databend_enterprise_data_mask_feature::get_datamask_handler;
use log::info;
use parking_lot::RwLock;

use crate::BindContext;
use crate::Metadata;
use crate::NameResolutionContext;
use crate::TypeChecker;

#[async_trait::async_trait]
pub trait ToReadDataSourcePlan {
    async fn read_plan(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        internal_columns: Option<BTreeMap<FieldIndex, InternalColumn>>,
        update_stream_columns: bool,
        dry_run: bool,
    ) -> Result<DataSourcePlan>;
}

#[async_trait::async_trait]
impl ToReadDataSourcePlan for dyn Table {
    #[async_backtrace::framed]
    async fn read_plan(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        internal_columns: Option<BTreeMap<FieldIndex, InternalColumn>>,
        update_stream_columns: bool,
        dry_run: bool,
    ) -> Result<DataSourcePlan> {
        let start = std::time::Instant::now();

        let (statistics, mut parts) = if let Some(PushDownInfo {
            filters:
                Some(Filters {
                    filter:
                        RemoteExpr::Constant {
                            scalar: Scalar::Boolean(false),
                            ..
                        },
                    ..
                }),
            ..
        }) = &push_downs
        {
            Ok((PartStatistics::default(), Partitions::default()))
        } else {
            ctx.set_status_info("[TABLE-SCAN] Reading table partitions");
            self.read_partitions(ctx.clone(), push_downs.clone(), dry_run)
                .await
        }?;

        let mut base_block_ids = None;
        if parts.partitions.len() == 1 {
            let part = parts.partitions[0].clone();
            if let Some(part) = StreamTablePart::from_part(&part) {
                parts = part.inner();
                base_block_ids = Some(part.base_block_ids());
            }
        }

        ctx.set_status_info(&format!(
            "[TABLE-SCAN] Partitions loaded, elapsed: {:?}",
            start.elapsed()
        ));

        ctx.incr_total_scan_value(ProgressValues {
            rows: statistics.read_rows,
            bytes: statistics.read_bytes,
        });

        // We need the partition sha256 to specify the result cache.
        let settings = ctx.get_settings();
        if settings.get_enable_query_result_cache()? {
            let sha = parts.compute_sha256()?;
            ctx.add_partitions_sha(sha);
        }

        let source_info = self.get_data_source_info();
        let description = statistics.get_description(&source_info.desc());
        let mut output_schema = match (self.support_column_projection(), &push_downs) {
            (true, Some(push_downs)) => {
                let schema = &self.schema_with_stream();
                match &push_downs.prewhere {
                    Some(prewhere) => Arc::new(prewhere.output_columns.project_schema(schema)),
                    _ => {
                        if let Some(output_columns) = &push_downs.output_columns {
                            Arc::new(output_columns.project_schema(schema))
                        } else if let Some(projection) = &push_downs.projection {
                            Arc::new(projection.project_schema(schema))
                        } else {
                            schema.clone()
                        }
                    }
                }
            }
            _ => self.schema(),
        };

        if let Some(ref push_downs) = push_downs {
            if let Some(ref virtual_column) = push_downs.virtual_column {
                let mut schema = output_schema.as_ref().clone();
                for field in &virtual_column.virtual_column_fields {
                    schema.add_internal_field(
                        &field.name,
                        *field.data_type.clone(),
                        field.column_id,
                    );
                }
                output_schema = Arc::new(schema);
            }
        }

        if let Some(ref internal_columns) = internal_columns {
            let mut schema = output_schema.as_ref().clone();
            for internal_column in internal_columns.values() {
                schema.add_internal_field(
                    internal_column.column_name(),
                    internal_column.table_data_type(),
                    internal_column.column_id(),
                );
            }
            output_schema = Arc::new(schema);
        }

        // check if need to apply data mask policy
        let data_mask_policy = if let DataSourceInfo::TableSource(table_info) = &source_info {
            let table_meta = &table_info.meta;
            let tenant = ctx.get_tenant();

            if !table_meta.column_mask_policy_columns_ids.is_empty() {
                let column_mask_policy = &table_meta.column_mask_policy_columns_ids;
                println!("column_mask_policy: {:?}", column_mask_policy.clone());
                if LicenseManagerSwitch::instance()
                    .check_enterprise_enabled(ctx.get_license_key(), DataMask)
                    .is_err()
                {
                    None
                } else {
                    let mut mask_policy_map = BTreeMap::new();
                    let meta_api = UserApiProvider::instance().get_meta_store_client();
                    let handler = get_datamask_handler();

                    // Pre-build bind_context with all columns to avoid rebuilding in the loop
                    let mut bind_context = BindContext::new();
                    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
                    let metadata = Arc::new(RwLock::new(Metadata::default()));

                    use crate::binder::ColumnBindingBuilder;
                    use crate::Visibility;
                    // Build bind_context using table schema for masking policy evaluation
                    for (col_idx, schema_field) in table_meta.schema.fields().iter().enumerate() {
                        let column_binding = ColumnBindingBuilder::new(
                            schema_field.name.clone(),
                            col_idx,
                            Box::new((&schema_field.data_type).into()),
                            Visibility::Visible,
                        )
                        .build();
                        bind_context.add_column_binding(column_binding);
                    }

                    // First, collect unique policies to avoid duplicate processing
                    let mut unique_policies = BTreeMap::new();
                    for policy in column_mask_policy.values() {
                        unique_policies.insert(policy.policy_id, policy);
                    }

                    // Process each unique policy
                    for (policy_id, policy_info) in unique_policies {
                        ctx.set_status_info(&format!(
                            "[TABLE-SCAN] Loading data mask policies, elapsed: {:?}",
                            start.elapsed()
                        ));
                        println!("policy is {:?}", policy_info.clone());

                        if let Ok(policy) = handler
                            .get_data_mask_by_id(meta_api.clone(), &tenant, policy_id)
                            .await
                        {
                            println!("policy body is {:?}", policy.body.clone());
                            let policy = policy.data;
                            let args = &policy.args;

                            // Map parameters to correct columns based on USING clause
                            let using_columns = &policy_info.columns_ids;
                            let arguments: Vec<Expr> = args
                                .iter()
                                .enumerate()
                                .map(|(param_idx, _)| {
                                    if let Some(&column_id) = using_columns.get(param_idx) {
                                        // Find the field name by column_id
                                        let field_name = table_meta.schema.fields()
                                            .iter()
                                            .find(|f| f.column_id == column_id)
                                            .map(|f| f.name.clone())
                                            .unwrap_or_else(|| {
                                                // This shouldn't happen, but provide a fallback
                                                format!("column_{}", column_id)
                                            });

                                        Expr::ColumnRef {
                                            span: None,
                                            column: ColumnRef {
                                                database: None,
                                                table: None,
                                                column: ast::ColumnID::Name(Identifier::from_name(None, field_name)),
                                            },
                                        }
                                    } else {
                                        // This shouldn't happen either - policy should have enough columns
                                        panic!("Policy {} doesn't have enough columns for parameter {}", policy_id, param_idx);
                                    }
                                })
                                .collect();

                            let body = &policy.body;
                            let tokens = tokenize_sql(body)?;
                            let ast_expr = parse_expr(&tokens, settings.get_sql_dialect()?)?;

                            let parameters =
                                args.iter().map(|arg| arg.0.to_string()).collect::<Vec<_>>();
                            let mut args_map = HashMap::with_capacity(parameters.len());

                            arguments.iter().enumerate().for_each(|(idx, argument)| {
                                if let Some(parameter) = parameters.get(idx) {
                                    args_map.insert(parameter.as_str(), (*argument).clone());
                                }
                            });

                            println!("ast_expr before replacement: {:?}", ast_expr);
                            println!("args_map: {:?}", args_map);
                            let expr =
                                TypeChecker::clone_expr_with_replacement(&ast_expr, |nest_expr| {
                                    if let Expr::ColumnRef { column, .. } = nest_expr {
                                        if let Some(arg) = args_map.get(column.column.name()) {
                                            return Ok(Some(arg.clone()));
                                        }
                                    }
                                    Ok(None)
                                })?;
                            println!("expr after replacement: {:?}", expr);

                            let mut type_checker = TypeChecker::try_create(
                                &mut bind_context,
                                ctx.clone(),
                                &name_resolution_ctx,
                                metadata.clone(),
                                &[],
                                false,
                            )?;

                            ctx.set_status_info(&format!(
                                "[TABLE-SCAN] Resolving mask expressions, elapsed: {:?}",
                                start.elapsed()
                            ));
                            let scalar = type_checker.resolve(&expr)?;
                            println!("scalar is {:?}", scalar.clone());
                            println!("policy_id = {}", policy_id);
                            // Create a specialized expression for each masked column
                            // We need to map the masked column reference to the current column index
                            // while keeping other column references intact
                            let masked_column_id = policy_info.columns_ids[0]; // First column in USING clause is the masked column
                            let expr = scalar.0.as_expr()?;

                            // Apply this policy to all columns that use it
                            for (field_idx, field) in output_schema.fields().iter().enumerate() {
                                if let Some(policy) = column_mask_policy.get(&field.column_id) {
                                    if policy.policy_id == policy_id {
                                        // Create a specialized expression for this specific column
                                        // Map table schema column indices to output schema column indices
                                        let specialized_expr = expr.project_column_ref(|col| {
                                            // Find which column this binding refers to in table schema
                                            let binding_column_id = table_meta
                                                .schema
                                                .fields()
                                                .get(col.index)
                                                .map(|f| f.column_id)
                                                .unwrap_or(0);

                                            if binding_column_id == masked_column_id {
                                                // This is the masked column reference, map it to current field_idx in output_schema
                                                Ok(field_idx)
                                            } else {
                                                // This is a condition column, find its index in output_schema
                                                if let Some(output_index) = output_schema
                                                    .fields()
                                                    .iter()
                                                    .position(|f| f.column_id == binding_column_id)
                                                {
                                                    Ok(output_index)
                                                } else {
                                                    // Condition column not in output_schema, but that's OK -
                                                    // the execution engine will handle this appropriately
                                                    // by failing at runtime when trying to access the column
                                                    Ok(col.index) // Keep original index as fallback
                                                }
                                            }
                                        })?;
                                        mask_policy_map
                                            .insert(field_idx, specialized_expr.as_remote_expr());
                                    }
                                }
                            }
                        } else {
                            info!(
                                "[TABLE-SCAN] Mask policy not found: {}/{}",
                                tenant.display(),
                                policy_id
                            );
                        }
                    }
                    Some(mask_policy_map)
                }
            } else {
                None
            }
        } else {
            None
        };

        ctx.set_status_info(&format!(
            "[TABLE-SCAN] Scan plan ready, elapsed: {:?}",
            start.elapsed()
        ));

        Ok(DataSourcePlan {
            source_info,
            output_schema,
            parts,
            statistics,
            description,
            tbl_args: self.table_args(),
            push_downs,
            internal_columns,
            base_block_ids,
            update_stream_columns,
            data_mask_policy,
            // Set a dummy id, will be set real id later
            table_index: usize::MAX,
            scan_id: usize::MAX,
        })
    }
}
