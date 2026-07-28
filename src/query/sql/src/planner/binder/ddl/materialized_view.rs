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

use databend_common_ast::Span;
use databend_common_ast::ast::CreateMaterializedViewStmt;
use databend_common_ast::ast::CreateOption as AstCreateOption;
use databend_common_ast::ast::DropMaterializedViewStmt;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Pivot;
use databend_common_ast::ast::PivotValues;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::ShowCreateMaterializedViewStmt;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::ShowMaterializedViewsStmt;
use databend_common_ast::ast::TableRef;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::quote::QuotedIdent;
use databend_common_ast::ast::quote::QuotedString;
use databend_common_ast::ast::quote::ident_opt_quote;
use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_meta_app::schema::OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID;
use databend_common_meta_app::schema::is_materialized_view_engine;
use databend_storages_common_table_meta::table::OPT_KEY_AGGRESSIVE_RECLUSTER;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::is_fuse_engine;
use derive_visitor::Drive;
use derive_visitor::DriveMut;
use derive_visitor::Visitor;
use derive_visitor::VisitorMut;
use log::debug;

use crate::BindContext;
use crate::NameResolutionContext;
use crate::SelectBuilder;
use crate::binder::Binder;
use crate::planner::binder::ddl::table::apply_fuse_storage_defaults;
use crate::planner::semantic::normalize_identifier;
use crate::plans::CreateMaterializedViewPlan;
use crate::plans::CreateTablePlan;
use crate::plans::DropMaterializedViewPlan;
use crate::plans::Plan;
use crate::plans::RewriteKind;
use crate::plans::ShowCreateMaterializedViewPlan;

#[derive(VisitorMut)]
#[visitor(Query, TableRef(enter))]
struct TableRefQualifier {
    catalog: Identifier,
    database: Identifier,
    name_resolution_ctx: NameResolutionContext,
    cte_scope_stack: Vec<Vec<String>>,
}

#[derive(Default, Visitor)]
#[visitor(Query(enter))]
struct MaterializedCteDetector {
    found: bool,
}

#[derive(Default, Visitor)]
#[visitor(Pivot(enter))]
struct ExecutorDependentPivotDetector {
    found: bool,
}

#[derive(Default, Visitor)]
#[visitor(TableReference(enter))]
struct UnsupportedMVSourceDetector {
    reason: Option<&'static str>,
}

#[derive(Default, Visitor)]
#[visitor(TableRef(enter))]
struct QualifiedSourceCollector {
    tables: Vec<TableRef>,
}

fn is_supported_materialized_view_source(table: &dyn Table) -> bool {
    !table.is_temp()
        && !table.is_stream()
        && !table.is_read_only()
        && is_fuse_engine(table.engine())
}

impl MaterializedCteDetector {
    fn enter_query(&mut self, query: &Query) {
        if self.found {
            return;
        }

        self.found = query
            .with
            .as_ref()
            .is_some_and(|with| with.ctes.iter().any(|cte| cte.user_specified_materialized));
    }
}

impl ExecutorDependentPivotDetector {
    fn enter_pivot(&mut self, pivot: &Pivot) {
        if self.found {
            return;
        }

        self.found = matches!(
            pivot.values,
            PivotValues::Subquery(_) | PivotValues::Any { .. }
        );
    }
}

impl UnsupportedMVSourceDetector {
    fn enter_table_reference(&mut self, table_ref: &TableReference) {
        if self.reason.is_some() {
            return;
        }

        self.reason = match table_ref {
            TableReference::Table {
                table, temporal, ..
            } if temporal.is_some() || table.branch.is_some() => {
                Some("time travel and table branches are not supported")
            }
            TableReference::TableFunction { .. } => Some("table functions are not supported"),
            TableReference::Join { .. } => Some("joins are not supported"),
            TableReference::Location { .. } => Some("stage locations are not supported"),
            _ => None,
        };
    }
}

impl QualifiedSourceCollector {
    fn enter_table_ref(&mut self, table_ref: &TableRef) {
        // TableRefQualifier leaves CTE references unqualified and qualifies
        // every persistent catalog table reference.
        if table_ref.database.is_some() {
            self.tables.push(table_ref.clone());
        }
    }
}

impl TableRefQualifier {
    fn enter_query(&mut self, query: &mut Query) {
        let cte_names = query
            .with
            .as_ref()
            .map(|w| {
                w.ctes
                    .iter()
                    .map(|c| {
                        self.name_resolution_ctx
                            .normalize_identifier(&c.alias.name)
                            .name
                    })
                    .collect()
            })
            .unwrap_or_default();
        self.cte_scope_stack.push(cte_names);
    }

    fn exit_query(&mut self, _query: &mut Query) {
        self.cte_scope_stack.pop();
    }

    fn enter_table_ref(&mut self, table_ref: &mut TableRef) {
        if table_ref.database.is_none() {
            let normalized = self
                .name_resolution_ctx
                .normalize_identifier(&table_ref.table)
                .name;
            let is_cte = self
                .cte_scope_stack
                .iter()
                .any(|scope| scope.iter().any(|n| n == &normalized));
            if !is_cte {
                table_ref.database = Some(self.database.clone());
                if table_ref.catalog.is_none() {
                    table_ref.catalog = Some(self.catalog.clone());
                }
            }
        } else if table_ref.catalog.is_none() {
            table_ref.catalog = Some(self.catalog.clone());
        }
    }
}

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_materialized_view(
        &mut self,
        stmt: &CreateMaterializedViewStmt,
    ) -> Result<Plan> {
        let CreateMaterializedViewStmt {
            create_option,
            sync_creation,
            catalog,
            database,
            view,
            cluster_by,
            origin_query,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let query_catalog_name = self.ctx.get_current_catalog();
        let query_database_name = self.ctx.get_current_database();

        // Reject definitions whose schema depends on request-local planning
        // execution. Phase 1 stores a stable, fully bound initial schema.
        let mut materialized_cte_detector = MaterializedCteDetector::default();
        origin_query.drive(&mut materialized_cte_detector);
        if materialized_cte_detector.found {
            return Err(ErrorCode::SyntaxException(
                "Materialized CTEs are not supported in materialized view definitions".to_string(),
            ));
        }
        let mut executor_dependent_pivot_detector = ExecutorDependentPivotDetector::default();
        origin_query.drive(&mut executor_dependent_pivot_detector);
        if executor_dependent_pivot_detector.found {
            return Err(ErrorCode::SyntaxException(
                "PIVOT with ANY or subquery values is not supported in materialized view definitions"
                    .to_string(),
            ));
        }
        let mut unsupported_source_detector = UnsupportedMVSourceDetector::default();
        origin_query.drive(&mut unsupported_source_detector);
        if let Some(reason) = unsupported_source_detector.reason {
            return Err(ErrorCode::SemanticError(format!(
                "Materialized view source is invalid: {reason}"
            )));
        }

        let (catalog_name, database_name, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);
        let target_catalog = self.ctx.get_catalog(&catalog_name).await?;
        if target_catalog.support_partition() {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "Catalog '{}' does not support MATERIALIZED VIEW",
                target_catalog.name()
            )));
        }
        let target_database = target_catalog.get_database(&tenant, &database_name).await?;
        let target_database_id = target_database.get_db_info().database_id.db_id;

        let original_query = origin_query.to_string();
        let mut qualified_query = origin_query.as_ref().clone();
        let qualified_identifier = |name: &str| {
            let quote = ident_opt_quote(
                name,
                false,
                self.name_resolution_ctx.quoted_ident_case_sensitive,
                self.dialect,
            );
            Identifier::from_name_with_quoted(Span::default(), name, quote)
        };
        let mut qualifier = TableRefQualifier {
            catalog: qualified_identifier(&query_catalog_name),
            database: qualified_identifier(&query_database_name),
            name_resolution_ctx: self.name_resolution_ctx.clone(),
            cte_scope_stack: Vec::new(),
        };
        qualified_query.drive_mut(&mut qualifier);

        let mut source_collector = QualifiedSourceCollector::default();
        qualified_query.drive(&mut source_collector);
        if source_collector.tables.len() != 1 {
            return Err(ErrorCode::SemanticError(
                "Materialized view v1 requires exactly one persistent table source".to_string(),
            ));
        }
        let source_ref = &source_collector.tables[0];
        let source_catalog_name = normalize_identifier(
            source_ref
                .catalog
                .as_ref()
                .expect("qualified source must have catalog"),
            &self.name_resolution_ctx,
        )
        .name;
        let source_database_name = normalize_identifier(
            source_ref
                .database
                .as_ref()
                .expect("qualified source must have database"),
            &self.name_resolution_ctx,
        )
        .name;
        let source_table_name =
            normalize_identifier(&source_ref.table, &self.name_resolution_ctx).name;
        let source_catalog = self.ctx.get_catalog(&source_catalog_name).await?;
        let direct_source = source_catalog
            .get_table(&tenant, &source_database_name, &source_table_name)
            .await?;
        let valid_source = source_catalog_name.eq_ignore_ascii_case(CATALOG_DEFAULT)
            && is_supported_materialized_view_source(direct_source.as_ref());
        if !valid_source {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "Materialized view source '{}.{}.{}' must be a persistent base table in the default catalog using FUSE engine",
                source_catalog_name, source_database_name, source_table_name
            )));
        }
        let source_table_id = direct_source.get_id();
        let expected_source_generation = source_catalog
            .get_mv_source_generation(&tenant, source_table_id)
            .await?;

        let as_query_plan = self.as_query_plan(origin_query).await?;
        if matches!(*create_option, AstCreateOption::CreateOrReplace) {
            let existing = match target_catalog
                .get_table(&tenant, &database_name, &view_name)
                .await
            {
                Ok(table) => Some(table),
                Err(e) if e.code() == ErrorCode::UNKNOWN_TABLE => None,
                Err(e) => return Err(e),
            };
            if let Some(existing) = existing {
                if !is_materialized_view_engine(existing.engine()) {
                    return Err(ErrorCode::TableEngineNotSupported(format!(
                        "Cannot replace non-materialized view `{}`.`{}` with a materialized view",
                        database_name, view_name
                    )));
                }

                if let Plan::Query { metadata, .. } = &as_query_plan {
                    let metadata = metadata.read();
                    let existing_table_id = existing.get_id();
                    if metadata
                        .tables()
                        .iter()
                        .any(|table| table.table().get_id() == existing_table_id)
                    {
                        return Err(ErrorCode::ViewDependencyError(format!(
                            "View dependency loop detected (view: {}.{})",
                            database_name, view_name
                        )));
                    }
                }
            }
        }

        let bind_context = as_query_plan.bind_context().unwrap();

        let source_table = if let Plan::Query { metadata, .. } = &as_query_plan {
            let metadata = metadata.read();
            if metadata.tables().len() != 1 {
                return Err(ErrorCode::SemanticError(
                    "Materialized view v1 requires exactly one base table source".to_string(),
                ));
            }

            metadata.tables()[0].table()
        } else {
            unreachable!("AS query plan must be a query")
        };

        let valid_source = source_table.get_id() == source_table_id
            && is_supported_materialized_view_source(source_table.as_ref());
        if !valid_source {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "Materialized view source '{}.{}.{}' must be a persistent base table in the default catalog using FUSE engine",
                source_catalog_name, source_database_name, source_table_name
            )));
        }

        let mut schema = bind_context.output_table_schema(self.metadata.clone())?;
        let mut fields = schema.fields().clone();
        for field in fields.iter_mut() {
            if field.data_type == TableDataType::Null {
                field.data_type = TableDataType::String.wrap_nullable();
            } else if !field.data_type().is_nullable_or_null() && !self.is_column_not_null() {
                field.data_type = field.data_type().clone().wrap_nullable();
            }
        }
        schema = TableSchemaRefExt::create(fields);
        Self::validate_create_table_schema(&schema)?;

        let current_source_generation = source_catalog
            .get_mv_source_generation(&tenant, source_table_id)
            .await?;
        if current_source_generation != expected_source_generation {
            return Err(ErrorCode::InvalidMaterializedView(format!(
                "source table binding generation changed from {} to {} while binding materialized view",
                expected_source_generation, current_source_generation
            )));
        }

        let query = format!("{}", qualified_query);

        let mut options = std::collections::BTreeMap::new();
        options.insert(
            OPT_KEY_DATABASE_ID.to_owned(),
            target_database_id.to_string(),
        );
        options.insert(
            OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID.to_owned(),
            source_table_id.to_string(),
        );
        let mut cluster_key = None;
        if let Some(cluster_opt) = cluster_by {
            let keys = self
                .analyze_cluster_keys(cluster_opt, schema.clone(), None)
                .await?;
            if !keys.is_empty() {
                options
                    .entry(OPT_KEY_AGGRESSIVE_RECLUSTER.to_owned())
                    .or_insert_with(|| "1".to_owned());
                cluster_key = Some(format!("({})", keys.join(", ")));
            }
        }

        let create_option = create_option.clone().into();

        let storage_params = self
            .resolve_database_default_storage_params(target_database.as_ref())
            .await?;

        apply_fuse_storage_defaults(&mut options, storage_params.as_ref())?;

        let table_plan = CreateTablePlan {
            create_option,
            tenant,
            catalog: catalog_name,
            database: database_name,
            table: view_name,
            schema,
            engine: databend_common_ast::ast::Engine::MaterializedView,
            engine_options: Default::default(),
            storage_params,
            options,
            table_properties: None,
            table_partition: None,
            field_comments: vec![],
            field_stats_truncate_len: vec![],
            cluster_key,
            // The query is bound above to validate the definition and derive a
            // stable schema, but CREATE publishes an empty materialized view.
            // Data population belongs to the later maintenance phase.
            as_select: None,
            table_indexes: None,
            table_constraints: None,
            attached_columns: None,
        };
        Ok(Plan::CreateMaterializedView(Box::new(
            CreateMaterializedViewPlan {
                table_plan,
                query_plan: Box::new(as_query_plan),
                original_query,
                query,
                sync_creation: *sync_creation,
                expected_source_generation,
            },
        )))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_materialized_view(
        &mut self,
        stmt: &DropMaterializedViewStmt,
    ) -> Result<Plan> {
        let DropMaterializedViewStmt {
            if_exists,
            catalog,
            database,
            view,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, database, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);
        let plan = DropMaterializedViewPlan {
            if_exists: *if_exists,
            tenant,
            catalog,
            database,
            view_name,
        };
        Ok(Plan::DropMaterializedView(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_create_materialized_view(
        &mut self,
        _bind_context: &mut BindContext,
        stmt: &ShowCreateMaterializedViewStmt,
    ) -> Result<Plan> {
        let ShowCreateMaterializedViewStmt {
            catalog,
            database,
            view,
        } = stmt;

        let (catalog_name, database_name, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);

        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Table", DataType::String),
            DataField::new("Create Table", DataType::String),
        ]);
        Ok(Plan::ShowCreateMaterializedView(Box::new(
            ShowCreateMaterializedViewPlan {
                catalog: catalog_name,
                database: database_name,
                view_name,
                schema,
            },
        )))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_materialized_views(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowMaterializedViewsStmt,
    ) -> Result<Plan> {
        let ShowMaterializedViewsStmt {
            catalog,
            database,
            limit,
        } = stmt;

        let catalog_name = match catalog {
            None => self.ctx.get_current_catalog(),
            Some(ident) => normalize_identifier(ident, &self.name_resolution_ctx).name,
        };

        let database_name = self.check_database_exist(catalog, database).await?;

        let mut select_builder = SelectBuilder::from(&format!(
            "{}.system.tables",
            QuotedIdent(catalog_name.to_lowercase(), '`')
        ));
        select_builder
            .with_column("name AS Name")
            .with_column("database AS Database")
            .with_column("engine AS Engine")
            .with_column("created_on AS \"Created On\"");

        select_builder.with_filter(format!("database = {}", QuotedString(&database_name, '\'')));
        select_builder.with_filter(format!("catalog = {}", QuotedString(&catalog_name, '\'')));
        select_builder.with_filter("table_type = 'MATERIALIZED VIEW'".to_string());

        select_builder
            .with_order_by("database")
            .with_order_by("name");

        let query = match limit {
            None => select_builder.build(),
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("name LIKE {}", QuotedString(pattern, '\'')));
                select_builder.build()
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
                select_builder.build()
            }
        };
        debug!("show materialized views rewrite to: {:?}", query);
        self.bind_rewrite_to_query(
            bind_context,
            query.as_str(),
            RewriteKind::ShowTables(catalog_name, database_name),
        )
        .await
    }
}
