// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use sqlparser::ast::{FunctionArg, OrderByExpr, Statement, TableFactor};

use crate::datavalues::{DataSchema, DataValue};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::{
    ExplainPlan, ExpressionPlan, PlanBuilder, PlanNode, SelectPlan, SettingPlan, StageState,
    VarValue,
};
use crate::sessions::FuseQueryContextRef;
use crate::sql::{DFExplainPlan, DFParser, DFStatement};

pub struct PlanParser {
    ctx: FuseQueryContextRef,
}

impl PlanParser {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        Self { ctx }
    }

    pub fn build_from_sql(&self, query: &str) -> FuseQueryResult<PlanNode> {
        let statements = DFParser::parse_sql(query)?;
        if statements.len() != 1 {
            return Err(FuseQueryError::Internal(
                "Only support single query".to_string(),
            ));
        }
        self.statement_to_plan(&statements[0])
    }

    pub fn statement_to_plan(&self, statement: &DFStatement) -> FuseQueryResult<PlanNode> {
        match statement {
            DFStatement::Statement(v) => self.sql_statement_to_plan(&v),
            DFStatement::Explain(v) => self.sql_explain_to_plan(&v),
            _ => Err(FuseQueryError::Internal(
                "Only Statement are implemented".to_string(),
            )),
        }
    }

    /// Builds plan from AST statement.
    pub fn sql_statement_to_plan(
        &self,
        statement: &sqlparser::ast::Statement,
    ) -> FuseQueryResult<PlanNode> {
        match statement {
            Statement::Query(query) => self.query_to_plan(query),
            Statement::SetVariable {
                variable, value, ..
            } => self.set_variable_to_plan(variable, value),
            _ => Err(FuseQueryError::Internal(format!(
                "Unsupported statement {:?}",
                statement
            ))),
        }
    }

    /// Generate a logic plan from an EXPLAIN
    pub fn sql_explain_to_plan(&self, explain: &DFExplainPlan) -> FuseQueryResult<PlanNode> {
        let plan = self.sql_statement_to_plan(&explain.statement)?;
        Ok(PlanNode::Explain(ExplainPlan {
            typ: explain.typ,
            input: Arc::new(plan),
        }))
    }

    /// Generate a logic plan from an SQL query
    pub fn query_to_plan(&self, query: &sqlparser::ast::Query) -> FuseQueryResult<PlanNode> {
        match &query.body {
            sqlparser::ast::SetExpr::Select(s) => {
                self.select_to_plan(s.as_ref(), &query.limit, &query.order_by)
            }
            _ => Err(FuseQueryError::Internal(format!(
                "Query {} not implemented yet",
                query.body
            ))),
        }
    }

    /// Generate a logic plan from an SQL select
    fn select_to_plan(
        &self,
        select: &sqlparser::ast::Select,
        limit: &Option<sqlparser::ast::Expr>,
        order_by: &[OrderByExpr],
    ) -> FuseQueryResult<PlanNode> {
        if select.having.is_some() {
            return Err(FuseQueryError::Internal(
                "HAVING is not implemented yet".to_string(),
            ));
        }

        // from.
        let plan = self.plan_tables_with_joins(&select.from)?;

        // filter (also known as selection) first
        let plan = self.filter(&plan, &select.selection)?;

        // projection.
        let projection_expr: Vec<ExpressionPlan> = select
            .projection
            .iter()
            .map(|e| self.sql_select_to_rex(&e, &plan.schema()))
            .collect::<FuseQueryResult<Vec<ExpressionPlan>>>()?;

        // Aggregator check.
        let mut has_aggregator = false;
        for expr in &projection_expr {
            if expr.has_aggregator(self.ctx.clone())? {
                has_aggregator = true;
                break;
            }
        }

        let plan = if !select.group_by.is_empty() || has_aggregator {
            self.aggregate(&plan, projection_expr, &select.group_by)?
        } else {
            self.project(&plan, projection_expr)?
        };

        // order by
        let plan = self.sort(&plan, order_by)?;

        // limit.
        let plan = self.limit(&plan, limit)?;

        Ok(PlanNode::Select(SelectPlan {
            input: Arc::new(plan),
        }))
    }

    /// Generate a relational expression from a select SQL expression
    fn sql_select_to_rex(
        &self,
        sql: &sqlparser::ast::SelectItem,
        schema: &DataSchema,
    ) -> FuseQueryResult<ExpressionPlan> {
        match sql {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => self.sql_to_rex(expr, schema),
            sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => Ok(ExpressionPlan::Alias(
                alias.value.clone(),
                Box::new(self.sql_to_rex(&expr, schema)?),
            )),
            sqlparser::ast::SelectItem::Wildcard => Ok(ExpressionPlan::Wildcard),
            _ => Err(FuseQueryError::Internal(format!(
                "SelectItem: {:?} are not supported",
                sql
            ))),
        }
    }

    fn plan_tables_with_joins(
        &self,
        from: &[sqlparser::ast::TableWithJoins],
    ) -> FuseQueryResult<PlanNode> {
        match from.len() {
            0 => self.plan_with_dummy_source(),
            1 => self.plan_table_with_joins(&from[0]),
            _ => Err(FuseQueryError::Internal(
                "Cannot support JOIN clause".to_string(),
            )),
        }
    }

    fn plan_with_dummy_source(&self) -> FuseQueryResult<PlanNode> {
        let db_name = "system";
        let table_name = "one";
        let table = self.ctx.get_table(db_name, table_name)?;
        let schema = table.schema()?;

        let scan = PlanBuilder::scan(
            self.ctx.clone(),
            db_name,
            table_name,
            schema.as_ref(),
            None,
            None,
        )?
        .build()?;
        let datasource_plan = table.read_plan(self.ctx.clone(), scan)?;
        Ok(PlanNode::ReadSource(datasource_plan))
    }

    fn plan_table_with_joins(
        &self,
        t: &sqlparser::ast::TableWithJoins,
    ) -> FuseQueryResult<PlanNode> {
        self.create_relation(&t.relation)
    }

    fn create_relation(&self, relation: &sqlparser::ast::TableFactor) -> FuseQueryResult<PlanNode> {
        match relation {
            sqlparser::ast::TableFactor::Table { name, args, .. } => {
                let mut db_name = self.ctx.get_default_db()?;
                let mut table_name = name.to_string();
                if name.0.len() == 2 {
                    db_name = name.0[0].to_string();
                    table_name = name.0[1].to_string();
                }
                let table = self.ctx.get_table(&db_name, table_name.as_str())?;
                let schema = table.schema()?;

                let mut table_args = None;
                if !args.is_empty() {
                    match &args[0] {
                        FunctionArg::Named { arg, .. } => {
                            table_args = Some(self.sql_to_rex(&arg, &schema)?);
                        }
                        FunctionArg::Unnamed(arg) => {
                            table_args = Some(self.sql_to_rex(&arg, &schema)?);
                        }
                    }
                }

                let scan = PlanBuilder::scan(
                    self.ctx.clone(),
                    &db_name,
                    &table_name,
                    schema.as_ref(),
                    None,
                    table_args,
                )?
                .build()?;
                let datasource_plan = table.read_plan(self.ctx.clone(), scan)?;
                Ok(PlanNode::ReadSource(datasource_plan))
            }
            sqlparser::ast::TableFactor::Derived { subquery, .. } => self.query_to_plan(subquery),
            sqlparser::ast::TableFactor::NestedJoin(table_with_joins) => {
                self.plan_table_with_joins(table_with_joins)
            }
            TableFactor::TableFunction { .. } => Err(FuseQueryError::Internal(
                "Unsupported table function".to_string(),
            )),
        }
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_rex(
        &self,
        sql: &sqlparser::ast::Expr,
        schema: &DataSchema,
    ) -> FuseQueryResult<ExpressionPlan> {
        match sql {
            sqlparser::ast::Expr::Identifier(ref v) => Ok(ExpressionPlan::Field(v.clone().value)),
            sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
                Ok(ExpressionPlan::Constant(DataValue::try_from_literal(n)?))
            }
            sqlparser::ast::Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => {
                Ok(ExpressionPlan::Constant(DataValue::String(Some(s.clone()))))
            }
            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                Ok(ExpressionPlan::BinaryExpression {
                    op: format!("{}", op),
                    left: Box::new(self.sql_to_rex(left, schema)?),
                    right: Box::new(self.sql_to_rex(right, schema)?),
                })
            }
            sqlparser::ast::Expr::Nested(e) => self.sql_to_rex(e, schema),
            sqlparser::ast::Expr::Function(e) => {
                let mut args = Vec::with_capacity(e.args.len());
                for arg in &e.args {
                    match &arg {
                        FunctionArg::Named { arg, .. } => {
                            args.push(self.sql_to_rex(arg, schema)?);
                        }
                        FunctionArg::Unnamed(arg) => {
                            args.push(self.sql_to_rex(arg, schema)?);
                        }
                    }
                }
                Ok(ExpressionPlan::Function {
                    op: e.name.to_string(),
                    args,
                })
            }
            sqlparser::ast::Expr::Wildcard => Ok(ExpressionPlan::Wildcard),
            _ => Err(FuseQueryError::Plan(format!(
                "Unsupported ExpressionPlan: {}",
                sql
            ))),
        }
    }

    pub fn set_variable_to_plan(
        &self,
        variable: &sqlparser::ast::Ident,
        values: &[sqlparser::ast::SetVariableValue],
    ) -> FuseQueryResult<PlanNode> {
        let mut vars = vec![];
        for value in values {
            let variable = variable.value.clone();
            let value = match value {
                sqlparser::ast::SetVariableValue::Ident(v) => v.value.clone(),
                sqlparser::ast::SetVariableValue::Literal(v) => v.to_string(),
            };
            vars.push(VarValue { variable, value });
        }
        Ok(PlanNode::SetVariable(SettingPlan { vars }))
    }

    /// Apply a filter to the plan
    fn filter(
        &self,
        plan: &PlanNode,
        predicate: &Option<sqlparser::ast::Expr>,
    ) -> FuseQueryResult<PlanNode> {
        match *predicate {
            Some(ref predicate_expr) => PlanBuilder::from(self.ctx.clone(), &plan)
                .filter(self.sql_to_rex(predicate_expr, &plan.schema())?)?
                .build(),
            _ => Ok(plan.clone()),
        }
    }

    /// Wrap a plan in a projection
    fn project(&self, input: &PlanNode, expr: Vec<ExpressionPlan>) -> FuseQueryResult<PlanNode> {
        PlanBuilder::from(self.ctx.clone(), input)
            .project(expr)?
            .build()
    }

    /// Wrap a plan for an aggregate
    fn aggregate(
        &self,
        input: &PlanNode,
        aggr_expr: Vec<ExpressionPlan>,
        group_by: &[sqlparser::ast::Expr],
    ) -> FuseQueryResult<PlanNode> {
        let group_expr: Vec<ExpressionPlan> = group_by
            .iter()
            .map(|e| self.sql_to_rex(&e, &input.schema()))
            .collect::<FuseQueryResult<Vec<ExpressionPlan>>>()?;

        // S0: Apply a partial aggregator plan.
        // S1: Apply a fragment plan for distributed planners split.
        // S2: Apply a final aggregator plan.
        PlanBuilder::from(self.ctx.clone(), &input)
            .aggregate_partial(aggr_expr.clone(), group_expr.clone())?
            .stage(StageState::AggregatorMerge)?
            .aggregate_final(aggr_expr, group_expr)?
            .build()
    }

    fn sort(&self, input: &PlanNode, order_by: &[OrderByExpr]) -> FuseQueryResult<PlanNode> {
        if order_by.is_empty() {
            return Ok(input.clone());
        }

        let order_by_exprs: Vec<ExpressionPlan> = order_by
            .iter()
            .map(|e| -> FuseQueryResult<ExpressionPlan> {
                Ok(ExpressionPlan::Sort {
                    expr: Box::new(self.sql_to_rex(&e.expr, &input.schema())?),
                    asc: e.asc.unwrap_or(true),
                    nulls_first: e.nulls_first.unwrap_or(true),
                })
            })
            .collect::<FuseQueryResult<Vec<ExpressionPlan>>>()?;

        PlanBuilder::from(self.ctx.clone(), &input)
            .sort(&order_by_exprs)?
            .build()
    }

    /// Wrap a plan in a limit
    fn limit(
        &self,
        input: &PlanNode,
        limit: &Option<sqlparser::ast::Expr>,
    ) -> FuseQueryResult<PlanNode> {
        match *limit {
            Some(ref limit_expr) => {
                let n = match self.sql_to_rex(&limit_expr, &input.schema())? {
                    ExpressionPlan::Constant(DataValue::UInt64(Some(n))) => Ok(n as usize),
                    _ => Err(FuseQueryError::Plan(
                        "Unexpected expression for LIMIT clause".to_string(),
                    )),
                }?;
                PlanBuilder::from(self.ctx.clone(), &input)
                    .limit(n)?
                    .build()
            }
            _ => Ok(input.clone()),
        }
    }
}
