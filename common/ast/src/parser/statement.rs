// Copyright 2022 Datafuse Labs.
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

use common_meta_types::AuthType;
use common_meta_types::PrincipalIdentity;
use common_meta_types::UserIdentity;
use nom::branch::alt;
use nom::combinator::map;
use nom::combinator::value;

use super::error::ErrorKind;
use crate::ast::*;
use crate::parser::expr::*;
use crate::parser::query::*;
use crate::parser::token::*;
use crate::parser::util::*;
use crate::rule;

pub fn statements(i: Input) -> IResult<Vec<Statement>> {
    let stmt = map(statement, Some);
    let eoi = map(rule! { &EOI }, |_| None);

    map(
        rule!(
            #separated_list1(rule! { ";"+ }, rule! { #stmt | #eoi }) ~ &EOI
        ),
        |(stmts, _)| stmts.into_iter().flatten().collect(),
    )(i)
}

pub fn statement(i: Input) -> IResult<Statement> {
    let explain = map(
        rule! {
            EXPLAIN ~ ( PIPELINE | GRAPH )? ~ #statement
        },
        |(_, opt_kind, statement)| Statement::Explain {
            kind: match opt_kind.map(|token| token.kind) {
                Some(TokenKind::PIPELINE) => ExplainKind::Pipeline,
                Some(TokenKind::GRAPH) => ExplainKind::Graph,
                None => ExplainKind::Syntax,
                _ => unreachable!(),
            },
            query: Box::new(statement),
        },
    );
    let show_databases = map(
        rule! {
            SHOW ~ ( DATABASES | SCHEMAS ) ~ #show_limit?
        },
        |(_, _, limit)| Statement::ShowDatabases { limit },
    );
    let show_create_database = map(
        rule! {
            SHOW ~ CREATE ~ ( DATABASE | SCHEMA ) ~ #ident
        },
        |(_, _, _, database)| Statement::ShowCreateDatabase { database },
    );
    let create_database = map(
        rule! {
            CREATE ~ ( DATABASE | SCHEMA ) ~ ( IF ~ NOT ~ EXISTS )? ~ ( #ident ~ "." )? ~ #ident ~ #database_engine?
        },
        |(_, _, opt_if_not_exists, opt_catalog, database, engine)| {
            Statement::CreateDatabase(CreateDatabaseStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                catalog: opt_catalog.map(|(catalog, _)| catalog),
                database,
                engine,
                options: vec![],
            })
        },
    );
    let drop_database = map(
        rule! {
            DROP ~ ( DATABASE | SCHEMA ) ~ ( IF ~ EXISTS )? ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, _, opt_if_exists, opt_catalog, database)| {
            Statement::DropDatabase(DropDatabaseStmt {
                if_exists: opt_if_exists.is_some(),
                catalog: opt_catalog.map(|(catalog, _)| catalog),
                database,
            })
        },
    );
    let alter_database = map(
        rule! {
            ALTER ~ DATABASE ~ ( IF ~ EXISTS )? ~ #ident ~ #alter_database_action
        },
        |(_, _, opt_if_exists, database, action)| Statement::AlterDatabase {
            if_exists: opt_if_exists.is_some(),
            database,
            action,
        },
    );
    let use_database = map(
        rule! {
            USE ~ #ident
        },
        |(_, database)| Statement::UseDatabase { database },
    );
    let show_tables = map(
        rule! {
            SHOW ~ FULL? ~ TABLES ~ HISTORY? ~ ( FROM ~ ^#ident )? ~ #show_limit?
        },
        |(_, opt_full, _, opt_history, opt_database, limit)| Statement::ShowTables {
            database: opt_database.map(|(_, database)| database),
            full: opt_full.is_some(),
            limit,
            with_history: opt_history.is_some(),
        },
    );
    let show_create_table = map(
        rule! {
            SHOW ~ CREATE ~ TABLE ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, _, _, opt_database, table)| Statement::ShowCreateTable {
            database: opt_database.map(|(database, _)| database),
            table,
        },
    );
    let show_tables_status = map(
        rule! {
            SHOW ~ TABLE ~ STATUS ~ ( FROM ~ ^#ident )? ~ #show_limit?
        },
        |(_, _, _, opt_database, limit)| Statement::ShowTablesStatus {
            database: opt_database.map(|(_, database)| database),
            limit,
        },
    );
    let create_table = map(
        rule! {
            CREATE ~ TABLE ~ ( IF ~ NOT ~ EXISTS )?
            ~ ( #ident ~ "." )? ~ #ident
            ~ #create_table_source?
            ~ ( #table_option )*
            ~ ( COMMENT ~ "=" ~ #literal_string )?
            ~ ( CLUSTER ~ ^BY ~ ^"(" ~ ^#comma_separated_list1(expr) ~ ^")" )?
            ~ ( AS ~ ^#query )?
        },
        |(
            _,
            _,
            opt_if_not_exists,
            opt_database,
            table,
            source,
            table_options,
            opt_comment,
            opt_cluster_by,
            opt_as_query,
        )| {
            Statement::CreateTable(CreateTableStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                database: opt_database.map(|(database, _)| database),
                table,
                source,
                table_options,
                comment: opt_comment.map(|(_, _, comment)| comment),
                cluster_by: opt_cluster_by
                    .map(|(_, _, _, exprs, _)| exprs)
                    .unwrap_or_default(),
                as_query: opt_as_query.map(|(_, query)| Box::new(query)),
            })
        },
    );
    let describe = map(
        rule! {
            ( DESC | DESCRIBE ) ~ ( #ident ~ "." )? ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, opt_catalog, opt_database, table)| Statement::Describe {
            catalog: opt_catalog.map(|(catalog, _)| catalog),
            database: opt_database.map(|(database, _)| database),
            table,
        },
    );
    let drop_table = map(
        rule! {
            DROP ~ TABLE ~ ( IF ~ EXISTS )? ~ ( #ident ~ "." )? ~ #ident ~ ( ALL )?
        },
        |(_, _, opt_if_exists, opt_database, table, opt_all)| Statement::DropTable {
            if_exists: opt_if_exists.is_some(),
            database: opt_database.map(|(database, _)| database),
            table,
            all: opt_all.is_some(),
        },
    );
    let undrop_table = map(
        rule! {
            UNDROP ~ TABLE ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, _, opt_database, table)| Statement::UndropTable {
            database: opt_database.map(|(database, _)| database),
            table,
        },
    );
    let alter_table = map(
        rule! {
            ALTER ~ TABLE ~ ( IF ~ EXISTS )? ~ ( #ident ~ "." )? ~ #ident ~ #alter_table_action
        },
        |(_, _, opt_if_exists, opt_database, table, action)| Statement::AlterTable {
            if_exists: opt_if_exists.is_some(),
            database: opt_database.map(|(database, _)| database),
            table,
            action,
        },
    );
    let rename_table = map(
        rule! {
            RENAME ~ TABLE ~ ( #ident ~ "." )? ~ #ident ~ TO ~ #ident
        },
        |(_, _, opt_database, table, _, new_table)| Statement::RenameTable {
            database: opt_database.map(|(database, _)| database),
            table,
            new_table,
        },
    );
    let truncate_table = map(
        rule! {
            TRUNCATE ~ TABLE ~ ( #ident ~ "." )? ~ #ident ~ PURGE?
        },
        |(_, _, opt_database, table, opt_purge)| Statement::TruncateTable {
            database: opt_database.map(|(database, _)| database),
            table,
            purge: opt_purge.is_some(),
        },
    );
    let optimize_table = map(
        rule! {
            OPTIMIZE ~ TABLE ~ ( #ident ~ "." )? ~ #ident ~ #optimize_table_action?
        },
        |(_, _, opt_database, table, action)| Statement::OptimizeTable {
            database: opt_database.map(|(database, _)| database),
            table,
            action,
        },
    );
    let create_view = map(
        rule! {
            CREATE ~ VIEW ~ ( IF ~ NOT ~ EXISTS )?
            ~ ( #ident ~ "." )? ~ #ident
            ~ AS ~ #query
        },
        |(_, _, opt_if_not_exists, opt_database, view, _, query)| {
            Statement::CreateView(CreateViewStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                database: opt_database.map(|(database, _)| database),
                view,
                query: Box::new(query),
            })
        },
    );
    let alter_view = map(
        rule! {
            ALTER ~ VIEW
            ~ ( #ident ~ "." )? ~ #ident
            ~ AS ~ #query
        },
        |(_, _, opt_database, view, _, query)| Statement::AlterView {
            database: opt_database.map(|(database, _)| database),
            view,
            query: Box::new(query),
        },
    );
    let drop_view = map(
        rule! {
            DROP ~ VIEW ~ ( IF ~ EXISTS )? ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, _, opt_if_exists, opt_database, view)| Statement::DropView {
            if_exists: opt_if_exists.is_some(),
            database: opt_database.map(|(database, _)| database),
            view,
        },
    );
    let show_settings = value(Statement::ShowSettings, rule! { SHOW ~ SETTINGS });
    let show_stages = value(Statement::ShowStages, rule! { SHOW ~ STAGES });
    let show_process_list = value(Statement::ShowProcessList, rule! { SHOW ~ PROCESSLIST });
    let show_metrics = value(Statement::ShowMetrics, rule! { SHOW ~ METRICS });
    let show_functions = map(
        rule! {
            SHOW ~ FUNCTIONS ~ #show_limit?
        },
        |(_, _, limit)| Statement::ShowFunctions { limit },
    );
    let kill_stmt = map(
        rule! {
            KILL ~ #kill_target ~ #ident
        },
        |(_, kill_target, object_id)| Statement::KillStmt {
            kill_target,
            object_id,
        },
    );
    let set_variable = map(
        rule! {
            SET ~ #ident ~ "=" ~ #literal
        },
        |(_, variable, _, value)| Statement::SetVariable { variable, value },
    );
    let insert = map(
        rule! {
            INSERT ~ ( INTO | OVERWRITE ) ~ TABLE?
            ~ ( #ident ~ "." )? ~ #ident
            ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
            ~ #insert_source
        },
        |(_, overwrite, _, opt_database, table, opt_columns, source)| Statement::Insert {
            database: opt_database.map(|(database, _)| database),
            table,
            columns: opt_columns
                .map(|(_, columns, _)| columns)
                .unwrap_or_default(),
            source,
            overwrite: overwrite.kind == OVERWRITE,
        },
    );
    let create_user = map(
        rule! {
            CREATE ~ USER ~ ( IF ~ NOT ~ EXISTS )?
            ~ #user_identity
            ~ IDENTIFIED ~ ( WITH ~ ^#auth_type )? ~ ( BY ~ ^#literal_string )?
            ~ ( WITH ~ ^#role_option+ )?
        },
        |(_, _, opt_if_not_exists, user, _, opt_auth_type, opt_password, opt_role_options)| {
            Statement::CreateUser(CreateUserStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                user,
                auth_option: AuthOption {
                    auth_type: opt_auth_type.map(|(_, auth_type)| auth_type),
                    password: opt_password.map(|(_, password)| password),
                },
                role_options: opt_role_options
                    .map(|(_, role_options)| role_options)
                    .unwrap_or_default(),
            })
        },
    );
    let alter_user = map(
        rule! {
            ALTER ~ USER ~ ( #map(rule! { USER ~ "(" ~ ")" }, |_| None) | #map(user_identity, Some) )
            ~ ( IDENTIFIED ~ ( WITH ~ ^#auth_type )? ~ ( BY ~ ^#literal_string )? )?
            ~ ( WITH ~ ^#role_option+ )?
        },
        |(_, _, user, opt_auth_option, opt_role_options)| Statement::AlterUser {
            user,
            auth_option: opt_auth_option.map(|(_, opt_auth_type, opt_password)| AuthOption {
                auth_type: opt_auth_type.map(|(_, auth_type)| auth_type),
                password: opt_password.map(|(_, password)| password),
            }),
            role_options: opt_role_options
                .map(|(_, role_options)| role_options)
                .unwrap_or_default(),
        },
    );
    let drop_user = map(
        rule! {
            DROP ~ USER ~ ( IF ~ EXISTS )? ~ #user_identity
        },
        |(_, _, opt_if_exists, user)| Statement::DropUser {
            if_exists: opt_if_exists.is_some(),
            user,
        },
    );
    //GRANT { ROLE <role_name> | {schemaObjectPrivileges | ALL [ PRIVILEGES ] ON <privileges_level>} } TO { [ROLE <role_name>] | [USER <user>] }
    let grant_priv = map(
        rule! {
            GRANT ~ #grant_source ~ TO ~ #grant_option
        },
        |(_, source, _, grant_option)| {
            Statement::GrantPriv(GrantPrivilegeStatement {
                source,
                principal: grant_option,
            })
        },
    );
    let create_udf = map(
        rule! {
            CREATE ~ FUNCTION ~ ( IF ~ NOT ~ EXISTS )?
            ~ #ident
            ~ AS ~ "(" ~ #comma_separated_list0(ident) ~ ")"
            ~ "->" ~ #expr
            ~ ( DESC ~ ^"=" ~ ^#literal_string )?
        },
        |(
            _,
            _,
            opt_if_not_exists,
            udf_name,
            _,
            _,
            parameters,
            _,
            _,
            definition,
            opt_description,
        )| {
            Statement::CreateUDF {
                if_not_exists: opt_if_not_exists.is_some(),
                udf_name,
                parameters,
                definition: Box::new(definition),
                description: opt_description.map(|(_, _, description)| description),
            }
        },
    );
    let drop_udf = map(
        rule! {
            DROP ~ FUNCTION ~ ( IF ~ EXISTS )? ~ #ident
        },
        |(_, _, opt_if_exists, udf_name)| Statement::DropUDF {
            if_exists: opt_if_exists.is_some(),
            udf_name,
        },
    );
    let alter_udf = map(
        rule! {
            ALTER ~ FUNCTION
            ~ #ident
            ~ AS ~ "(" ~ #comma_separated_list0(ident) ~ ")"
            ~ "->" ~ #expr
            ~ ( DESC ~ ^"=" ~ ^#literal_string )?
        },
        |(_, _, udf_name, _, _, parameters, _, _, definition, opt_description)| {
            Statement::AlterUDF {
                udf_name,
                parameters,
                definition: Box::new(definition),
                description: opt_description.map(|(_, _, description)| description),
            }
        },
    );

    // stages
    let create_stage = map(
        rule! {
            CREATE ~ STAGE ~ ( IF ~ NOT ~ EXISTS )?
            ~ #ident
            ~ ( URL ~ "=" ~ #literal_string
                ~ (CREDENTIALS ~ "=" ~ #options)?
                ~ (ENCRYPTION ~ "=" ~ #options)?
              )?
            ~ ( FILE_FORMAT ~ "=" ~ #options)?
            ~ ( ON_ERROR ~ "=" ~ #ident)?
            ~ ( SIZE_LIMIT ~ "=" ~ #literal_u64)?
            ~ ( VALIDATION_MODE ~ "=" ~ #ident)?
            ~ ( (COMMENT | COMMENTS) ~ "=" ~ #literal_string)?
        },
        |(
            _,
            _,
            opt_if_not_exists,
            stage,
            url_opt,
            file_format_opt,
            on_error_opt,
            size_limit_opt,
            validation_mode_opt,
            comment_opt,
        )| {
            let (location, credential_options, encryption_options) = url_opt
                .map(|(_, _, url, c, e)| {
                    (
                        url,
                        c.map(|v| v.2).unwrap_or_default(),
                        e.map(|v| v.2).unwrap_or_default(),
                    )
                })
                .unwrap_or_default();

            Statement::CreateStage(CreateStageStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                stage_name: stage.to_string(),
                location,
                credential_options,
                encryption_options,
                file_format_options: file_format_opt
                    .map(|(_, _, file_format_opt)| file_format_opt)
                    .unwrap_or_default(),
                on_error: on_error_opt.map(|v| v.2.to_string()).unwrap_or_default(),
                size_limit: size_limit_opt.map(|v| v.2 as usize).unwrap_or_default(),
                validation_mode: validation_mode_opt
                    .map(|v| v.2.to_string())
                    .unwrap_or_default(),
                comments: comment_opt.map(|v| v.2).unwrap_or_default(),
            })
        },
    );

    let list_stage = map(
        rule! {
            LIST ~ #at_string ~ (PATTERN ~ "=" ~ #literal_string)?
        },
        |(_, stage_name, pattern_opt)| Statement::ListStage {
            stage_name,
            pattern: pattern_opt.map(|v| v.2).unwrap_or_default(),
        },
    );

    let _remove_stage = map(
        rule! {
            REMOVE ~ #at_string
        },
        |(_, stage_name)| Statement::RemoveStage { stage_name },
    );

    let drop_stage = map(
        rule! {
            DROP ~ STAGE ~ ( IF ~ EXISTS )? ~ #ident
        },
        |(_, _, opt_if_exists, stage_name)| Statement::DropStage {
            if_exists: opt_if_exists.is_some(),
            stage_name: stage_name.to_string(),
        },
    );

    let desc_stage = map(
        rule! {
            DESC ~ STAGE ~ #ident
        },
        |(_, _, stage_name)| Statement::DescStage {
            stage_name: stage_name.to_string(),
        },
    );

    alt((
        rule!(
            #explain : "`EXPLAIN [PIPELINE | GRAPH] <statement>`"
            | #map(query, |query| Statement::Query(Box::new(query)))
            | #show_databases : "`SHOW DATABASES [<show_limit>]`"
            | #show_create_database : "`SHOW CREATE DATABASE <database>`"
            | #create_database : "`CREATE DATABASE [IF NOT EXIST] <database> [ENGINE = <engine>]`"
            | #drop_database : "`DROP DATABASE [IF EXISTS] <database>`"
            | #alter_database : "`ALTER DATABASE [IF EXISTS] <action>`"
            | #use_database : "`USE <database>`"
            | #show_tables : "`SHOW [FULL] TABLES [FROM <database>] [<show_limit>]`"
            | #show_create_table : "`SHOW CREATE TABLE [<database>.]<table>`"
            | #show_tables_status : "`SHOW TABLES STATUS [FROM <database>] [<show_limit>]`"
            | #create_table : "`CREATE TABLE [IF NOT EXISTS] [<database>.]<table> [<source>] [<table_options>]`"
            | #describe : "`DESCRIBE [<database>.]<table>`"
            | #drop_table : "`DROP TABLE [IF EXISTS] [<database>.]<table>`"
            | #undrop_table : "`UNDROP TABLE [<database>.]<table>`"
            | #alter_table : "`ALTER TABLE [<database>.]<table> <action>`"
            | #rename_table : "`RENAME TABLE [<database>.]<table> TO <new_table>`"
            | #truncate_table : "`TRUNCATE TABLE [<database>.]<table> [PURGE]`"
            | #optimize_table : "`OPTIMIZE TABLE [<database>.]<table> (ALL | PURGE | COMPACT)`"
        ),
        rule!(
            #create_view : "`CREATE VIEW [IF NOT EXISTS] [<database>.]<view> AS SELECT ...`"
            | #drop_view : "`DROP VIEW [IF EXISTS] [<database>.]<view>`"
            | #alter_view : "`ALTER VIEW [<database>.]<view> AS SELECT ...`"
            | #show_settings : "`SHOW SETTINGS`"
            | #show_stages : "`SHOW STAGES`"
            | #show_process_list : "`SHOW PROCESSLIST`"
            | #show_metrics : "`SHOW METRICS`"
            | #show_functions : "`SHOW FUNCTIONS [<show_limit>]`"
            | #kill_stmt : "`KILL (QUERY | CONNECTION) <object_id>`"
            | #set_variable : "`SET <variable> = <value>`"
            | #insert : "`INSERT INTO [TABLE] <table> [(<column>, ...)] (FORMAT <format> | VALUES <values> | <query>)`"
            | #create_user : "`CREATE USER [IF NOT EXISTS] '<username>'@'hostname' IDENTIFIED [WITH <auth_type>] [BY <password>] [WITH <role_option> ...]`"
            | #alter_user : "`ALTER USER ('<username>'@'hostname' | USER()) [IDENTIFIED [WITH <auth_type>] [BY <password>]] [WITH <role_option> ...]`"
            | #drop_user : "`DROP USER [IF EXISTS] '<username>'@'hostname'`"
            | #create_udf : "`CREATE FUNCTION [IF NOT EXISTS] <udf_name> (<parameter>, ...) -> <definition expr> [DESC = <description>]`"
            | #drop_udf : "`DROP FUNCTION [IF EXISTS] <udf_name>`"
            | #alter_udf : "`ALTER FUNCTION <udf_name> (<parameter>, ...) -> <definition_expr> [DESC = <description>]`"
            | #create_stage: "`CREATE STAGE [ IF NOT EXISTS ] <internal_stage_name>
                [ FILE_FORMAT = ( { TYPE = { CSV | PARQUET } [ formatTypeOptions ] ) } ]
                [ COPY_OPTIONS = ( copyOptions ) ]
                [ COMMENT = '<string_literal>' ]`"
            | #desc_stage: "`DESC STAGE <stage_name>`"
            // | #remove_stage: "`REMOVE @<stage_name>`"
            | #list_stage: "`LIST @<stage_name> [pattern = '<pattern>']`"
            | #drop_stage: "`DROP STAGE <stage_name>`"
        ),
        rule!(//| #grant_priv : "`GRANT <grant_source> ON <grant_level> TO <grant_option>`"
            #grant_priv : "`GRANT { ROLE <role_name> | schemaObjectPrivileges | ALL [ PRIVILEGES ] ON <privileges_level> } TO { [ROLE <role_name>] | [USER <user>] }`"),
    ))(i)
}

pub fn column_def(i: Input) -> IResult<ColumnDefinition> {
    #[derive(Clone)]
    enum ColumnConstraint<'a> {
        Nullable(bool),
        DefaultExpr(Box<Expr<'a>>),
    }

    let nullable = alt((
        value(ColumnConstraint::Nullable(true), rule! { NULL }),
        value(ColumnConstraint::Nullable(false), rule! { NOT ~ ^NULL }),
    ));
    let default_expr = map(
        rule! {
            DEFAULT ~ ^#subexpr(NOT_PREC)
        },
        |(_, default_expr)| ColumnConstraint::DefaultExpr(Box::new(default_expr)),
    );

    map(
        rule! {
            #ident
            ~ #type_name
            ~ ( #nullable | #default_expr )*
            : "`<column name> <type> [NOT NULL | NULL] [DEFAULT <default value>]`"
        },
        |(name, data_type, constraints)| {
            let mut def = ColumnDefinition {
                name,
                data_type,
                nullable: false,
                default_expr: None,
            };
            for constraint in constraints {
                match constraint {
                    ColumnConstraint::Nullable(nullable) => def.nullable = nullable,
                    ColumnConstraint::DefaultExpr(default_expr) => {
                        def.default_expr = Some(default_expr)
                    }
                }
            }
            def
        },
    )(i)
}

pub fn insert_source(i: Input) -> IResult<InsertSource> {
    let streaming = map(
        rule! {
            FORMAT ~ #ident
        },
        |(_, format)| InsertSource::Streaming {
            format: format.name,
        },
    );
    let values = map(
        rule! {
            VALUES ~ #values_tokens
        },
        |(_, values_tokens)| InsertSource::Values { values_tokens },
    );
    let query = map(query, |query| InsertSource::Select {
        query: Box::new(query),
    });

    rule!(
        #streaming
        | #values
        | #query
    )(i)
}

//{ ROLE <role_name> | schemaObjectPrivileges | ALL [ PRIVILEGES ] ON <privileges_level> }
pub fn grant_source(i: Input) -> IResult<GrantSource> {
    let role = map(
        rule! {
            ROLE ~ #literal_string
        },
        |(_, role_name)| GrantSource::Role { role: role_name },
    );
    let privs = map(
        rule! {
            #comma_separated_list1(literal_string) ~ ON ~ #grant_level
        },
        |(privs, _, level)| GrantSource::Privs {
            privileges: privs,
            level,
        },
    );
    let all = map(
        rule! { ALL ~ PRIVILEGES? ~ ON ~ #grant_level },
        |(_, _, _, level)| GrantSource::ALL { all: true, level },
    );

    rule!(
        #role
        | #privs
        | #all
    )(i)
}

// privileges_level ::=
//     *.*
//   | db_name.*
//   | *
//   | db_name.tbl_name
pub fn grant_level(i: Input) -> IResult<GrantLevel> {
    // *.*
    let global = map(rule! { "*.*" }, |_| GrantLevel::Global);
    // db.*
    // "*": as current db or "table" with current db
    let db = map(
        rule! {
            ( #literal_string ~ "." )? ~ "*"
        },
        |(database_name, _)| {
            if database_name.is_none() {
                GrantLevel::Database(None)
            } else {
                GrantLevel::Database(Some(
                    database_name
                        .map(|(db_name, _)| db_name)
                        .unwrap_or_default(),
                ))
            }
        },
    );
    // db.table
    let table = map(
        rule! {
            #literal_string ~ "." ~ #literal_string
        },
        |(database_name, _, table_name)| GrantLevel::Table(Some(database_name), table_name),
    );

    rule!(
        #global
        | #db
        | #table
    )(i)
}

pub fn grant_option(i: Input) -> IResult<PrincipalIdentity> {
    let role = map(
        rule! {
            ROLE ~ #literal_string
        },
        |(_, role_name)| PrincipalIdentity::Role(role_name),
    );

    let user = map(
        rule! {
            USER? ~ #user_identity
        },
        |(_, user)| PrincipalIdentity::User(user),
    );

    rule!(
        #role
        | #user
    )(i)
}

pub fn create_table_source(i: Input) -> IResult<CreateTableSource> {
    let columns = map(
        rule! {
            "(" ~ ^#comma_separated_list1(column_def) ~ ^")"
        },
        |(_, columns, _)| CreateTableSource::Columns(columns),
    );
    let like = map(
        rule! {
            LIKE ~ ( #ident ~ "." )? ~ ^#ident
        },
        |(_, opt_database, table)| CreateTableSource::Like {
            database: opt_database.map(|(database, _)| database),
            table,
        },
    );

    rule!(
        #columns
        | #like
    )(i)
}

pub fn alter_database_action(i: Input) -> IResult<AlterDatabaseAction> {
    let mut rename_database = map(
        rule! {
            RENAME ~ TO ~ #ident
        },
        |(_, _, new_db)| AlterDatabaseAction::RenameDatabase { new_db },
    );

    rule!(
        #rename_database
    )(i)
}

pub fn alter_table_action(i: Input) -> IResult<AlterTableAction> {
    let rename_table = map(
        rule! {
            RENAME ~ TO ~ #ident
        },
        |(_, _, new_table)| AlterTableAction::RenameTable { new_table },
    );

    let alter_table_cluster_key = map(
        rule! {
            CLUSTER ~ ^BY ~ ^"(" ~ ^#comma_separated_list1(expr) ~ ^")"
        },
        |(_, _, _, cluster_by, _)| AlterTableAction::AlterTableClusterKey { cluster_by },
    );

    let drop_table_cluster_key = map(
        rule! {
            DROP ~ CLUSTER ~ KEY
        },
        |(_, _, _)| AlterTableAction::DropTableClusterKey,
    );

    rule!(
        #rename_table
        | #alter_table_cluster_key
        | #drop_table_cluster_key
    )(i)
}

pub fn optimize_table_action(i: Input) -> IResult<OptimizeTableAction> {
    alt((
        value(OptimizeTableAction::All, rule! { ALL }),
        value(OptimizeTableAction::Purge, rule! { PURGE }),
        value(OptimizeTableAction::Compact, rule! { COMPACT }),
    ))(i)
}

pub fn kill_target(i: Input) -> IResult<KillTarget> {
    alt((
        value(KillTarget::Query, rule! { QUERY }),
        value(KillTarget::Connection, rule! { CONNECTION }),
    ))(i)
}

pub fn show_limit(i: Input) -> IResult<ShowLimit> {
    let limit_like = map(
        rule! {
            LIKE ~ #literal_string
        },
        |(_, pattern)| ShowLimit::Like { pattern },
    );
    let limit_where = map(
        rule! {
            WHERE ~ #expr
        },
        |(_, selection)| ShowLimit::Where {
            selection: Box::new(selection),
        },
    );

    rule!(
        #limit_like
        | #limit_where
    )(i)
}

pub fn table_option(i: Input) -> IResult<TableOption> {
    alt((
        map(engine, TableOption::Engine),
        map(
            rule! {
                COMMENT ~ ^"=" ~ #literal_string
            },
            |(_, _, comment)| TableOption::Comment(comment),
        ),
    ))(i)
}

pub fn engine(i: Input) -> IResult<Engine> {
    let engine = alt((
        value(Engine::Null, rule! { NULL }),
        value(Engine::Memory, rule! { MEMORY }),
        value(Engine::Fuse, rule! { FUSE }),
        value(Engine::Github, rule! { GITHUB }),
        value(Engine::View, rule! { VIEW }),
    ));

    map(
        rule! {
            ENGINE ~ ^"=" ~ ^#engine
        },
        |(_, _, engine)| engine,
    )(i)
}

pub fn database_engine(i: Input) -> IResult<DatabaseEngine> {
    let engine = alt((
        value(DatabaseEngine::Default, rule! {DEFAULT}),
        map(
            rule! {
                GITHUB ~ "(" ~ TOKEN ~ ^"=" ~ #literal_string ~ ")"
            },
            |(_, _, _, _, github_token, _)| DatabaseEngine::Github(github_token),
        ),
    ));

    map(
        rule! {
            ENGINE ~ ^"=" ~ ^#engine
        },
        |(_, _, engine)| engine,
    )(i)
}

pub fn values_tokens(i: Input) -> IResult<&[Token]> {
    let semicolon_pos = i
        .iter()
        .position(|token| token.text() == ";")
        .unwrap_or(i.len() - 1);
    let (value_tokens, rest_tokens) = i.0.split_at(semicolon_pos);
    Ok((Input(rest_tokens, i.1), value_tokens))
}

pub fn role_option(i: Input) -> IResult<RoleOption> {
    alt((
        value(RoleOption::TenantSetting, rule! { TENANTSETTING }),
        value(RoleOption::NoTenantSetting, rule! { NOTENANTSETTING }),
        value(RoleOption::ConfigReload, rule! { CONFIGRELOAD }),
        value(RoleOption::NoConfigReload, rule! { NOCONFIGRELOAD }),
    ))(i)
}

pub fn user_identity(i: Input) -> IResult<UserIdentity> {
    map(
        rule! {
            #literal_string ~ ( "@" ~ #literal_string )?
        },
        |(username, opt_hostname)| UserIdentity {
            username,
            hostname: opt_hostname
                .map(|(_, hostname)| hostname)
                .unwrap_or_else(|| "%".to_string()),
        },
    )(i)
}

pub fn auth_type(i: Input) -> IResult<AuthType> {
    alt((
        value(AuthType::NoPassword, rule! { NO_PASSWORD }),
        value(AuthType::Sha256Password, rule! { SHA256_PASSWORD }),
        value(AuthType::DoubleSha1Password, rule! { DOUBLE_SHA1_PASSWORD }),
        value(AuthType::JWT, rule! { JWT }),
    ))(i)
}

// parse: (k = v ...)* into a map
pub fn options(i: Input) -> IResult<BTreeMap<String, String>> {
    let ident_to_string = |i| {
        map_res(ident, |ident| {
            if ident.quote.is_none() {
                Ok(ident.to_string())
            } else {
                Err(ErrorKind::Other(
                    "unexpected quoted identifier, try to remove the quote",
                ))
            }
        })(i)
    };

    let ident_with_format = alt((
        ident_to_string,
        map(rule! { FORMAT }, |_| "FORMAT".to_string()),
    ));

    map(
        rule! {
            "(" ~ ( #ident_with_format ~ "=" ~ (#ident_to_string | #literal_string) )* ~ ")"
        },
        |(_, opts, _)| BTreeMap::from_iter(opts.iter().map(|(k, _, v)| (k.clone(), v.clone()))),
    )(i)
}
