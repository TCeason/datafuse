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

use common_ast::ast::AuthOption;
use common_ast::ast::CreateUserStmt;
use common_ast::ast::GrantLevel;
use common_ast::ast::GrantSource;
use common_ast::ast::GrantStatement;
use common_ast::ast::RoleOption;
use common_exception::Result;
use common_meta_types::AuthInfo;
use common_meta_types::GrantObject;
use common_meta_types::UserIdentity;
use common_meta_types::UserOption;
use common_meta_types::UserPrivilegeSet;
use common_planners::AlterUserPlan;
use common_planners::CreateUserPlan;
use common_planners::GrantPrivilegePlan;
use common_planners::GrantRolePlan;
use common_planners::RevokePrivilegePlan;
use common_planners::RevokeRolePlan;

use crate::sql::plans::Plan;
use crate::sql::Binder;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_grant(
        &mut self,
        stmt: &GrantStatement,
    ) -> Result<Plan> {
        let GrantStatement { source, principal } = stmt;

        match source {
            GrantSource::Role { role } => {
                let plan = GrantRolePlan {
                    principal: principal.clone(),
                    role: role.clone(),
                };
                Ok(Plan::GrantRole(Box::new(plan)))
            }
            GrantSource::ALL { level } => {
                // ALL PRIVILEGES have different available privileges set on different grant objects
                // Now in this case all is always true.
                let grant_object = self.convert_to_grant_object(level);
                let priv_types = grant_object.available_privileges();
                let plan = GrantPrivilegePlan {
                    principal: principal.clone(),
                    on: grant_object,
                    priv_types,
                };
                Ok(Plan::GrantPriv(Box::new(plan)))
            }
            GrantSource::Privs { privileges, level } => {
                let grant_object = self.convert_to_grant_object(level);
                let mut priv_types = UserPrivilegeSet::empty();
                for x in privileges {
                    priv_types.set_privilege(*x);
                }
                let plan = GrantPrivilegePlan {
                    principal: principal.clone(),
                    on: grant_object,
                    priv_types,
                };
                Ok(Plan::GrantPriv(Box::new(plan)))
            }
        }
    }

    pub(in crate::sql::planner::binder) async fn bind_revoke(
        &mut self,
        stmt: &GrantStatement,
    ) -> Result<Plan> {
        let GrantStatement { source, principal } = stmt;

        match source {
            GrantSource::Role { role } => {
                let plan = RevokeRolePlan {
                    principal: principal.clone(),
                    role: role.clone(),
                };
                Ok(Plan::RevokeRole(Box::new(plan)))
            }
            GrantSource::ALL { level } => {
                // ALL PRIVILEGES have different available privileges set on different grant objects
                // Now in this case all is always true.
                let grant_object = self.convert_to_grant_object(level);
                let priv_types = grant_object.available_privileges();
                let plan = RevokePrivilegePlan {
                    principal: principal.clone(),
                    on: grant_object,
                    priv_types,
                };
                Ok(Plan::RevokePriv(Box::new(plan)))
            }
            GrantSource::Privs { privileges, level } => {
                let grant_object = self.convert_to_grant_object(level);
                let mut priv_types = UserPrivilegeSet::empty();
                for x in privileges {
                    priv_types.set_privilege(*x);
                }
                let plan = RevokePrivilegePlan {
                    principal: principal.clone(),
                    on: grant_object,
                    priv_types,
                };
                Ok(Plan::RevokePriv(Box::new(plan)))
            }
        }
    }

    //Copy from query/src/sql/statements/statement_grant.rs
    pub(in crate::sql::planner::binder) fn convert_to_grant_object(
        &self,
        source: &GrantLevel,
    ) -> GrantObject {
        // TODO fetch real catalog
        let catalog_name = self.ctx.get_current_catalog();
        match source {
            GrantLevel::Global => GrantObject::Global,
            GrantLevel::Table(database_name, table_name) => {
                let database_name = database_name
                    .clone()
                    .unwrap_or_else(|| self.ctx.get_current_database());
                GrantObject::Table(catalog_name, database_name, table_name.clone())
            }
            GrantLevel::Database(database_name) => {
                let database_name = database_name
                    .clone()
                    .unwrap_or_else(|| self.ctx.get_current_database());
                GrantObject::Database(catalog_name, database_name)
            }
        }
    }

    pub(in crate::sql::planner::binder) async fn bind_create_user(
        &mut self,
        stmt: &CreateUserStmt,
    ) -> Result<Plan> {
        let mut user_option = UserOption::default();
        for option in &stmt.role_options {
            option.apply(&mut user_option);
        }
        let plan = CreateUserPlan {
            user: stmt.user.clone(),
            auth_info: AuthInfo::create2(&stmt.auth_option.auth_type, &stmt.auth_option.password)?,
            user_option,
            if_not_exists: stmt.if_not_exists,
        };
        Ok(Plan::CreateUser(Box::new(plan)))
    }

    pub(in crate::sql::planner::binder) async fn bind_alter_user(
        &mut self,
        user: &Option<UserIdentity>,
        auth_option: &Option<AuthOption>,
        role_options: &Vec<RoleOption>,
    ) -> Result<Plan> {
        // None means current user
        let user_info = if user.is_none() {
            self.ctx.get_current_user()?
        } else {
            self.ctx
                .get_user_manager()
                .get_user(&self.ctx.get_tenant(), user.clone().unwrap())
                .await?
        };

        // None means no change to make
        let new_auth_info = if let Some(auth_option) = &auth_option {
            let auth_info = user_info
                .auth_info
                .alter2(&auth_option.auth_type, &auth_option.password)?;
            if user_info.auth_info == auth_info {
                None
            } else {
                Some(auth_info)
            }
        } else {
            None
        };

        let mut user_option = user_info.option.clone();
        for option in role_options {
            option.apply(&mut user_option);
        }
        let new_user_option = if user_option == user_info.option {
            None
        } else {
            Some(user_option)
        };
        let plan = AlterUserPlan {
            user: user_info.identity(),
            auth_info: new_auth_info,
            user_option: new_user_option,
        };

        Ok(Plan::AlterUser(Box::new(plan)))
    }
}
