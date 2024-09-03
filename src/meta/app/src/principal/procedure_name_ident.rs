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

use crate::tenant_key::ident::TIdent;
use crate::tenant_key::raw::TIdentRaw;

pub type ProcedureNameIdent = TIdent<ProcedureName>;
pub type ProcedureNameIdentRaw = TIdentRaw<ProcedureName>;

pub use kvapi_impl::ProcedureName;

impl ProcedureNameIdent {
    pub fn procedure_name(&self) -> &str {
        self.name()
    }
}

impl ProcedureNameIdentRaw {
    pub fn procedure_name(&self) -> &str {
        self.name()
    }
}

mod kvapi_impl {
    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::Key;

    use crate::principal::procedure_id_ident::ProcedureId;
    use crate::principal::ProcedureNameIdent;
    use crate::tenant_key::resource::TenantResource;
    use crate::KeyWithTenant;

    pub struct ProcedureName;
    impl TenantResource for ProcedureName {
        const PREFIX: &'static str = "__fd_procedure";
        const HAS_TENANT: bool = true;
        type ValueType = ProcedureId;
    }

    impl kvapi::Value for ProcedureId {
        type KeyType = ProcedureNameIdent;
        fn dependency_keys(&self, key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            [self.into_t_ident(key.tenant()).to_string_key()]
        }
    }

    // // Use these error types to replace usage of ErrorCode if possible.
    // impl From<ExistError<Resource>> for ErrorCode {
    // impl From<UnknownError<Resource>> for ErrorCode {
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use super::ProcedureNameIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_ident() {
        let tenant = Tenant::new_literal("test");
        let ident = ProcedureNameIdent::new(tenant, "test1");

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_procedure/test/test1");

        assert_eq!(ident, ProcedureNameIdent::from_str_key(&key).unwrap());
    }
}