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

use crate::schema::EmptyProto;
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::resource::TenantResource;

/// `__fd_materialized_view_source_binding_version/<tenant>/<source_table_id>`
/// -> [`EmptyProto`]
pub type MVSourceBindingVersionIdent = TIdent<MVSourceBindingVersionResource, u64>;

pub struct MVSourceBindingVersionResource;

impl TenantResource for MVSourceBindingVersionResource {
    const PREFIX: &'static str = "__fd_materialized_view_source_binding_version";
    const TYPE: &'static str = "MVSourceBindingVersionIdent";
    const HAS_TENANT: bool = true;
    type ValueType = EmptyProto;
}

#[cfg(test)]
mod tests {
    use databend_meta_client::kvapi::testing::assert_round_trip;

    use super::MVSourceBindingVersionIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_mv_source_binding_version_ident() {
        let ident = MVSourceBindingVersionIdent::new(Tenant::new_literal("tenant1"), 42);
        assert_round_trip(
            ident,
            "__fd_materialized_view_source_binding_version/tenant1/42",
        );
    }
}
