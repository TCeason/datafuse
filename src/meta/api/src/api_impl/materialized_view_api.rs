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

//! An MV reuses ordinary `TableMeta`; its table ID is also its MV ID.
//!
//! ```text
//! __fd_table_by_id/<mv_id>
//!     -> TableMeta
//!        options["materialized_view_source_table_id"] = <source_id>
//! __fd_materialized_view_definition/<tenant>/<mv_id>
//!     -> MVDefinition
//! __fd_materialized_view_by_source/<tenant>/<source_id>/<mv_id>
//!     -> EmptyProto
//! __fd_materialized_view_source_binding_version/<tenant>/<source_id>
//!     -> EmptyProto
//! ```
//!
//! CREATE/DROP MV only puts/deletes
//! `SourceTableMVIdent(tenant, source_id, mv_id)`. RENAME and schema-changing
//! source-table DDL delete every `SourceTableMVIdent` under the source-table
//! relationship prefix and
//! rewrite `MVSourceBindingVersionIdent(tenant, source_id)` to advance its KV
//! sequence.
//!
//! Query binding must return a `TableInfo` and binding-version sequence from one
//! stable read window. Query-layer integration is a follow-up; its
//! `TableContext`/`Catalog` implementation follows this read/check loop:
//!
//! ```text
//! loop {
//!     (name_seq_1, source_id_1) = get(source_name_ident);
//!     version_seq_1 = get_seq(MVSourceBindingVersionIdent(source_id_1));
//!     source_meta = get(TableId(source_id_1));
//!     (name_seq_2, source_id_2) = get(source_name_ident);
//!     version_seq_2 = get_seq(MVSourceBindingVersionIdent(source_id_1));
//!
//!     if name_seq_1 == name_seq_2
//!         && source_id_1 == source_id_2
//!         && version_seq_1 == version_seq_2
//!     {
//!         return (source_meta, version_seq_1);
//!     }
//! }
//! ```
//!
//! The CREATE interpreter stores that sequence in
//! `CreateMaterializedViewMeta::source_binding_version_seq`. `create_table`
//! verifies that the source table exists and is not dropped, then appends:
//!
//! ```text
//! txn.condition.push(txn_cond_eq_seq(
//!     &MVSourceBindingVersionIdent::new(tenant, source_id),
//!     source_binding_version_seq,
//! ));
//! txn.if_then.push(txn_put_pb(
//!     &SourceTableMVIdent::new_generic(
//!         tenant,
//!         SourceTableMV::new(source_id, mv_id),
//!     ),
//!     &EmptyProto {},
//! ));
//! // The same txn creates TableMeta, MVDefinition, and table-name mappings.
//! ```
//!
//! CREATE publishes a visible empty MV and records no source-table snapshot.
//! The source-table `TableMeta` seq is not the binding token because INSERT also
//! advances it.
//!
//! Source-table DDL invalidation is a follow-up. RENAME and
//! ADD/DROP/MODIFY/RENAME COLUMN append the following operations to the
//! source-table change transaction:
//!
//! ```text
//! loop {
//!     prefix = DirName(SourceTableMVIdent(tenant, source_id, 0));
//!     relationships = list_pb_vec(prefix);
//!     (version_seq, _) = get_pb_seq_and_value(
//!         MVSourceBindingVersionIdent(source_id),
//!     ); // version_seq is 0 when the key is missing.
//!
//!     txn.condition.push(txn_cond_eq_keys_with_prefix(prefix, relationships.len()));
//!     for relationship in relationships {
//!         txn_delete_exact(txn, relationship.key, relationship.seqv.seq);
//!     }
//!     txn_replace_exact(
//!         txn,
//!         MVSourceBindingVersionIdent(source_id),
//!         version_seq,
//!         EmptyProto {},
//!     );
//!     // The same txn changes the source-table TableMeta or its name mappings.
//!
//!     if send_txn(txn).success {
//!         break;
//!     }
//!     // Retry from list_pb_vec() and get_pb_seq_and_value().
//! }
//! ```
//!
//! Query and refresh use
//! `get_pb(SourceTableMVIdent(tenant, source_id, mv_id)).is_some()` as the MV
//! validity check. Renaming the source table back does not put this relationship
//! key, so an invalidated MV remains invalid.
//!
//! ```text
//! CREATE MV txn:
//!     txn_put_pb(TableId(new_mv_id), new_mv_table_meta)
//!     txn_put_pb(MVDefinitionIdent(tenant, new_mv_id), definition)
//!     txn_put_pb(SourceTableMVIdent(tenant, source_id, new_mv_id), EmptyProto {})
//!
//! REPLACE MV txn:
//!     txn_put_pb(TableId(old_mv_id), old_mv_table_meta_with_drop_on)
//!     txn_del(MVDefinitionIdent(tenant, old_mv_id))
//!     txn_del(SourceTableMVIdent(tenant, old_source_id, old_mv_id))
//!     // Append the CREATE MV operations for new_mv_id.
//!
//! DROP MV txn:
//!     txn_put_pb(TableId(mv_id), mv_table_meta_with_drop_on)
//!     txn_del(MVDefinitionIdent(tenant, mv_id))
//!     txn_del(SourceTableMVIdent(tenant, source_id, mv_id))
//!
//! DROP source table txn:
//!     // Do not delete SourceTableMVIdent(tenant, source_id, *).
//!     // Do not delete MVSourceBindingVersionIdent(tenant, source_id).
//!     // Source UNDROP therefore sees the same relationships and version key.
//!
//! GC MV txn:
//!     // Delete TableId(mv_id) and the other ordinary table-GC metadata.
//!     txn_del(MVDefinitionIdent(tenant, mv_id)) // idempotent for database GC
//!     if let Ok(source_table_id) = table_meta.materialized_view_source_table_id() {
//!         txn_del(SourceTableMVIdent(tenant, source_table_id, mv_id))
//!     } else {
//!         warn // Do not block GC for a malformed MV TableMeta.
//!     }
//!
//! GC source table txn:
//!     for relationship in list(SourceTableMVIdent(tenant, source_id, *)) {
//!         txn_del(relationship)
//!     }
//!     txn_del(MVSourceBindingVersionIdent(tenant, source_id))
//! ```

use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::schema::MVDefinitionIdent;
use databend_common_meta_app::schema::MVInfo;
use databend_common_meta_app::schema::SourceTableMV;
use databend_common_meta_app::schema::SourceTableMVIdent;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::DirName;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnGetResponse;
use databend_meta_client::types::protobuf as pb;
use log::warn;

use crate::deserialize_struct_get_response;
use crate::kv_pb_api::KVPbApi;

/// APIs for metadata that belongs exclusively to materialized views.
#[async_trait::async_trait]
pub trait MaterializedViewApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
{
    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_mv_definition(
        &self,
        tenant: &Tenant,
        mv_table_id: u64,
    ) -> Result<Option<SeqV<MVDefinition>>, MetaError> {
        let ident = MVDefinitionIdent::new(tenant, mv_table_id);
        self.get_pb(&ident).await
    }

    /// List the MVs that depend on a source table.
    ///
    /// ```text
    /// list SourceTableMVIdent(tenant, source_table_id, *) -> mv_table_ids
    /// mget MVDefinitionIdent(mv_table_id) + TableId(mv_table_id) -> MVInfo
    /// ```
    ///
    /// The result contains both maintenance modes.
    /// INSERT should selects `MVDefinition::sync_creation = true`;
    /// scheduled refresh selects `false`.
    /// No collection version is returned. An MV created after the relationship
    /// list is a new empty table, so the current INSERT does not write it. An MV
    /// dropped after the list may still receive the current INSERT while its
    /// dropped `TableMeta` is retained for GC. A definition or `TableMeta`
    /// missing between the list and mget is omitted with a warning.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_mvs_by_source_table_id(
        &self,
        tenant: &Tenant,
        source_table_id: u64,
    ) -> Result<Vec<MVInfo>, MetaError> {
        let source_mv_prefix = DirName::new(SourceTableMVIdent::new_generic(
            tenant,
            SourceTableMV::new(source_table_id, 0),
        ));
        let source_mvs = self
            .list_pb_vec(ListOptions::unlimited(&source_mv_prefix))
            .await?;
        let mv_ids = source_mvs
            .iter()
            .map(|(ident, _)| ident.name().mv_table_id)
            .collect::<Vec<_>>();
        if mv_ids.is_empty() {
            return Ok(vec![]);
        }

        let mut keys = Vec::with_capacity(mv_ids.len() * 2);
        keys.extend(
            mv_ids
                .iter()
                .map(|mv_id| MVDefinitionIdent::new(tenant, *mv_id).to_string_key()),
        );
        keys.extend(
            mv_ids
                .iter()
                .map(|mv_id| TableId::new(*mv_id).to_string_key()),
        );

        let values = self.mget_kv(&keys).await?;
        let mut responses = keys
            .iter()
            .zip(values)
            .map(|(key, value)| TxnGetResponse::new(key, value.map(pb::SeqV::from)))
            .collect::<Vec<_>>();
        let table_meta_responses = responses.split_off(mv_ids.len());
        let definition_responses = responses;
        let mut mvs = Vec::with_capacity(mv_ids.len());

        for (mv_id, (definition_response, table_meta_response)) in mv_ids
            .iter()
            .zip(definition_responses.into_iter().zip(table_meta_responses))
        {
            let (_, definition) =
                deserialize_struct_get_response::<MVDefinitionIdent>(definition_response)?;
            let (_, table_meta) = deserialize_struct_get_response::<TableId>(table_meta_response)?;

            let (Some(definition), Some(table_meta)) = (definition, table_meta) else {
                warn!(
                    "source table {} references MV {} with incomplete metadata",
                    source_table_id, mv_id
                );
                continue;
            };
            mvs.push(MVInfo {
                mv_id: *mv_id,
                definition,
                table_meta,
            });
        }

        Ok(mvs)
    }
}

impl<KV> MaterializedViewApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
