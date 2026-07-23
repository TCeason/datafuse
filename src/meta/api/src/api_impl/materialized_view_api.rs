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

//! Materialized-view metadata reuses the ordinary table lifecycle and adds an
//! immutable definition plus a versioned source-table reverse index.
//!
//! Key-value layout:
//! - `__fd_materialized_view_definition/<tenant>/<mv_table_id>` ->
//!   [`MVDefinition`]
//! - `__fd_materialized_view_by_source/<tenant>/<source_table_id>` ->
//!   `SourceTableMVIds [mv_table_id, ...]`
//! - `__fd_table_by_id/<mv_table_id>` -> `TableMeta`; an MV reuses ordinary
//!   table metadata and storage, and its table ID is also its MV ID.
//! - `TableMeta.options["materialized_view_source_table_id"]` stores the
//!   immutable source-table ID. It is not a source snapshot location.
//! - [`MVDefinition::sync_creation`] selects the immutable maintenance mode.
//!   Changing it requires recreating the MV.
//!
//! CREATE and source-DDL concurrency:
//! - Query-layer integration is a follow-up. While binding the definition, the
//!   Binder must read the source index through `TableContext`/`Catalog`, not by
//!   depending on this Meta API directly. The catalog implementation calls
//!   `MaterializedViewApi::get_source_table_mv_ids`, and the bound plan records
//!   the returned `SeqV::seq`.
//! - The CREATE interpreter copies that bound sequence into
//!   `CreateMaterializedViewMeta { definition, source_index_seq }`; it must not
//!   fetch a new sequence after binding, because doing so would miss a source
//!   DDL committed between binding and interpretation. V1 requires the MV and
//!   source to use the same database.
//! - `TableApi::create_table` gets the source table ID from `TableMeta.options`
//!   and checks that its `TableMeta` is present and not dropped.
//! - `publish_mv_to_source_table_index` requires the current reverse-index
//!   sequence to equal the bound plan's `source_index_seq`, updates the complete
//!   `SourceTableMVIds`, and adds it through `txn_replace_exact`.
//! - The reverse-index condition is committed with the MV `TableMeta`,
//!   `MVDefinition`, and table-name mappings in one transaction. Source
//!   `TableMeta` seq is deliberately not a condition: ordinary INSERT advances
//!   it without invalidating the definition. CREATE publishes a visible empty
//!   MV and records no source snapshot.
//! - Source-DDL invalidation is a required follow-up contract, not implemented
//!   in this module. RENAME and ADD/DROP/MODIFY/RENAME COLUMN must replace
//!   `SourceTableMVIdsIdent(tenant, source_table_id)` with a retained empty
//!   `SourceTableMVIds` through `txn_replace_exact` in the same transaction as
//!   the source change. The advanced sequence rejects a CREATE bound before
//!   such a DDL, even if Meta receives CREATE after the DDL has committed.
//! - If CREATE changes the index after DDL reads it, the DDL transaction fails
//!   its exact-sequence condition and must rebuild after rereading the index;
//!   its retry then applies the DDL result to the newly created relationship.
//!   RENAME and column DDL leave the index empty. Query and refresh must treat
//!   an MV absent from this index as invalid; renaming the source back does not
//!   restore the relationship.
//! - DROP source does not rewrite the index. The source and MV to
//!   share a database: `create_table` and `drop_table_by_id` both condition on
//!   and update that database's `DatabaseId` KV, so an overlapping transaction
//!   retries and then observes the source `TableMeta::drop_on`. Keeping the
//!   index unchanged preserves the relationships for UNDROP.
//!
//! Replacement, DROP, and GC:
//! - CREATE OR REPLACE first calls `construct_drop_table_txn_operations` with
//!   `remove_mv_source_index = false`. It deletes the old `MVDefinition` and
//!   marks the old MV dropped. `publish_mv_to_source_table_index` then removes
//!   the old MV ID and adds the new ID in the same CREATE transaction, including
//!   when the source table changes.
//! - DROP MV calls `construct_drop_table_txn_operations` with
//!   `remove_mv_source_index = true`; the same transaction deletes its
//!   definition, removes its ID from `SourceTableMVIds`, and marks it dropped.
//! - DROP source table retains `SourceTableMVIds`, so UNDROP restores the same
//!   relationships. Source-table GC deletes the reverse-index key.
//! - Table GC deletes ordinary table metadata. When the table engine is
//!   `MATERIALIZED_VIEW`, it also idempotently deletes `MVDefinition`, because
//!   database GC may bypass the per-table DROP path.
//!
//! Source writes:
//! - `mget_mvs_by_source_table_id` reads `SourceTableMVIds`, then fetches every
//!   listed `MVDefinition` and MV `TableMeta`. Writers select synchronous MVs
//!   and commit the already-planned source and MV targets with
//!   `TableApi::update_multi_table_meta`.
//! - Source writes intentionally do not condition their commit on the index
//!   sequence: an MV created after planning is not included, while a dropped or
//!   replaced MV may receive the in-progress write, matching ordinary
//!   multi-table INSERT behavior while dropped `TableMeta` remains available.

use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::schema::MVDefinitionIdent;
use databend_common_meta_app::schema::MVInfo;
use databend_common_meta_app::schema::SourceTableMVIds;
use databend_common_meta_app::schema::SourceTableMVIdsIdent;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::KvApiExt;
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

    /// Get the complete source-to-MV index with its KV sequence.
    ///
    /// Query-layer integration should expose this through `TableContext` or
    /// `Catalog`. The Binder stores the returned sequence in the bound plan,
    /// and the CREATE interpreter later copies it to
    /// `CreateMaterializedViewMeta::source_index_seq`. A missing index is
    /// represented as sequence 0 with an empty [`SourceTableMVIds`].
    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_source_table_mv_ids(
        &self,
        tenant: &Tenant,
        source_table_id: u64,
    ) -> Result<SeqV<SourceTableMVIds>, MetaError> {
        let ident = SourceTableMVIdsIdent::new(tenant, source_table_id);
        Ok(self
            .get_pb(&ident)
            .await?
            .unwrap_or_else(|| SeqV::new(0, SourceTableMVIds::default())))
    }

    /// Get the MVs that depend on a source table.
    ///
    /// The result includes both synchronous and asynchronous MVs. Each [`MVInfo`]
    /// contains its definition and `TableMeta`. A source-table write includes
    /// only MVs whose [`MVDefinition::sync_creation`] is true; scheduled
    /// refreshes maintain those for which it is false.
    ///
    /// Definitions and table metadata are fetched in one `mget_kv` request;
    /// incomplete MVs are omitted with a warning. The query binding path should
    /// use the narrower source-index API through its catalog abstraction.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn mget_mvs_by_source_table_id(
        &self,
        tenant: &Tenant,
        source_table_id: u64,
    ) -> Result<Vec<MVInfo>, MetaError> {
        let source_mv_ids = self
            .get_source_table_mv_ids(tenant, source_table_id)
            .await?;
        let mv_ids = source_mv_ids.data.mv_ids();
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
