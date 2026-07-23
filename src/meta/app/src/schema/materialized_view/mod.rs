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

use databend_common_expression::TableSchema;
use databend_meta_client::types::SeqV;

use super::TableMeta;
use crate::app_error::AppError;
use crate::app_error::InvalidMaterializedView;

mod mv_definition_ident;
mod source_table_mv_ids_ident;

pub use mv_definition_ident::MVDefinitionIdent;
pub use mv_definition_ident::MVDefinitionResource;
pub use source_table_mv_ids_ident::SourceTableMVIdsIdent;
pub use source_table_mv_ids_ident::SourceTableMVIdsResource;

pub const MATERIALIZED_VIEW_ENGINE: &str = "MATERIALIZED_VIEW";
/// Internal table option containing the source table ID of a materialized view.
pub const OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID: &str = "materialized_view_source_table_id";

pub fn is_materialized_view_engine(engine: &str) -> bool {
    engine == MATERIALIZED_VIEW_ENGINE
}

impl TableMeta {
    /// Return the source table ID required by a materialized view.
    pub fn materialized_view_source_table_id(&self) -> Result<u64, AppError> {
        let source_table_id = self
            .options
            .get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID)
            .ok_or_else(|| {
                AppError::InvalidMaterializedView(InvalidMaterializedView::new(format!(
                    "missing required table option {OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID}"
                )))
            })?;

        source_table_id.parse::<u64>().map_err(|_| {
            AppError::InvalidMaterializedView(InvalidMaterializedView::new(format!(
                "invalid table option {OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID}: '{source_table_id}'"
            )))
        })
    }
}

/// Definition associated with a materialized-view table.
///
/// A materialized view reuses table metadata and storage for its materialized
/// data, and its table ID is also its materialized view ID. [`TableMeta`]
/// describes how the data is stored, while this record stores the defining
/// query and the columns returned to users under the same table ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MVDefinition {
    pub original_query: String,
    pub query: String,
    pub logical_schema: TableSchema,
    pub sync_creation: bool,
}

/// Materialized-view metadata supplied only while creating a table.
///
/// `definition` is persisted as [`MVDefinition`]. `source_index_seq` binds that
/// definition to the source metadata observed while binding and is used as a
/// transaction condition; it is not persisted in the MV `TableMeta`.
/// A source `TableMeta` sequence is intentionally not carried here because
/// ordinary source-table writes advance it without invalidating the bound
/// schema. `create_table` reads the current source `TableMeta` itself to reject
/// a missing or dropped source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateMaterializedViewMeta {
    pub definition: MVDefinition,
    /// Sequence of [`SourceTableMVIds`] observed while binding the definition.
    ///
    /// `txn_replace_exact` protects changes after `create_table` reads the
    /// index. This sequence additionally covers the earlier binding-to-Meta
    /// window: a definition-changing source DDL clears the index and advances
    /// its sequence, rejecting a definition bound before the DDL.
    pub source_index_seq: u64,
}

/// Complete metadata needed to use one materialized view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MVInfo {
    pub mv_id: u64,
    pub definition: SeqV<MVDefinition>,
    pub table_meta: SeqV<TableMeta>,
}

/// Reverse index from a source table to its dependent materialized-view table IDs.
///
/// All relationships for one source are stored in one value so its KV sequence
/// detects concurrent membership changes and source-DDL invalidation. With one
/// key per relationship, a relationship inserted after a prefix scan would not
/// change any key observed by that scan.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SourceTableMVIds {
    mv_ids: Vec<u64>,
}

impl SourceTableMVIds {
    pub fn from_ids(mv_ids: Vec<u64>) -> Self {
        let mut source_mv_ids = Self::default();
        for mv_id in mv_ids {
            source_mv_ids.add(mv_id);
        }
        source_mv_ids
    }

    pub fn mv_ids(&self) -> &[u64] {
        &self.mv_ids
    }

    pub fn add(&mut self, mv_id: u64) {
        if !self.mv_ids.contains(&mv_id) {
            self.mv_ids.push(mv_id);
        }
    }

    pub fn remove(&mut self, mv_id: u64) {
        self.mv_ids.retain(|id| *id != mv_id);
    }
}

#[cfg(test)]
mod tests {
    use super::OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID;
    use crate::schema::TableMeta;

    #[test]
    fn test_materialized_view_source_table_id() {
        let mut table_meta = TableMeta::default();

        assert!(table_meta.materialized_view_source_table_id().is_err());

        table_meta.options.insert(
            OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID.to_string(),
            "invalid".to_string(),
        );
        assert!(table_meta.materialized_view_source_table_id().is_err());

        table_meta.options.insert(
            OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID.to_string(),
            "42".to_string(),
        );
        assert_eq!(table_meta.materialized_view_source_table_id().unwrap(), 42);
    }
}
