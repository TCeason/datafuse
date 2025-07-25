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

//! A builder to build a [`DB`] from a series of sorted key-value.

use std::io;
use std::path::Path;
use std::sync::Arc;

use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::sys_data::SysData;
use futures::Stream;
use futures_util::TryStreamExt;
use rotbl::storage::impls::fs::FsStorage;
use rotbl::v001::Rotbl;
use rotbl::v001::RotblMeta;
use rotbl::v001::SeqMarked;

use crate::leveled_store::leveled_map::LeveledMap;
use crate::sm_v003::open_snapshot::OpenSnapshot;
#[cfg(doc)]
use crate::sm_v003::SnapshotStoreV004;
use crate::snapshot_config::SnapshotConfig;
use crate::state_machine::MetaSnapshotId;

/// Builds a snapshot from series of key-value in `(String, SeqMarked)`
pub(crate) struct DBBuilder {
    rotbl_builder: rotbl::v001::Builder<FsStorage>,
}

impl DBBuilder {
    pub fn new<P: AsRef<Path>>(
        storage_path: P,
        rel_path: &str,
        rotbl_config: rotbl::v001::Config,
    ) -> Result<Self, io::Error> {
        let storage = FsStorage::new(storage_path.as_ref().to_path_buf());

        let rel_path = rel_path.to_string();

        let inner = rotbl::v001::Builder::new(storage, rotbl_config, &rel_path)?;

        let b = Self {
            rotbl_builder: inner,
        };

        Ok(b)
    }

    /// Append a key-value pair to the builder, the keys must be sorted.
    pub fn append_kv(&mut self, k: String, v: SeqMarked) -> Result<(), io::Error> {
        self.rotbl_builder.append_kv(k, v)
    }

    /// This method is only used for test
    #[allow(dead_code)]
    pub async fn append_kv_stream(
        &mut self,
        mut strm: impl Stream<Item = Result<(String, SeqMarked), io::Error>>,
    ) -> Result<(), io::Error> {
        let mut strm = std::pin::pin!(strm);
        while let Some((k, v)) = strm.try_next().await? {
            self.append_kv(k, v)?;
        }
        Ok(())
    }

    /// Commit the building to the provided [`SnapshotStoreV004`].
    pub fn commit_to_snapshot_store(
        self,
        snapshot_config: &SnapshotConfig,
        snapshot_id: MetaSnapshotId,
        sys_data: SysData,
    ) -> Result<DB, io::Error> {
        let config = self.rotbl_builder.config().clone();
        let storage_path = self.rotbl_builder.storage().base_dir_str().to_string();

        let (current_rel_path, _r) = self.commit(sys_data)?;

        let current_path = format!("{}/{}", storage_path, current_rel_path);

        let (_, rel_path) =
            snapshot_config.move_to_final_path(&current_path, snapshot_id.to_string())?;

        let db = DB::open_snapshot(&storage_path, &rel_path, snapshot_id.to_string(), config)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "{}; when:(open snapshot at: {}/{})",
                        e, storage_path, rel_path
                    ),
                )
            })?;

        Ok(db)
    }

    /// Commit the building.
    pub(crate) fn commit(self, sys_data: SysData) -> Result<(String, Rotbl), io::Error> {
        let meta = serde_json::to_string(&sys_data).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("serialize sys_data failed: {}", e),
            )
        })?;

        // the first arg `seq` is not used.
        let rotbl_meta = RotblMeta::new(0, meta);

        // The relative path of the built rotbl.
        let rel_path = self.rotbl_builder.rel_path().to_string();

        let r = self.rotbl_builder.commit(rotbl_meta)?;

        Ok((rel_path, r))
    }

    /// Build a [`DB`] from a leveled map.
    ///
    /// The DB contains all data in the leveled map and the snapshot id is generated by `make_snapshot_id`.
    // Only used by test
    #[allow(dead_code)]
    pub async fn build_from_leveled_map(
        mut self,
        lm: &mut LeveledMap,
        make_snapshot_id: impl FnOnce(&SysData) -> String + Send,
    ) -> Result<DB, io::Error> {
        lm.freeze_writable();

        let mut compacter = lm.acquire_compactor().await;
        let (sys_data, strm) = compacter.compact_into_stream().await?;

        self.append_kv_stream(strm).await?;

        let storage_path = self.storage_path().to_string();

        let snapshot_id = make_snapshot_id(&sys_data);
        let (rel_path, r) = self.commit(sys_data)?;
        let db = DB::new(storage_path, rel_path, snapshot_id, Arc::new(r))?;

        Ok(db)
    }

    /// Returns the base dir of the internal FS-storage.
    fn storage_path(&self) -> &str {
        self.rotbl_builder.storage().base_dir_str()
    }
}
