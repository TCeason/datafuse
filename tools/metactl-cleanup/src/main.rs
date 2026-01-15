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

use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use clap::Parser;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_kvapi::kvapi::KvApiExt;
use databend_common_meta_types::txn_op;
use databend_common_meta_types::TxnDeleteRequest;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use databend_common_version::BUILD_INFO;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;

#[derive(Parser, Debug)]
#[clap(
    name = "metactl-cleanup",
    about = "Batch delete keys from databend-meta"
)]
struct Args {
    /// Meta server address
    #[clap(long, default_value = "127.0.0.1:9191")]
    meta_addr: String,

    /// Tenant name
    #[clap(long)]
    tenant: String,

    /// File containing table IDs to delete (one per line)
    #[clap(long)]
    orphan_ids_file: String,

    /// Batch size for concurrent deletions
    #[clap(long, default_value = "1000")]
    batch_size: usize,

    /// Sleep between batches (milliseconds). Useful to reduce load on meta.
    #[clap(long, default_value = "0")]
    sleep_ms_between_batches: u64,

    /// Dry run - only print what would be deleted
    #[clap(long)]
    dry_run: bool,

    /// Optional checkpoint file path.
    /// If set, the tool will:
    /// - read the last processed count on startup (to resume)
    /// - write updated progress after each completed batch (only when not `--dry-run`)
    ///
    /// The checkpoint value is the number of non-empty IDs already processed from `--orphan-ids-file`.
    #[clap(long)]
    checkpoint_file: Option<String>,

    /// Use meta transaction API to batch delete ownership keys.
    /// This reduces RPC count, but each transaction will include up to `--delete-txn-max-ops` delete operations.
    #[clap(long)]
    delete_txn: bool,

    /// Max delete operations per transaction (only used with `--delete-txn`).
    #[clap(long, default_value = "1000")]
    delete_txn_max_ops: usize,

    /// Recheck table existence before deleting.
    /// If table meta still exists, the ownership key will be skipped.
    #[clap(long)]
    recheck: bool,
}

#[derive(Default)]
struct BatchStats {
    deleted: u64,
    would_delete: u64,
    skipped_still_exists: u64,
    failed: u64,
}

struct RecheckResult {
    exists: Vec<bool>,
    failed: Vec<bool>,
}

async fn recheck_tables_exist_batch(
    client: &Arc<ClientHandle>,
    table_ids: &[String],
) -> anyhow::Result<RecheckResult> {
    const MAX_KEYS_PER_MGET: usize = 4096;

    let mut keys: Vec<String> = Vec::with_capacity(table_ids.len().saturating_mul(2));
    let mut key_to_id_index: Vec<usize> = Vec::with_capacity(table_ids.len().saturating_mul(2));

    for (i, table_id) in table_ids.iter().enumerate() {
        keys.push(format!("__fd_table_by_id/{}", table_id));
        key_to_id_index.push(i);
        keys.push(format!("__fd_table_id_to_name/{}", table_id));
        key_to_id_index.push(i);
    }

    let mut exists = vec![false; table_ids.len()];
    let mut failed = vec![false; table_ids.len()];

    for chunk_start in (0..keys.len()).step_by(MAX_KEYS_PER_MGET) {
        let chunk_end = std::cmp::min(chunk_start + MAX_KEYS_PER_MGET, keys.len());
        let key_chunk = &keys[chunk_start..chunk_end];
        let vals = client
            .mget_kv(key_chunk)
            .await
            .with_context(|| format!("recheck table meta batch (keys={})", key_chunk.len()));

        match vals {
            Ok(vals) => {
                for (j, v) in vals.iter().enumerate() {
                    if v.is_some() {
                        let id_index = key_to_id_index[chunk_start + j];
                        exists[id_index] = true;
                    }
                }
            }
            Err(e) => {
                eprintln!("  Recheck batch failed (keys={}): {:#}", key_chunk.len(), e);
                for id_index in key_to_id_index[chunk_start..chunk_end].iter().copied() {
                    failed[id_index] = true;
                }
            }
        }
    }

    Ok(RecheckResult { exists, failed })
}

async fn process_table_id_batch(
    client: &Arc<ClientHandle>,
    tenant: &str,
    table_ids: Vec<String>,
    dry_run: bool,
    recheck: bool,
    delete_txn: bool,
    delete_txn_max_ops: usize,
) -> anyhow::Result<BatchStats> {
    let mut stats = BatchStats::default();

    let to_process = if recheck {
        let recheck_result = recheck_tables_exist_batch(client, &table_ids).await?;
        let mut ids = Vec::with_capacity(table_ids.len());

        for (i, table_id) in table_ids.into_iter().enumerate() {
            if recheck_result.failed[i] {
                stats.failed += 1;
                continue;
            }
            if recheck_result.exists[i] {
                stats.skipped_still_exists += 1;
                continue;
            }
            ids.push(table_id);
        }

        ids
    } else {
        table_ids
    };

    if dry_run {
        stats.would_delete = to_process.len() as u64;
        return Ok(stats);
    }

    if delete_txn {
        let tenant = tenant.to_string();
        let max_ops = std::cmp::max(1, delete_txn_max_ops);

        for chunk in to_process.chunks(max_ops) {
            let mut ops: Vec<TxnOp> = Vec::with_capacity(chunk.len());

            for table_id in chunk {
                let key = format!("__fd_object_owners/{}/table-by-id/{}", tenant, table_id);
                ops.push(TxnOp {
                    request: Some(txn_op::Request::Delete(TxnDeleteRequest::new(
                        key, false, None,
                    ))),
                });
            }

            let txn = TxnRequest {
                operations: vec![],
                condition: vec![],
                if_then: ops,
                else_then: vec![],
            };

            match client
                .transaction(txn)
                .await
                .with_context(|| format!("transaction delete (ops={})", chunk.len()))
            {
                Ok(reply) => {
                    if reply.success {
                        stats.deleted += chunk.len() as u64;
                    } else {
                        stats.failed += chunk.len() as u64;
                        eprintln!(
                            "  Transaction delete reported success=false (ops={}, execution_path={})",
                            chunk.len(),
                            reply.execution_path
                        );
                    }
                }
                Err(e) => {
                    stats.failed += chunk.len() as u64;
                    eprintln!("  Transaction delete failed (ops={}): {:#}", chunk.len(), e);
                }
            }
        }

        return Ok(stats);
    }

    let mut futures = FuturesUnordered::new();
    let tenant = tenant.to_string();

    for table_id in to_process {
        let client_clone = client.clone();
        let key = format!("__fd_object_owners/{}/table-by-id/{}", tenant, &table_id);

        futures.push(async move {
            client_clone
                .upsert_kv(UpsertKV::delete(&key))
                .await
                .with_context(|| format!("delete ownership key {}", key))?;
            Ok::<(), anyhow::Error>(())
        });
    }

    while let Some(result) = futures.next().await {
        match result {
            Ok(()) => stats.deleted += 1,
            Err(e) => {
                stats.failed += 1;
                eprintln!("  Failed: {:#}", e);
            }
        }
    }

    Ok(stats)
}

fn count_and_validate_ids(path: &str) -> anyhow::Result<usize> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut count = 0usize;

    for (line_no, line) in reader.lines().enumerate() {
        let raw = line?;
        let trimmed = raw.trim();

        if trimmed.is_empty() {
            continue;
        }

        if !trimmed.as_bytes().iter().all(|b| b.is_ascii_digit()) {
            anyhow::bail!(
                "Invalid table_id at {}:{}: {:?} (expect digits only)",
                path,
                line_no + 1,
                trimmed
            );
        }

        count += 1;
    }

    Ok(count)
}

fn read_checkpoint(path: &str) -> anyhow::Result<usize> {
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("read checkpoint file {}", path))?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(0);
    }
    let processed = trimmed
        .parse::<usize>()
        .with_context(|| format!("parse checkpoint value in {}", path))?;
    Ok(processed)
}

fn write_checkpoint(path: &str, processed: usize) -> anyhow::Result<()> {
    let parent = Path::new(path).parent();
    if let Some(parent) = parent {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create checkpoint dir {}", parent.display()))?;
        }
    }

    let tmp_path = format!("{}.tmp", path);
    std::fs::write(&tmp_path, format!("{}\n", processed))
        .with_context(|| format!("write checkpoint temp file {}", tmp_path))?;
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("rename checkpoint temp file to {}", path))?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    println!("Configuration:");
    println!("  Meta address: {}", args.meta_addr);
    println!("  Tenant: {}", args.tenant);
    println!("  Orphan IDs file: {}", args.orphan_ids_file);
    println!("  Batch size: {}", args.batch_size);
    println!(
        "  Sleep between batches: {}ms",
        args.sleep_ms_between_batches
    );
    println!("  Dry run: {}", args.dry_run);
    println!(
        "  Checkpoint file: {}",
        args.checkpoint_file.as_deref().unwrap_or("<disabled>")
    );
    println!("  Delete via transaction: {}", args.delete_txn);
    println!("  Delete txn max ops: {}", args.delete_txn_max_ops);
    println!("  Recheck: {}", args.recheck);
    println!();

    if args.batch_size == 0 {
        anyhow::bail!("--batch-size must be greater than 0");
    }
    if args.delete_txn && args.delete_txn_max_ops == 0 {
        anyhow::bail!("--delete-txn-max-ops must be greater than 0");
    }

    // Count IDs (and validate format) first, to show total progress.
    println!("Reading orphan table IDs from: {}", args.orphan_ids_file);
    let total = count_and_validate_ids(&args.orphan_ids_file)?;
    println!("Total orphan table IDs to process: {}", total);

    if total == 0 {
        println!("Nothing to do.");
        return Ok(());
    }

    let mut resume_processed = 0usize;
    if let Some(ref checkpoint_file) = args.checkpoint_file {
        resume_processed = if Path::new(checkpoint_file).exists() {
            read_checkpoint(checkpoint_file)?
        } else {
            0
        };
        if resume_processed > total {
            anyhow::bail!(
                "Checkpoint value {} exceeds total IDs {}. Check --checkpoint-file {} and --orphan-ids-file {}",
                resume_processed,
                total,
                checkpoint_file,
                args.orphan_ids_file
            );
        }
        if resume_processed > 0 {
            println!(
                "Resuming from checkpoint: already processed {}/{} IDs",
                resume_processed, total
            );
        }
    }

    // Create meta client
    println!("\nConnecting to meta: {}", args.meta_addr);
    let client = MetaGrpcClient::try_create(
        vec![args.meta_addr.clone()],
        &BUILD_INFO,
        "root",
        "xxx",
        Some(Duration::from_secs(10)),
        Some(Duration::from_secs(5)),
        None,
    )?;

    let mut total_deleted = 0u64;
    let mut total_would_delete = 0u64;
    let mut total_skipped_still_exists = 0u64;
    let mut total_failed = 0u64;
    let mut processed = resume_processed;

    println!(
        "Starting {} (batch size: {})",
        if args.dry_run {
            "dry run"
        } else {
            "batch deletion"
        },
        args.batch_size
    );

    let total_batches = (total + args.batch_size - 1) / args.batch_size;
    let file = File::open(&args.orphan_ids_file)?;
    let reader = BufReader::new(file);
    let mut batch: Vec<String> = Vec::with_capacity(args.batch_size);
    let mut skipped_by_checkpoint = 0usize;

    // Process in batches
    for line in reader.lines() {
        let table_id = line?.trim().to_string();
        if table_id.is_empty() {
            continue;
        }

        if skipped_by_checkpoint < resume_processed {
            skipped_by_checkpoint += 1;
            continue;
        }

        batch.push(table_id);

        if batch.len() < args.batch_size {
            continue;
        }

        let batch_num = (processed / args.batch_size) + 1;
        let batch_start = processed;
        let batch_end = processed + batch.len();
        println!(
            "\nProcessing batch {}/{} (items {}-{})...",
            batch_num,
            total_batches,
            batch_start + 1,
            batch_end
        );

        let ids = std::mem::replace(&mut batch, Vec::with_capacity(args.batch_size));
        let stats = process_table_id_batch(
            &client,
            &args.tenant,
            ids,
            args.dry_run,
            args.recheck,
            args.delete_txn,
            args.delete_txn_max_ops,
        )
        .await?;
        total_deleted += stats.deleted;
        total_would_delete += stats.would_delete;
        total_skipped_still_exists += stats.skipped_still_exists;
        total_failed += stats.failed;

        println!(
            "  Batch complete. Progress: processed {}/{}; deleted {}; would-delete {}; skipped {}; failed {}",
            batch_end,
            total,
            total_deleted,
            total_would_delete,
            total_skipped_still_exists,
            total_failed
        );

        processed = batch_end;
        if let Some(ref checkpoint_file) = args.checkpoint_file {
            if !args.dry_run {
                write_checkpoint(checkpoint_file, processed)?;
            }
        }

        if args.sleep_ms_between_batches > 0 && batch_num < total_batches {
            println!(
                "  Sleeping {}ms before next batch...",
                args.sleep_ms_between_batches
            );
            tokio::time::sleep(Duration::from_millis(args.sleep_ms_between_batches)).await;
        }
    }

    // Process remaining items in the last batch (if any).
    if !batch.is_empty() {
        let batch_num = (processed / args.batch_size) + 1;
        let batch_start = processed;
        let batch_end = processed + batch.len();
        println!(
            "\nProcessing batch {}/{} (items {}-{})...",
            batch_num,
            total_batches,
            batch_start + 1,
            batch_end
        );

        let ids = std::mem::replace(&mut batch, Vec::with_capacity(args.batch_size));
        let stats = process_table_id_batch(
            &client,
            &args.tenant,
            ids,
            args.dry_run,
            args.recheck,
            args.delete_txn,
            args.delete_txn_max_ops,
        )
        .await?;
        total_deleted += stats.deleted;
        total_would_delete += stats.would_delete;
        total_skipped_still_exists += stats.skipped_still_exists;
        total_failed += stats.failed;

        println!(
            "  Batch complete. Progress: processed {}/{}; deleted {}; would-delete {}; skipped {}; failed {}",
            batch_end,
            total,
            total_deleted,
            total_would_delete,
            total_skipped_still_exists,
            total_failed
        );

        processed = batch_end;
        if let Some(ref checkpoint_file) = args.checkpoint_file {
            if !args.dry_run {
                write_checkpoint(checkpoint_file, processed)?;
            }
        }
    }

    println!("\n========================================");
    println!(
        "{} complete!",
        if args.dry_run { "Dry run" } else { "Cleanup" }
    );
    if args.dry_run {
        println!("  Would delete: {}", total_would_delete);
    } else {
        println!("  Successfully deleted: {}", total_deleted);
    }
    println!(
        "  Skipped (table still exists): {}",
        total_skipped_still_exists
    );
    println!("  Failed: {}", total_failed);
    println!("  Total processed: {}", processed);
    println!("========================================");

    Ok(())
}
