# metactl-cleanup

Batch deletion tool for cleaning up orphan ownership keys in databend-meta.

## Problem

When tables are dropped, their ownership records (`__fd_object_owners/<tenant>/table-by-id/<table_id>`) may remain in meta storage. Over time, these orphan keys can accumulate and cause performance issues, especially when querying `information_schema.columns` which needs to check ownership for visibility.

## Solution

This tool safely identifies and deletes orphan ownership keys in batches with concurrent processing.

## Safety Guarantees

1. **Snapshot Consistency**: Uses two snapshots (existing tables + ownership keys) to identify orphans
2. **Concurrent Safety**: New tables created after taking snapshots won't be in the orphan list
3. **Batch Processing**: Processes deletions in configurable batches to avoid overwhelming meta
4. **Dry Run**: Runs the full workflow without deleting any keys
5. **Recheck**: Optionally rechecks table meta before deletion to avoid accidental removal

## Usage

For step-by-step validation (mock data + real data sampling), see `VALIDATION.md`.

### 1. Prepare the orphan key list

```bash
# Set environment variables (optional, defaults shown)
export TENANT="kffwpt"
export META_ADDR="127.0.0.1:9191"
export QUERY_ADDR="127.0.0.1:8000"
export DB_USER="databend"
export DB_PASS="your_password"
export WORK_DIR="/tmp/metactl-cleanup"

# Run the preparation script
cd /data1/eason/databend/tools/metactl-cleanup
./prepare_orphan_list.sh
```

This will:
- Query `system.tables` to get existing table IDs
- Export ownership keys from meta
- Identify orphan keys (in ownership but not in tables, with table_id < max)
- Save results to `$WORK_DIR/orphan_table_ids.txt`

If you already have ownership keys collected into a file (e.g. from `databend-metactl watch`),
you can skip the export step by providing `OWNERSHIP_KEYS_FILE`:

```bash
export OWNERSHIP_KEYS_FILE="/home/eason/big_data/fd_object_owners_watch.log"
# Optional: auto|keys|watch (default: auto)
export OWNERSHIP_KEYS_FILE_KIND="watch"

./prepare_orphan_list.sh
```

### 2. Review the orphan list

```bash
# Check the count
wc -l /tmp/metactl-cleanup/orphan_table_ids.txt

# Review the IDs
less /tmp/metactl-cleanup/orphan_table_ids.txt
```

### 3. Dry run

```bash
# Smoke test: dry-run only the first 100 IDs (recommended)
head -n 100 /tmp/metactl-cleanup/orphan_table_ids.txt > /tmp/metactl-cleanup/orphan_table_ids_100.txt

cargo run -p metactl-cleanup -- \
  --tenant kffwpt \
  --meta-addr 172.26.39.252:9191 \
  --orphan-ids-file /tmp/metactl-cleanup/orphan_table_ids_100.txt \
  --batch-size 100 \
  --sleep-ms-between-batches 500 \
  --dry-run \
  --recheck
```

### 4. Delete

```bash
cargo run -p metactl-cleanup -- \
  --tenant kffwpt \
  --meta-addr 172.26.39.252:9191 \
  --orphan-ids-file /tmp/metactl-cleanup/orphan_table_ids.txt \
  --batch-size 100 \
  --sleep-ms-between-batches 500 \
  --recheck \
  --checkpoint-file /tmp/metactl-cleanup/checkpoint.txt
```

For release build (faster):

```bash
cargo build --release -p metactl-cleanup

./target/release/metactl-cleanup \
  --tenant kffwpt \
  --meta-addr 172.26.39.252:9191 \
  --orphan-ids-file /tmp/metactl-cleanup/orphan_table_ids.txt \
  --batch-size 100 \
  --sleep-ms-between-batches 500 \
  --recheck \
  --checkpoint-file /tmp/metactl-cleanup/checkpoint.txt
```

For better portability across older Linux distributions (e.g. glibc 2.28), build a static musl binary:

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl -p metactl-cleanup

./target/x86_64-unknown-linux-musl/release/metactl-cleanup --help
```

## Options

- `--tenant`: Tenant name (required)
- `--meta-addr`: Meta server address (default: `127.0.0.1:9191`)
- `--orphan-ids-file`: File containing orphan table IDs, one per line (required)
- `--batch-size`: Number of keys to delete concurrently in each batch (default: `1000`)
- `--sleep-ms-between-batches`: Sleep between batches (milliseconds, default: `0`)
- `--dry-run`: Run without deleting any keys (still reports `would delete` count)
- `--checkpoint-file`: Persist processed count after each batch (non-`--dry-run`); rerun with the same file to resume after interruption
- `--delete-txn`: Use meta transaction API to batch delete ownership keys (fewer RPCs)
- `--delete-txn-max-ops`: Max delete operations per transaction (only with `--delete-txn`, default: `1000`)
- `--recheck`: Before deleting, check `__fd_table_by_id/<table_id>` and `__fd_table_id_to_name/<table_id>`; skip if either exists

## Performance

With `--batch-size 100`, the tool processes 100 deletions concurrently per batch. For 1.3 million keys:
- ~13000 batches
- Deletion speed depends on meta server performance
- Estimated time: varies based on network latency and meta load

## Example Output

```
Configuration:
  Meta address: 172.26.39.252:9191
  Tenant: kffwpt
  Orphan IDs file: /tmp/metactl-cleanup/orphan_table_ids.txt
  Batch size: 1000
  Dry run: false

Reading orphan table IDs from: /tmp/metactl-cleanup/orphan_table_ids.txt
Total orphan table IDs to delete: 1300000

Connecting to meta: 172.26.39.252:9191
Starting batch deletion (batch size: 1000)

Processing batch 1/1300 (items 1-1000)...
  Batch complete. Total progress: 1000/1300000 deleted, 0 failed

Processing batch 2/1300 (items 1001-2000)...
  Batch complete. Total progress: 2000/1300000 deleted, 0 failed
...

========================================
Cleanup complete!
  Successfully deleted: 1300000
  Failed: 0
  Total processed: 1300000
========================================
```

## Troubleshooting

**Connection errors**: Check meta server address and connectivity

**Permission errors**: Ensure the meta client has permission to delete keys

**High failure rate**: Check meta server logs, may need to reduce `--batch-size`
