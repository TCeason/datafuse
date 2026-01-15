# 验证指南（mock + 真实数据抽样）

本文档用于验证 `metactl-cleanup` 的 `--dry-run`、`--recheck`、`--sleep-ms-between-batches` 行为是否符合预期。

## 前置条件

- 本地或目标环境已经有可用的 meta：你目前是 `127.0.0.1:9193`
  - 例：`./target/debug/databend-meta --single --log-level=info --admin-api-address 127.0.0.1:28003 --grpc-api-address 127.0.0.1:9193`
- 需要 `databend-metactl`：
  - 例：`/data1/eason/databend/target/debug/databend-metactl`
- `metactl-cleanup` 已在 workspace：
  - `cd /data1/eason/databend && cargo build -p metactl-cleanup`

建议先确认 meta 状态：

```bash
/data1/eason/databend/target/debug/databend-metactl status --grpc-api-address 127.0.0.1:9193
```

## A. 用 mock 数据在本地 meta 上验证（推荐先跑）

目标：
- 6 个 table_id（其中 3 个“仍存在”，3 个“孤儿”）
- `--dry-run --recheck` 应该：`would delete = 3`、`skipped = 3`
- 真删（不带 `--dry-run`）应该只删除 3 个孤儿 ownership key

### A1. 写入 mock 数据

```bash
MC=/data1/eason/databend/target/debug/databend-metactl
META=127.0.0.1:9193
TENANT=kffwpt

# 6 个 ownership key（都写）
for id in 990000001 990000002 990000003 990000004 990000005 990000006; do
  $MC upsert --grpc-api-address $META \
    --key "__fd_object_owners/${TENANT}/table-by-id/${id}" \
    --value "owner-${id}"
done

# 3 个“仍存在的表”：写 table meta，让 --recheck 能检测到并 skip
for id in 990000001 990000002 990000003; do
  $MC upsert --grpc-api-address $META --key "__fd_table_by_id/${id}" --value "table-by-id-${id}"
  $MC upsert --grpc-api-address $META --key "__fd_table_id_to_name/${id}" --value "table-id-to-name-${id}"
done

cat > /tmp/metactl_cleanup_mock_ids.txt <<'EOF'
990000001
990000002
990000003
990000004
990000005
990000006
EOF
wc -l /tmp/metactl_cleanup_mock_ids.txt
```

### A2. Dry-run 验证（不删除）

```bash
cd /data1/eason/databend
cargo run -p metactl-cleanup -- \
  --tenant kffwpt \
  --meta-addr 127.0.0.1:9193 \
  --orphan-ids-file /tmp/metactl_cleanup_mock_ids.txt \
  --batch-size 2 \
  --sleep-ms-between-batches 10 \
  --dry-run \
  --recheck
```

预期输出包含：
- `Would delete: 3`
- `Skipped (table still exists): 3`
- `Failed: 0`

### A3. 真删验证（只删孤儿 3 个）

```bash
cd /data1/eason/databend
cargo run -p metactl-cleanup -- \
  --tenant kffwpt \
  --meta-addr 127.0.0.1:9193 \
  --orphan-ids-file /tmp/metactl_cleanup_mock_ids.txt \
  --batch-size 2 \
  --recheck
```

预期输出包含：
- `Successfully deleted: 3`
- `Skipped (table still exists): 3`
- `Failed: 0`

### A4. 用 metactl get 做点对点校验

```bash
MC=/data1/eason/databend/target/debug/databend-metactl
META=127.0.0.1:9193

# 仍存在的表：ownership key 应该还在（非 null）
$MC get --grpc-api-address $META --key "__fd_object_owners/kffwpt/table-by-id/990000001"

# 孤儿表：ownership key 应该被删（null）
$MC get --grpc-api-address $META --key "__fd_object_owners/kffwpt/table-by-id/990000004"
```

## B. 用真实数据抽样做 dry-run 验证（不删除）

目标：
- 从 `~/big_data` 的离线结果里做一个 30k 的抽样列表
- 这 30k 必须包含全部真实存在的 4k+ table_id（用于验证 `--recheck` 的 skip 逻辑）
- 用 `--dry-run --recheck` 跑一遍，确认不会误判“仍存在的表”

### B1. 生成 30k 抽样文件

你当前目录里已有：
- `~/big_data/existing_table_ids.txt`（约 4699 行）
- `~/big_data/orphan_table_ids.txt`（约 1,596,242 行）

生成一个 30k 的验证文件（包含全部 existing + 25301 个 orphan 抽样）：

```bash
WORK=~/big_data
EX=$WORK/existing_table_ids.txt
OR=$WORK/orphan_table_ids.txt
OUT=$WORK/sample_30k_table_ids.txt
TMP=$(mktemp)

cat "$EX" > "$TMP"
awk 'NR%63==0{print}' "$OR" | head -n 25301 >> "$TMP"
LC_ALL=C sort -u "$TMP" > "$OUT"
rm -f "$TMP"

wc -l "$OUT"
# 确认 existing 全包含（输出应该是 0）
LC_ALL=C comm -23 <(LC_ALL=C sort -u "$EX") <(LC_ALL=C sort -u "$OUT") | wc -l
```

### B2. 对目标 meta 做 dry-run（强烈建议先用小 batch + sleep）

如果你要对线上/远端 meta 做验证（只读，不删），建议从较小并发开始：

```bash
cd /data1/eason/databend
cargo run -p metactl-cleanup -- \
  --tenant kffwpt \
  --meta-addr <META_GRPC_ADDR> \
  --orphan-ids-file ~/big_data/sample_30k_table_ids.txt \
  --batch-size 200 \
  --sleep-ms-between-batches 50 \
  --dry-run \
  --recheck
```

期望现象：
- `Skipped (table still exists)` 应该接近“样本里真实存在的表数量”（至少应覆盖那 4k+）
- `Would delete` 是样本里 orphan 的数量

### B3. 手工抽查几个 id 的 recheck 逻辑

从两个文件里各取几个 id：
- existing：`head -n 5 ~/big_data/existing_table_ids.txt`
- orphan：`head -n 5 ~/big_data/orphan_table_ids.txt`

对其中某个 `id`，用 metactl get 校验：

```bash
MC=/data1/eason/databend/target/debug/databend-metactl
META=<META_GRPC_ADDR>
ID=<TABLE_ID>

# 如果是 existing：至少应有一个不为 null（表示表 meta 仍在）
$MC get --grpc-api-address $META --key "__fd_table_by_id/${ID}"
$MC get --grpc-api-address $META --key "__fd_table_id_to_name/${ID}"

# 如果是 orphan：ownership key 大概率存在，但 table meta 应为 null（才“可删”）
$MC get --grpc-api-address $META --key "__fd_object_owners/kffwpt/table-by-id/${ID}"
$MC get --grpc-api-address $META --key "__fd_table_by_id/${ID}"
$MC get --grpc-api-address $META --key "__fd_table_id_to_name/${ID}"
```

注意：`metactl-cleanup --dry-run` 不会删任何 key，但会对每个 id 做 recheck 读请求；对远端环境请务必控制 `--batch-size` 并加 `--sleep-ms-between-batches`。

