#!/bin/bash
# Script to identify orphan ownership keys that are safe to delete

set -euo pipefail

# Configuration
TENANT="${TENANT:-kffwpt}"
META_ADDR="${META_ADDR:-172.26.39.252:9191}"
QUERY_ADDR="${QUERY_ADDR:-172.26.39.252:8000}"
USER="${DB_USER:-databend}"
PASS="${DB_PASS:-}"

WORK_DIR="${WORK_DIR:-/tmp/metactl-cleanup}"
mkdir -p "$WORK_DIR"

OWNERSHIP_KEYS_FILE="${OWNERSHIP_KEYS_FILE:-}"
# OWNERSHIP_KEYS_FILE_KIND:
# - auto: detect format from file content
# - keys: file contains raw keys (or lines containing keys); just extract all ids
# - watch: file contains output from `databend-metactl watch` (appended); try to keep "current" keys only
OWNERSHIP_KEYS_FILE_KIND="${OWNERSHIP_KEYS_FILE_KIND:-auto}"

echo "========================================"
echo "Orphan Ownership Key Identification"
echo "========================================"
echo "Configuration:"
echo "  Tenant: $TENANT"
echo "  Meta address: $META_ADDR"
echo "  Query address: $QUERY_ADDR"
echo "  Work directory: $WORK_DIR"
if [ -n "$OWNERSHIP_KEYS_FILE" ]; then
	echo "  Ownership keys file: $OWNERSHIP_KEYS_FILE"
	echo "  Ownership keys file kind: $OWNERSHIP_KEYS_FILE_KIND"
fi
echo ""

# Step 1: Info
echo "[1/3] Fetching data from databend..."
echo "  This operation uses snapshot consistency:"
echo "  - Snapshot 1: Current table IDs from system.tables"
echo "  - Snapshot 2: Ownership keys from meta"
echo "  - Orphans = keys in Snapshot 2 but not in Snapshot 1"
echo ""

# Step 2: Get existing table IDs
echo "[2/3] Fetching existing table IDs from system.tables..."
bendsql -h "${QUERY_ADDR%:*}" -P "${QUERY_ADDR#*:}" -u "$USER" -p "$PASS" \
	--query="SELECT table_id FROM system.tables" -o tsv |
	LC_ALL=C sort -n -u >"$WORK_DIR/existing_table_ids.txt"

EXISTING_COUNT=$(wc -l <"$WORK_DIR/existing_table_ids.txt")
echo "  Found $EXISTING_COUNT existing tables"
echo ""

# Step 3: Export ownership keys
echo "[3/3] Extracting ownership keys from meta..."
echo "  This may take a while for large datasets..."

extract_ids_from_keys_file() {
	# This path is used when you already have ownership keys collected to a file.
	# It supports either:
	# - raw keys (or lines containing keys), e.g. "__fd_object_owners/<tenant>/table-by-id/<id>"
	# - `databend-metactl watch` output lines appended over time
	#
	# For "watch" mode we try to keep only keys whose latest event still has current != None.
	local file="$1"
	local kind="$2"

	if [ "$kind" = "auto" ]; then
		if head -n 50 "$file" | grep -qE 'received-watch-event: (INIT|CHANGE):\(' || head -n 50 "$file" | grep -qE '^(INIT|CHANGE):\('; then
			kind="watch"
		else
			kind="keys"
		fi
	fi

	if [ "$kind" = "watch" ]; then
		echo "  Using watch log file (keep latest state per key)..."
		local states="$WORK_DIR/ownership_key_states.tsv"
		local latest="$WORK_DIR/ownership_key_latest.tsv"

		# Extract "<key>\t<line_no>\t<exists>".
		# In watch output, deletion is represented as "-> None)" (i.e. current=None).
		awk -v tenant="$TENANT" -v OFS="\t" '
      {
        if (match($0, "__fd_object_owners/" tenant "/table-by-id/[0-9]+", m)) {
          key = m[0];
          exists = ($0 ~ /-> None\\)/) ? 0 : 1;
          print key, NR, exists;
        }
      }
    ' "$file" >"$states"

		# Sort by key then line number, then keep the latest record per key.
		LC_ALL=C sort -t $'\t' -k1,1 -k2,2n "$states" |
			awk -F $'\t' -v OFS="\t" '
          BEGIN { prev=""; last_exists=0 }
          {
            if (prev != "" && $1 != prev) {
              print prev, last_exists
            }
            prev = $1
            last_exists = $3
          }
          END {
            if (prev != "") {
              print prev, last_exists
            }
          }
        ' >"$latest"

		# Keep only keys whose latest exists=1 and extract table_id.
		awk -F $'\t' '$2 == 1 { sub(/^.*table-by-id\\//, "", $1); print $1 }' "$latest" |
			LC_ALL=C sort -n -u >"$WORK_DIR/ownership_table_ids.txt"
	else
		echo "  Using keys file (extract all ids)..."
		grep -aoE "__fd_object_owners/${TENANT}/table-by-id/[0-9]+" "$file" |
			sed -n 's|.*/table-by-id/||p' |
			LC_ALL=C sort -n -u >"$WORK_DIR/ownership_table_ids.txt"
	fi
}

if [ -n "$OWNERSHIP_KEYS_FILE" ]; then
	extract_ids_from_keys_file "$OWNERSHIP_KEYS_FILE" "$OWNERSHIP_KEYS_FILE_KIND"
else
	databend-metactl export --grpc-api-address "$META_ADDR" 2>/dev/null |
		grep "\"__fd_object_owners/${TENANT}/table-by-id/" |
		sed -n 's/.*table-by-id\/\([0-9]*\).*/\1/p' |
		LC_ALL=C sort -n -u >"$WORK_DIR/ownership_table_ids.txt"
fi

OWNERSHIP_COUNT=$(wc -l <"$WORK_DIR/ownership_table_ids.txt")
echo "  Found $OWNERSHIP_COUNT ownership keys"
echo ""

# Step 4: Identify orphans
echo ""
echo "Identifying orphan keys..."
comm -23 "$WORK_DIR/ownership_table_ids.txt" "$WORK_DIR/existing_table_ids.txt" \
	>"$WORK_DIR/orphan_table_ids.txt"

ORPHAN_COUNT=$(wc -l <"$WORK_DIR/orphan_table_ids.txt")

echo ""
echo "========================================"
echo "Summary:"
echo "  Existing tables: $EXISTING_COUNT"
echo "  Ownership keys: $OWNERSHIP_COUNT"
echo "  Orphan keys: $ORPHAN_COUNT"
echo ""
echo "Files generated:"
echo "  - Existing table IDs: $WORK_DIR/existing_table_ids.txt"
echo "  - Ownership keys: $WORK_DIR/ownership_table_ids.txt"
echo "  - Orphan keys: $WORK_DIR/orphan_table_ids.txt"
echo ""
echo "Preview of orphan keys (first 10):"
head -10 "$WORK_DIR/orphan_table_ids.txt"
if [ "$ORPHAN_COUNT" -gt 10 ]; then
	echo "  ... and $((ORPHAN_COUNT - 10)) more"
fi
echo ""
echo "Next steps:"
echo "  1. Review: less $WORK_DIR/orphan_table_ids.txt"
echo "  2. Dry run: cargo run -p metactl-cleanup -- --tenant $TENANT --meta-addr $META_ADDR --orphan-ids-file $WORK_DIR/orphan_table_ids.txt --dry-run"
echo "  3. Delete: cargo run -p metactl-cleanup -- --tenant $TENANT --meta-addr $META_ADDR --orphan-ids-file $WORK_DIR/orphan_table_ids.txt --batch-size 1000"
echo "========================================"
