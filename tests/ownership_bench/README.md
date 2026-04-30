# Ownership Visibility Benchmark

Benchmark for comparing ownership visibility check performance between `main` and `ownership-optimize` branches.

## Data Spec

- 25 big databases × 1,000 tables = 25,000 tables
- 25 small databases × 200 tables = 5,000 tables
- Total: 50 databases, 30,000 tables

## Roles & Ownership

| Role | Ownership Count | Description |
|------|----------------|-------------|
| role_heavy | ~10,000 | 10 big dbs |
| role_medium | ~1,400 | 1 big db + 2 small dbs |
| role_light | ~400 | 2 small dbs |
| role_grant | 0 | SELECT grant on 5 big + 5 small dbs |

## Users

| User | Roles | Scenario |
|------|-------|----------|
| u_admin | account_admin | Baseline, skips ownership |
| u_grant | role_grant + role_empty1 + role_empty2 | Grant-first full hit |
| u_owner | role_heavy + role_medium + role_light | Pure ownership (~11,800) |
| u_mixed | role_grant + role_medium + role_light | Grant + ownership (~1,800) |

## Usage

```bash
# 1. Start databend (main or ownership-optimize branch)
make run-debug

# 2. Setup test data (once per clean environment)
python3 tests/ownership_bench/setup_bench_data.py

# 3. Run benchmark
python3 tests/ownership_bench/run_bench.py

# 4. Switch branch, rebuild, restart, run benchmark again
# 5. Compare results

# Cleanup
python3 tests/ownership_bench/cleanup_bench_data.py
```

## Tested SQL Paths

| Query | Path | Expected Improvement |
|-------|------|---------------------|
| `SELECT count(*) FROM system.tables WHERE database LIKE 'bench_%'` | slow path (wide) | 2x→1x list_ownerships |
| `SELECT name,database,engine FROM system.tables WHERE database LIKE 'bench_%'` | slow path (wide) | 2x→1x list_ownerships |
| `SELECT * FROM system.tables WHERE database='bench_big_00'` | optimized path | 0x list_ownerships (mget only) |
| `SELECT * FROM system.columns WHERE database='bench_big_00' AND table='t_0001'` | optimized path | 0x list_ownerships (mget only) |
| `SHOW TABLES FROM bench_big_00` | privilege_access + system.tables | point query instead of list |
| `USE bench_big_00` | privilege_access | point query instead of list |
