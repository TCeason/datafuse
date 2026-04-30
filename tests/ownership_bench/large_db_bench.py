#!/usr/bin/env python3
"""
Large-scale ownership benchmark: 10,000 tables.

Scenario:
- Create a user u_few_owner who only owns 5 tables across 10 big dbs
  (bench_big_00..09, each 1000 tables = 10,000 tables total)
- This user has NO grants, NO db ownership, only 5 table ownerships
- Query all 10 dbs: optimized path must mget_ownerships for all 10,000 tables

Compare with:
- u_grant: has SELECT grant on bench_big_00..04 (5000 tables grant-visible)
- u_owner: owns bench_big_10..19 via role_heavy (10,000 tables via db ownership)
- u_admin: baseline

Run after setup_bench_data.py on both main and optimize branches.
"""

import time
import sys
import statistics

try:
    from databend_driver import BlockingDatabendClient
except ImportError:
    print("ERROR: pip install databend-driver")
    sys.exit(1)

HOST = "localhost"
PORT = 8001
WARMUP_RUNS = 1
BENCH_RUNS = 5


def get_cursor(user="root", password=""):
    dsn = f"databend://{user}:{password}@{HOST}:{PORT}/?sslmode=disable"
    client = BlockingDatabendClient(dsn)
    return client.cursor()


def bench_query(user, sql, password="123"):
    times = []
    for _ in range(WARMUP_RUNS):
        try:
            cur = get_cursor(user, password)
            cur.execute("SET secondary_roles = ALL")
            cur.execute(sql)
            cur.fetchall()
        except Exception:
            pass

    for _ in range(BENCH_RUNS):
        cur = get_cursor(user, password)
        try:
            cur.execute("SET secondary_roles = ALL")
        except Exception:
            pass
        start = time.perf_counter()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            row_count = len(rows)
        except Exception as e:
            print(f"    ERROR: {e}")
            times.append((-1, 0))
            continue
        elapsed = (time.perf_counter() - start) * 1000
        times.append((elapsed, row_count))

    valid = [(t, r) for t, r in times if t >= 0]
    if not valid:
        return None
    elapsed_list = [t for t, _ in valid]
    row_count = valid[0][1]
    return {
        "median": statistics.median(elapsed_list),
        "min": min(elapsed_list),
        "max": max(elapsed_list),
        "rows": row_count,
    }


def main():
    print("=" * 80)
    print("10,000-table ownership benchmark")
    print(f"  {WARMUP_RUNS} warmup + {BENCH_RUNS} runs per query")
    print("=" * 80)

    root = get_cursor()

    # Setup: create role with only 5 table ownerships spread across 10 big dbs
    print("\nSetup...")
    root.execute("CREATE ROLE IF NOT EXISTS 'role_few_tables'")
    root.execute("CREATE USER IF NOT EXISTS 'u_few_owner' IDENTIFIED BY '123'")
    root.execute("GRANT ROLE 'role_few_tables' TO 'u_few_owner'")
    # 1 table in each of the first 5 dbs
    for i in range(5):
        root.execute(f"GRANT OWNERSHIP ON bench_big_{i:02d}.t_0500 TO ROLE 'role_few_tables'")
    print("  Created u_few_owner with 5 table ownerships across bench_big_00..04")
    print("  Query target: bench_big_00..09 = 10,000 tables")

    # Single db query (1,000 tables)
    single_db_sql = "SELECT * FROM system.tables WHERE database = 'bench_big_00'"
    single_db_no_owner = "SELECT name, database, engine FROM system.tables WHERE database = 'bench_big_00'"

    # 3-db query (3,000 tables) - within optimized path threshold (<=5 dbs)
    three_db_sql = "SELECT * FROM system.tables WHERE database IN ('bench_big_00','bench_big_01','bench_big_02')"
    three_db_no_owner = "SELECT name, database, engine FROM system.tables WHERE database IN ('bench_big_00','bench_big_01','bench_big_02')"

    # 5-db query (5,000 tables) - at optimized path threshold
    five_db_sql = "SELECT * FROM system.tables WHERE database IN ('bench_big_00','bench_big_01','bench_big_02','bench_big_03','bench_big_04')"

    scenarios = [
        # u_few_owner: worst case for optimized path
        ("few_1db",        "u_few_owner", single_db_sql,      "1 db (1k tables), 5 table ownerships, with owner col"),
        ("few_1db_no_own", "u_few_owner", single_db_no_owner, "1 db (1k tables), 5 table ownerships, no owner col"),
        ("few_3db",        "u_few_owner", three_db_sql,       "3 dbs (3k tables), 5 table ownerships, with owner col"),
        ("few_3db_no_own", "u_few_owner", three_db_no_owner,  "3 dbs (3k tables), 5 table ownerships, no owner col"),
        ("few_5db",        "u_few_owner", five_db_sql,        "5 dbs (5k tables), 5 table ownerships, with owner col"),

        # u_grant: grant-first hits all
        ("grant_1db",      "u_grant",     single_db_sql,      "1 db (1k tables), SELECT grant, with owner col"),
        ("grant_3db",      "u_grant",     three_db_sql,       "3 dbs (3k tables), SELECT grant, with owner col"),
        ("grant_5db",      "u_grant",     five_db_sql,        "5 dbs (5k tables), SELECT grant, with owner col"),

        # u_admin: baseline
        ("admin_1db",      "u_admin",     single_db_sql,      "1 db (1k tables), admin, with owner col"),
        ("admin_5db",      "u_admin",     five_db_sql,        "5 dbs (5k tables), admin, with owner col"),
    ]

    results = {}
    total = len(scenarios)
    for idx, (name, user, sql, desc) in enumerate(scenarios):
        print(f"\n[{idx+1}/{total}] {desc}")
        print(f"  User: {user}  SQL: {sql[:80]}...")

        stats = bench_query(user, sql)
        results[name] = stats

        if stats is None:
            print(f"  ERROR")
        else:
            print(
                f"  median={stats['median']:7.1f}ms  "
                f"min={stats['min']:7.1f}ms  "
                f"max={stats['max']:7.1f}ms  "
                f"rows={stats['rows']}"
            )

    # Summary
    print(f"\n{'=' * 80}")
    print("Summary (median ms)")
    print(f"{'=' * 80}")
    print(f"{'Scenario':20s}  {'median':>8s}  {'min':>8s}  {'max':>8s}  {'rows':>6s}  description")
    print("-" * 80)
    for name, user, sql, desc in scenarios:
        st = results.get(name)
        if st:
            print(
                f"{name:20s}  {st['median']:>7.1f}  {st['min']:>7.1f}  "
                f"{st['max']:>7.1f}  {st['rows']:>6d}  {desc}"
            )
        else:
            print(f"{name:20s}  {'ERROR':>8s}  {desc}")

    # Cleanup
    print(f"\nCleanup...")
    for i in range(5):
        try:
            root.execute(f"GRANT OWNERSHIP ON bench_big_{i:02d}.t_0500 TO ROLE 'account_admin'")
        except Exception:
            pass
    root.execute("DROP USER IF EXISTS 'u_few_owner'")
    root.execute("DROP ROLE IF EXISTS 'role_few_tables'")
    print("  Done.")


if __name__ == "__main__":
    main()
