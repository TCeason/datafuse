#!/usr/bin/env python3
"""
Benchmark ownership visibility optimization.

Run this on both `main` and `ownership-optimize` branches to compare.
Requires setup_bench_data.py to have been run first.

Tests:
1. Wide query:     SELECT count(*) FROM system.tables
2. Wide query:     SELECT * FROM system.tables (all columns)
3. Filtered query: SELECT * FROM system.tables WHERE database='bench_big_00'
4. Filtered query: SELECT * FROM system.columns WHERE database='bench_big_00' AND table='t_0001'
5. SHOW TABLES:    SHOW TABLES IN bench_big_00
6. USE DATABASE:   USE bench_big_00
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
ROUNDS = 5  # Number of full rounds; each round runs all benchmarks

USERS = [
    ("u_admin", "admin (baseline)"),
    ("u_grant", "grant-only"),
    ("u_owner", "ownership-only (~11.8k ownerships)"),
    ("u_mixed", "grant + ownership (~1.8k ownerships)"),
]

# Per-user target database for filtered queries.
# Each user must have access (grant or ownership) to their target db.
# u_admin:  admin, can access anything
# u_grant:  has SELECT grant on bench_big_00..04, bench_small_00..04
# u_owner:  owns bench_big_10..19, bench_small_10..13
# u_mixed:  grant on bench_big_00..04 + owns bench_big_20, bench_small_10..13
USER_TARGET_DB = {
    "u_admin": "bench_big_00",
    "u_grant": "bench_big_00",
    "u_owner": "bench_big_15",
    "u_mixed": "bench_big_00",
}

def get_queries(user):
    db = USER_TARGET_DB[user]
    return [
        ("wide_count",      "SELECT count(*) FROM system.tables WHERE database LIKE 'bench_%'"),
        ("wide_select",     "SELECT name, database, engine FROM system.tables WHERE database LIKE 'bench_%'"),
        ("filtered_tables", f"SELECT * FROM system.tables WHERE database = '{db}'"),
        ("filtered_columns",f"SELECT * FROM system.columns WHERE database = '{db}' AND table = 't_0001'"),
        ("show_tables",     f"SHOW TABLES FROM {db}"),
        ("use_db",          f"USE {db}"),
    ]


def get_cursor(user):
    dsn = f"databend://{user}:123@{HOST}:{PORT}/?sslmode=disable"
    client = BlockingDatabendClient(dsn)
    return client.cursor()


def progress_bar(current, total, width=30):
    pct = current / total if total > 0 else 0
    filled = int(width * pct)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {current}/{total}"


def bench_query(user, sql, warmup=WARMUP_RUNS, runs=BENCH_RUNS):
    """Run a query multiple times and return timing stats."""
    cur = get_cursor(user)

    # Set secondary roles
    try:
        cur.execute("SET secondary_roles = ALL")
    except Exception:
        pass

    # Warmup
    for _ in range(warmup):
        try:
            cur.execute(sql)
            cur.fetchall()
        except Exception:
            pass

    # Bench
    times = []
    for r in range(runs):
        cur = get_cursor(user)
        try:
            cur.execute("SET secondary_roles = ALL")
        except Exception:
            pass
        start = time.perf_counter()
        try:
            cur.execute(sql)
            cur.fetchall()
        except Exception:
            times.append(-1)
            continue
        elapsed = (time.perf_counter() - start) * 1000  # ms
        times.append(elapsed)

    valid = [t for t in times if t >= 0]
    if not valid:
        return {"median": -1, "min": -1, "max": -1, "runs": runs, "errors": runs}

    return {
        "median": statistics.median(valid),
        "min": min(valid),
        "max": max(valid),
        "runs": len(valid),
        "errors": runs - len(valid),
    }


def main():
    total_queries = len(USERS) * 6  # 6 queries per user
    total_benchmarks = total_queries * ROUNDS

    print("=" * 90)
    print("Ownership Visibility Benchmark")
    print(f"  {len(USERS)} users × 6 queries = {total_queries} benchmarks per round")
    print(f"  {ROUNDS} rounds, {WARMUP_RUNS} warmup + {BENCH_RUNS} runs per benchmark")
    print("=" * 90)

    # Verify connectivity and data
    try:
        cur = get_cursor("u_admin")
        cur.execute("SELECT count(*) FROM system.tables WHERE database LIKE 'bench_%'")
        table_count = cur.fetchone()[0]
        cur.execute("SELECT count(DISTINCT database) FROM system.tables WHERE database LIKE 'bench_%'")
        db_count = cur.fetchone()[0]
        print(f"  Data: {db_count} databases, {table_count} tables")
    except Exception as e:
        print(f"ERROR: Cannot connect: {e}")
        sys.exit(1)

    if table_count == 0:
        print("ERROR: No bench data found. Run setup_bench_data.py first.")
        sys.exit(1)

    # Collect all median values across rounds: key -> list of medians
    all_medians = {}
    total_start = time.time()
    done = 0

    for round_idx in range(ROUNDS):
        print(f"\n{'━' * 90}")
        print(f"Round {round_idx + 1}/{ROUNDS}")
        print(f"{'━' * 90}")

        for user_idx, (user, user_desc) in enumerate(USERS):
            print(f"\n{'─' * 90}")
            print(f"User {user_idx + 1}/{len(USERS)}: {user} ({user_desc})  target_db={USER_TARGET_DB[user]}")
            print(f"{'─' * 90}")

            queries = get_queries(user)
            for q_idx, (query_name, sql) in enumerate(queries):
                done += 1
                elapsed_total = time.time() - total_start
                rate = done / elapsed_total if elapsed_total > 0 else 0
                remaining = (total_benchmarks - done) / rate if rate > 0 else 0

                sys.stdout.write(
                    f"\r  {progress_bar(done, total_benchmarks)}  "
                    f"ETA {remaining:.0f}s  running: {query_name}...          "
                )
                sys.stdout.flush()

                stats = bench_query(user, sql)
                key = f"{user}/{query_name}"
                if key not in all_medians:
                    all_medians[key] = []
                if stats["median"] >= 0:
                    all_medians[key].append(stats["median"])

                if stats["median"] < 0:
                    result_str = "ERROR"
                else:
                    result_str = (
                        f"median={stats['median']:8.1f}ms  "
                        f"min={stats['min']:8.1f}ms  "
                        f"max={stats['max']:8.1f}ms"
                    )

                # Clear progress line and print result
                sys.stdout.write(f"\r  {query_name:25s}  {result_str}\n")
                sys.stdout.flush()

    elapsed = time.time() - total_start
    print(f"\nAll benchmarks done in {elapsed:.1f}s ({elapsed/60:.1f}m)")

    # Summary table: median of medians across rounds
    print(f"\n{'=' * 90}")
    print(f"Summary — median of {ROUNDS} rounds (ms)")
    print(f"{'=' * 90}")
    header = f"{'Query':25s}"
    for user, _ in USERS:
        header += f"  {user:>15s}"
    print(header)
    print("-" * 90)

    query_names = [name for name, _ in get_queries("u_admin")]

    for query_name in query_names:
        row = f"{query_name:25s}"
        for user, _ in USERS:
            key = f"{user}/{query_name}"
            medians = all_medians.get(key, [])
            if medians:
                row += f"  {statistics.median(medians):>14.1f}"
            else:
                row += f"  {'ERROR':>14s}"
        print(row)

    print("-" * 90)
    print(f"{'':25s}  {'admin':>15s}  {'grant-only':>15s}  {'owner-only':>15s}  {'mixed':>15s}")


if __name__ == "__main__":
    main()
