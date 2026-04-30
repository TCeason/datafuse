#!/usr/bin/env python3
"""
Focused regression benchmark for use_db and show_tables.

Runs only the queries that showed regression in commit3 vs optimize,
with more rounds and runs for higher confidence.
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
WARMUP_RUNS = 3
BENCH_RUNS = 10
ROUNDS = 5

USERS = [
    ("u_owner", "ownership-only (~11.8k ownerships)"),
    ("u_mixed", "grant + ownership (~1.8k ownerships)"),
]

USER_TARGET_DB = {
    "u_owner": "bench_big_15",
    "u_mixed": "bench_big_00",
}

def get_queries(user):
    db = USER_TARGET_DB[user]
    return [
        ("show_tables", f"SHOW TABLES FROM {db}"),
        ("use_db",      f"USE {db}"),
    ]


def get_cursor(user):
    dsn = f"databend://{user}:123@{HOST}:{PORT}/?sslmode=disable"
    client = BlockingDatabendClient(dsn)
    return client.cursor()


def bench_query(user, sql, warmup=WARMUP_RUNS, runs=BENCH_RUNS):
    cur = get_cursor(user)
    try:
        cur.execute("SET secondary_roles = ALL")
    except Exception:
        pass

    for _ in range(warmup):
        try:
            cur.execute(sql)
            cur.fetchall()
        except Exception:
            pass

    times = []
    for _ in range(runs):
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
        elapsed = (time.perf_counter() - start) * 1000
        times.append(elapsed)

    valid = [t for t in times if t >= 0]
    if not valid:
        return {"median": -1, "min": -1, "max": -1}
    return {"median": statistics.median(valid), "min": min(valid), "max": max(valid)}


def main():
    print("=" * 70)
    print("Regression Benchmark: show_tables + use_db")
    print(f"  {len(USERS)} users × 2 queries × {ROUNDS} rounds")
    print(f"  {WARMUP_RUNS} warmup + {BENCH_RUNS} runs per benchmark")
    print("=" * 70)

    all_medians = {}
    total_start = time.time()

    for round_idx in range(ROUNDS):
        print(f"\n--- Round {round_idx + 1}/{ROUNDS} ---")
        for user, user_desc in USERS:
            queries = get_queries(user)
            for query_name, sql in queries:
                stats = bench_query(user, sql)
                key = f"{user}/{query_name}"
                if key not in all_medians:
                    all_medians[key] = []
                if stats["median"] >= 0:
                    all_medians[key].append(stats["median"])
                print(
                    f"  {user:10s}  {query_name:15s}  "
                    f"median={stats['median']:8.1f}ms  "
                    f"min={stats['min']:8.1f}ms  "
                    f"max={stats['max']:8.1f}ms"
                )

    elapsed = time.time() - total_start
    print(f"\nDone in {elapsed:.1f}s")

    print(f"\n{'=' * 70}")
    print(f"Summary — median of {ROUNDS} rounds (ms)")
    print(f"{'=' * 70}")
    print(f"{'Query':15s}  {'User':10s}  {'median':>10s}  {'all rounds':s}")
    print("-" * 70)
    for user, _ in USERS:
        for query_name, _ in get_queries(user):
            key = f"{user}/{query_name}"
            medians = all_medians.get(key, [])
            if medians:
                vals = ", ".join(f"{v:.1f}" for v in medians)
                print(
                    f"{query_name:15s}  {user:10s}  "
                    f"{statistics.median(medians):>10.1f}  "
                    f"[{vals}]"
                )
            else:
                print(f"{query_name:15s}  {user:10s}  {'ERROR':>10s}")
    print("-" * 70)


if __name__ == "__main__":
    main()
