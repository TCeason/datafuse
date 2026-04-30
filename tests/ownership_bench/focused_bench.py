#!/usr/bin/env python3
"""
Focused benchmark: compare u_grant vs u_owner on the SAME database.

Setup: grant u_grant SELECT on bench_big_15 (which u_owner already owns).
Then both users query bench_big_15, eliminating db-level differences.

Run after setup_bench_data.py.
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
WARMUP_RUNS = 2
BENCH_RUNS = 10
TARGET_DB = "bench_big_15"


def get_cursor(user, password="123"):
    dsn = f"databend://{user}:{password}@{HOST}:{PORT}/?sslmode=disable"
    client = BlockingDatabendClient(dsn)
    return client.cursor()


def bench_query(user, sql):
    times = []
    for _ in range(WARMUP_RUNS):
        try:
            cur = get_cursor(user)
            cur.execute("SET secondary_roles = ALL")
            cur.execute(sql)
            cur.fetchall()
        except Exception:
            pass

    for _ in range(BENCH_RUNS):
        cur = get_cursor(user)
        try:
            cur.execute("SET secondary_roles = ALL")
        except Exception:
            pass
        start = time.perf_counter()
        try:
            cur.execute(sql)
            cur.fetchall()
        except Exception as e:
            print(f"    ERROR: {e}")
            times.append(-1)
            continue
        elapsed = (time.perf_counter() - start) * 1000
        times.append(elapsed)

    valid = [t for t in times if t >= 0]
    if not valid:
        return None
    return {
        "median": statistics.median(valid),
        "min": min(valid),
        "max": max(valid),
        "p25": sorted(valid)[len(valid) // 4],
        "p75": sorted(valid)[3 * len(valid) // 4],
    }


def main():
    print("=" * 80)
    print(f"Focused benchmark: u_grant vs u_owner on same db ({TARGET_DB})")
    print(f"  {WARMUP_RUNS} warmup + {BENCH_RUNS} runs per query")
    print("=" * 80)

    # Setup: grant u_grant SELECT on bench_big_15
    print("\nSetup: granting u_grant SELECT on bench_big_15...")
    root = get_cursor("root", "")
    root.execute(f"GRANT SELECT ON {TARGET_DB}.* TO ROLE 'role_grant'")
    print("  Done.\n")

    queries = [
        ("filtered_tables",  f"SELECT * FROM system.tables WHERE database = '{TARGET_DB}'"),
        ("filtered_no_owner", f"SELECT name, database, engine FROM system.tables WHERE database = '{TARGET_DB}'"),
        ("show_tables",      f"SHOW TABLES FROM {TARGET_DB}"),
        ("use_db",           f"USE {TARGET_DB}"),
    ]

    users = [
        ("u_grant", "grant-only (SELECT on db)"),
        ("u_owner", "ownership-only (owns db)"),
    ]

    results = {}

    for user, desc in users:
        print(f"{'─' * 80}")
        print(f"User: {user} ({desc})")
        print(f"{'─' * 80}")

        for qname, sql in queries:
            stats = bench_query(user, sql)
            key = f"{user}/{qname}"
            results[key] = stats

            if stats is None:
                print(f"  {qname:25s}  ERROR")
            else:
                print(
                    f"  {qname:25s}  "
                    f"median={stats['median']:7.1f}ms  "
                    f"p25={stats['p25']:7.1f}ms  "
                    f"p75={stats['p75']:7.1f}ms  "
                    f"min={stats['min']:7.1f}ms  "
                    f"max={stats['max']:7.1f}ms"
                )

    # Summary
    print(f"\n{'=' * 80}")
    print(f"Summary: median ms on {TARGET_DB} (same db, {BENCH_RUNS} runs)")
    print(f"{'=' * 80}")
    print(f"{'Query':25s}  {'u_grant':>10s}  {'u_owner':>10s}  {'diff':>10s}")
    print("-" * 65)
    for qname, _ in queries:
        g = results.get(f"u_grant/{qname}")
        o = results.get(f"u_owner/{qname}")
        gv = f"{g['median']:.1f}" if g else "ERROR"
        ov = f"{o['median']:.1f}" if o else "ERROR"
        if g and o:
            diff_pct = (g['median'] - o['median']) / o['median'] * 100
            diff = f"{diff_pct:+.0f}%"
        else:
            diff = "—"
        print(f"{qname:25s}  {gv:>10s}  {ov:>10s}  {diff:>10s}")

    # Cleanup: remove the extra grant
    print(f"\nCleanup: revoking extra grant...")
    root.execute(f"REVOKE SELECT ON {TARGET_DB}.* FROM ROLE 'role_grant'")
    print("  Done.")


if __name__ == "__main__":
    main()
