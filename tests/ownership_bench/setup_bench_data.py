#!/usr/bin/env python3
"""
Setup benchmark data for ownership visibility optimization testing.

Data spec:
- 25 "big" databases × 1000 tables = 25,000 tables
- 25 "small" databases × 200 tables = 5,000 tables
- Total: 50 databases, 30,000 tables

Roles & table-level ownership:
- role_heavy:  owns all tables in 10 big dbs = 10,000 table ownerships
- role_medium: owns all tables in 1 big db + 2 small dbs = 1,400 table ownerships
- role_light:  owns all tables in 2 small dbs = 400 table ownerships
- role_grant:  SELECT grant on 5 big dbs + 5 small dbs (no ownership)
- role_empty1, role_empty2: no grants, no ownership (padding roles)

Users (password: 123):
- u_admin:  account_admin
- u_grant:  role_grant + role_empty1 + role_empty2
- u_owner:  role_heavy + role_medium + role_light (~11,800 table ownerships)
- u_mixed:  role_grant + role_medium + role_light (grant + ~1,800 table ownerships)
"""

import time
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from databend_driver import BlockingDatabendClient
except ImportError:
    print("ERROR: pip install databend-driver")
    sys.exit(1)

DSN = "databend://root:@localhost:8001/?sslmode=disable"
NUM_BIG_DBS = 25
NUM_SMALL_DBS = 25
TABLES_PER_BIG_DB = 1000
TABLES_PER_SMALL_DB = 200
PARALLELISM = 16

# Global progress tracking
_progress_lock = threading.Lock()
_progress_count = 0
_progress_total = 0
_progress_start = 0.0
_progress_label = ""


def get_cursor():
    client = BlockingDatabendClient(DSN)
    cur = client.cursor()
    try:
        cur.execute("SYSTEM FLUSH PRIVILEGE")
    except Exception:
        pass
    return cur


def progress_bar(current, total, width=40):
    pct = current / total if total > 0 else 0
    filled = int(width * pct)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {current:>6}/{total} ({pct:5.1%})"


def print_progress(extra=""):
    global _progress_count, _progress_total, _progress_start, _progress_label
    elapsed = time.time() - _progress_start
    rate = _progress_count / elapsed if elapsed > 0 else 0
    remaining = (_progress_total - _progress_count) / rate if rate > 0 else 0
    bar = progress_bar(_progress_count, _progress_total)
    msg = f"\r  {bar}  {rate:.0f} {_progress_label}/s  ETA {remaining:.0f}s"
    if extra:
        msg += f"  {extra}"
    sys.stdout.write(msg + "    ")
    sys.stdout.flush()


def inc_progress(n=1):
    global _progress_count
    with _progress_lock:
        _progress_count += n
        if _progress_count % 100 == 0 or _progress_count == _progress_total:
            print_progress()


def reset_progress(total, label):
    global _progress_count, _progress_total, _progress_start, _progress_label
    _progress_count = 0
    _progress_total = total
    _progress_start = time.time()
    _progress_label = label


def create_tables_for_db(db_name, num_tables):
    """Create all tables in one database."""
    cur = get_cursor()
    for t in range(num_tables):
        for attempt in range(3):
            try:
                cur.execute(f"CREATE TABLE IF NOT EXISTS {db_name}.t_{t:04d} (id INT)")
                break
            except Exception:
                if attempt == 2:
                    raise
                cur = get_cursor()
        inc_progress()
    return db_name, num_tables


def grant_table_ownerships_for_db(db_name, num_tables, role):
    """Grant ownership on each table in a database to a role."""
    cur = get_cursor()
    for t in range(num_tables):
        table_name = f"t_{t:04d}"
        for attempt in range(3):
            try:
                cur.execute(f"GRANT OWNERSHIP ON {db_name}.{table_name} TO ROLE '{role}'")
                break
            except Exception:
                if attempt == 2:
                    raise
                cur = get_cursor()
        inc_progress()
    return db_name, num_tables


def main():
    total_start = time.time()
    cur = get_cursor()

    # ── Phase 1: Create databases ──
    print("=" * 70)
    print("Phase 1/5: Creating databases")
    print("=" * 70)
    t0 = time.time()
    total_dbs = NUM_BIG_DBS + NUM_SMALL_DBS
    for i in range(NUM_BIG_DBS):
        cur.execute(f"CREATE DATABASE IF NOT EXISTS bench_big_{i:02d}")
        sys.stdout.write(f"\r  {progress_bar(i + 1, total_dbs)}")
        sys.stdout.flush()
    for i in range(NUM_SMALL_DBS):
        cur.execute(f"CREATE DATABASE IF NOT EXISTS bench_small_{i:02d}")
        sys.stdout.write(f"\r  {progress_bar(NUM_BIG_DBS + i + 1, total_dbs)}")
        sys.stdout.flush()
    print(f"\n  Done: {total_dbs} databases in {time.time() - t0:.1f}s\n")

    # ── Phase 2: Create tables (parallel) ──
    total_tables = NUM_BIG_DBS * TABLES_PER_BIG_DB + NUM_SMALL_DBS * TABLES_PER_SMALL_DB
    print("=" * 70)
    print(f"Phase 2/5: Creating {total_tables} tables ({PARALLELISM} workers)")
    print("=" * 70)
    reset_progress(total_tables, "tables")
    tasks = []
    with ThreadPoolExecutor(max_workers=PARALLELISM) as pool:
        for i in range(NUM_BIG_DBS):
            tasks.append(pool.submit(create_tables_for_db, f"bench_big_{i:02d}", TABLES_PER_BIG_DB))
        for i in range(NUM_SMALL_DBS):
            tasks.append(pool.submit(create_tables_for_db, f"bench_small_{i:02d}", TABLES_PER_SMALL_DB))
        for f in as_completed(tasks):
            f.result()
    print_progress()
    print(f"\n  Done: {total_tables} tables in {time.time() - _progress_start:.1f}s\n")

    # ── Phase 3: Create roles and grants ──
    print("=" * 70)
    print("Phase 3/5: Creating roles and grants")
    print("=" * 70)
    t0 = time.time()
    roles = ["role_heavy", "role_medium", "role_light", "role_grant", "role_empty1", "role_empty2"]
    for i, role in enumerate(roles):
        cur.execute(f"CREATE ROLE IF NOT EXISTS '{role}'")
        sys.stdout.write(f"\r  Roles: {progress_bar(i + 1, len(roles))}")
        sys.stdout.flush()
    print()

    grant_sqls = []
    for i in range(5):
        grant_sqls.append(f"GRANT SELECT ON bench_big_{i:02d}.* TO ROLE 'role_grant'")
    for i in range(5):
        grant_sqls.append(f"GRANT SELECT ON bench_small_{i:02d}.* TO ROLE 'role_grant'")
    for i, sql in enumerate(grant_sqls):
        cur.execute(sql)
        sys.stdout.write(f"\r  Grants: {progress_bar(i + 1, len(grant_sqls))}")
        sys.stdout.flush()
    print(f"\n  Done: {len(roles)} roles, {len(grant_sqls)} grants in {time.time() - t0:.1f}s\n")

    # ── Phase 4: Grant table-level ownership (parallel) ──
    # Ownership plan: (db_name, num_tables, role)
    ownership_plan = []
    # role_heavy: 10 big dbs (index 10-19) = 10,000 tables
    for i in range(10, 20):
        ownership_plan.append((f"bench_big_{i:02d}", TABLES_PER_BIG_DB, "role_heavy"))
    # role_medium: 1 big db (index 20) + 2 small dbs (index 10-11) = 1,400 tables
    ownership_plan.append(("bench_big_20", TABLES_PER_BIG_DB, "role_medium"))
    for i in range(10, 12):
        ownership_plan.append((f"bench_small_{i:02d}", TABLES_PER_SMALL_DB, "role_medium"))
    # role_light: 2 small dbs (index 12-13) = 400 tables
    for i in range(12, 14):
        ownership_plan.append((f"bench_small_{i:02d}", TABLES_PER_SMALL_DB, "role_light"))

    total_ownership_tables = sum(n for _, n, _ in ownership_plan)
    print("=" * 70)
    print(f"Phase 4/5: Granting {total_ownership_tables} table ownerships ({PARALLELISM} workers)")
    print("  role_heavy:  10,000 tables (10 big dbs)")
    print("  role_medium:  1,400 tables (1 big + 2 small dbs)")
    print("  role_light:     400 tables (2 small dbs)")
    print("=" * 70)
    reset_progress(total_ownership_tables, "grants")

    with ThreadPoolExecutor(max_workers=PARALLELISM) as pool:
        futures = []
        for db_name, num_tables, role in ownership_plan:
            futures.append(pool.submit(grant_table_ownerships_for_db, db_name, num_tables, role))
        for f in as_completed(futures):
            f.result()
    print_progress()
    print(f"\n  Done: {total_ownership_tables} table ownerships in {time.time() - _progress_start:.1f}s\n")

    # ── Phase 5: Create users ──
    print("=" * 70)
    print("Phase 5/5: Creating users")
    print("=" * 70)
    t0 = time.time()

    user_setup = [
        ("u_admin",  ["account_admin"]),
        ("u_grant",  ["role_grant", "role_empty1", "role_empty2"]),
        ("u_owner",  ["role_heavy", "role_medium", "role_light"]),
        ("u_mixed",  ["role_grant", "role_medium", "role_light"]),
    ]
    for i, (user, user_roles) in enumerate(user_setup):
        cur.execute(f"CREATE USER IF NOT EXISTS '{user}' IDENTIFIED BY '123'")
        for role in user_roles:
            cur.execute(f"GRANT ROLE '{role}' TO '{user}'")
        sys.stdout.write(f"\r  {progress_bar(i + 1, len(user_setup))}  {user} <- {', '.join(user_roles)}")
        sys.stdout.flush()
    print(f"\n  Done: {len(user_setup)} users in {time.time() - t0:.1f}s\n")

    # ── Verification ──
    print("=" * 70)
    print("Verification")
    print("=" * 70)
    cur.execute("SELECT count(*) FROM system.tables WHERE database LIKE 'bench_%'")
    table_count = cur.fetchone()[0]
    cur.execute("SELECT count(DISTINCT database) FROM system.tables WHERE database LIKE 'bench_%'")
    db_count = cur.fetchone()[0]
    print(f"  Databases: {db_count}")
    print(f"  Tables:    {table_count}")

    # Verify ownership counts per role
    for role in ["role_heavy", "role_medium", "role_light"]:
        cur.execute(f"SELECT count(*) FROM system.tables WHERE owner = '{role}'")
        count = cur.fetchone()[0]
        print(f"  {role:15s} owns {count} tables")

    for user, user_roles in user_setup:
        role_list = ", ".join(user_roles)
        print(f"  User {user:10s} -> roles: {role_list}")

    elapsed = time.time() - total_start
    print(f"\nTotal setup time: {elapsed:.1f}s ({elapsed/60:.1f}m)")
    print("Ready to benchmark. Run: python3 tests/ownership_bench/run_bench.py")


if __name__ == "__main__":
    main()
