#!/usr/bin/env python3
"""Cleanup all benchmark data."""

import sys

try:
    from databend_driver import BlockingDatabendClient
except ImportError:
    print("ERROR: pip install databend-driver")
    sys.exit(1)

DSN = "databend://root:@localhost:8001/?sslmode=disable"


def main():
    client = BlockingDatabendClient(DSN)
    cur = client.cursor()

    print("Dropping users...")
    for user in ["u_admin", "u_grant", "u_owner", "u_mixed"]:
        cur.execute(f"DROP USER IF EXISTS '{user}'")

    print("Dropping roles...")
    for role in ["role_heavy", "role_medium", "role_light", "role_grant", "role_empty1", "role_empty2"]:
        cur.execute(f"DROP ROLE IF EXISTS '{role}'")

    print("Dropping databases...")
    for i in range(25):
        cur.execute(f"DROP DATABASE IF EXISTS bench_big_{i:02d}")
    for i in range(25):
        cur.execute(f"DROP DATABASE IF EXISTS bench_small_{i:02d}")

    print("Cleanup done.")


if __name__ == "__main__":
    main()
