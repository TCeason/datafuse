#!/usr/bin/env python3
"""Debug: check what extra rows u_few_owner sees."""

from databend_driver import BlockingDatabendClient

HOST = "localhost"
PORT = 8001


def get_cursor(user="root", password=""):
    dsn = f"databend://{user}:{password}@{HOST}:{PORT}/?sslmode=disable"
    client = BlockingDatabendClient(dsn)
    return client.cursor()


def main():
    root = get_cursor()

    # Setup
    root.execute("CREATE ROLE IF NOT EXISTS 'role_few_tables'")
    root.execute("CREATE USER IF NOT EXISTS 'u_few_owner' IDENTIFIED BY '123'")
    root.execute("GRANT ROLE 'role_few_tables' TO 'u_few_owner'")
    for i in range(5):
        root.execute(f"GRANT OWNERSHIP ON bench_big_{i:02d}.t_0500 TO ROLE 'role_few_tables'")

    cur = get_cursor("u_few_owner", "123")

    print("=== 1 db query ===")
    cur.execute("SELECT database, name, owner FROM system.tables WHERE database = 'bench_big_00'")
    rows = cur.fetchall()
    print(f"Rows: {len(rows)}")
    for row in rows:
        print(f"  {row}")

    print("\n=== 3 db query ===")
    cur.execute("SELECT database, name, owner FROM system.tables WHERE database IN ('bench_big_00','bench_big_01','bench_big_02')")
    rows = cur.fetchall()
    print(f"Rows: {len(rows)}")
    for row in rows:
        print(f"  {row}")

    # Cleanup
    for i in range(5):
        try:
            root.execute(f"GRANT OWNERSHIP ON bench_big_{i:02d}.t_0500 TO ROLE 'account_admin'")
        except Exception:
            pass
    root.execute("DROP USER IF EXISTS 'u_few_owner'")
    root.execute("DROP ROLE IF EXISTS 'role_few_tables'")


if __name__ == "__main__":
    main()
