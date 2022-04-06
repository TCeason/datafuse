---
title: SHOW TABLES
---

Shows the list of tables in the currently selected database.

## Syntax

```
SHOW TABLES  [LIKE 'pattern' | WHERE expr | FROM 'pattern' | IN 'pattern']
```

## Examples

```sql
mysql> SHOW TABLES;
+---------------+
| table_name    |
+---------------+
| clusters      |
| contributors  |
| databases     |
| functions     |
| numbers       |
| numbers_local |
| numbers_mt    |
| one           |
| processes     |
| settings      |
| tables        |
| tracing       |
+---------------+
```

Showing the tables with table name `"numbers_local"`:
```sql
mysql> SHOW TABLES LIKE 'settings';
+------------+
| table_name |
+------------+
| settings   |
+------------+
```

Showing the tables begin with `"numbers"`:
```sql
mysql> SHOW TABLES LIKE 'co%';
+--------------+
| table_name   |
+--------------+
| columns      |
| configs      |
| contributors |
+--------------+
```

Showing the tables begin with `"numbers"` with `WHERE`:
```sql
mysql> SHOW TABLES WHERE table_name LIKE 'co%';
+--------------+
| table_name   |
+--------------+
| columns      |
| configs      |
| contributors |
+--------------+
```

Showing the tables are inside `"ss"`:
```sql
mysql> SHOW TABLES FROM 'system';
+--------------+
| table_name   |
+--------------+
| clusters     |
| columns      |
| configs      |
| contributors |
| credits      |
| databases    |
| engines      |
| functions    |
| metrics      |
| one          |
| processes    |
| query_log    |
| roles        |
| settings     |
| tables       |
| tracing      |
| users        |
| warehouses   |
+--------------+
```
