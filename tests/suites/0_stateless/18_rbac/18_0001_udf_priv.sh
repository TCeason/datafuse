#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "=== test UDF priv"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql --user=test-user --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

stmt "set global enable_experimental_rbac_check=1"

stmt "drop user if exists 'test-user'"
stmt "DROP FUNCTION IF EXISTS f1;"
stmt "DROP FUNCTION IF EXISTS f2;"
stmt "drop table if exists default.t;"
stmt "drop table if exists default.t2;"

stmt "CREATE FUNCTION f1 AS (p) -> (p)"
stmt "CREATE FUNCTION f2 AS (p) -> (p)"
stmt "create table default.t(i UInt8 not null);"
stmt "create table default.t2(i UInt8 not null);"

## create user
stmt "create user 'test-user' IDENTIFIED BY '$TEST_USER_PASSWORD'"
stmt "grant insert, delete, update, select on default.t to 'test-user';"
stmt "grant select on default.t to 'test-user';"
stmt "grant super on *.* to 'test-user';"
sleep 2;

# error test need privielge f1
echo "=== Only Has Privilege on f2 ==="
stmt "grant usage on udf f2 to 'test-user';"
sleep 1;
stmt_with_user "select f2(f1(1));" "$TEST_USER_CONNECT"
# bug need error
stmt_with_user "select add(1, f2(f1(1)));"  "$TEST_USER_CONNECT"
stmt_with_user "select max(f2(f1(1)));" "$TEST_USER_CONNECT"
stmt_with_user "select 1 from (select f2(f1(10)));" "$TEST_USER_CONNECT"
stmt_with_user "select * from system.one where f2(f1(1));" "$TEST_USER_CONNECT"
# bug need error
stmt_with_user "insert into t values (f2(f1(1)));" | "$TEST_USER_CONNECT"
stmt_with_user "update t set i=f2(f1(2)) where i=f2(f1(1));" | "$TEST_USER_CONNECT"
# bug need error
stmt_with_user "SELECT i,	nth_value(i, f2(f1(2))) OVER (PARTITION BY i) fv FROM t" "$TEST_USER_CONNECT"
stmt_with_user "delete from t where f2(f1(1));" "$TEST_USER_CONNECT"
stmt_with_user "select f2(f1(1)) union all select 2 order by \`f2(f1(1))\`" "$TEST_USER_CONNECT"
stmt_with_user "select 22, t.i as ti from t join (select f2(f1(1)), i from t) as t1 on false;" "$TEST_USER_CONNECT"
stmt_with_user "select f2(f1(1)), t.i as ti from t join (select 22, i from t) as t1 on false;" "$TEST_USER_CONNECT"
stmt_with_user "select 22, t.i as ti from t join (select 22, i from t) as t1 on f2(f1(false));" "$TEST_USER_CONNECT"
stmt_with_user "set enable_experimental_merge_into = 1; merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when matched then update set t3.i=f2(f1(13));" "$TEST_USER_CONNECT"
stmt_with_user "set enable_experimental_merge_into = 1; merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when not matched  then insert (i) values(f2(f1(100)));" "$TEST_USER_CONNECT"
# Same issue: https://github.com/datafuselabs/databend/issues/13727, better to fix it in other issue.
#echo "REPLACE INTO t ON(i) values (f2(f1(2))), (3), (4)" | "$TEST_USER_CONNECT"
#echo "insert into t select f2(f1(33))" | "$TEST_USER_CONNECT"
stmt_with_user "delete from t;" "$TEST_USER_CONNECT"
stmt_with_user "insert into t values (99),(199);" "$TEST_USER_CONNECT"
stmt_with_user "select case when i > f2(f1(100)) then 200 else 100 end as c1 from t;" "$TEST_USER_CONNECT"
stmt_with_user "select case when i > 100 then 200 else f2(f1(100)) end as c1 from t;" "$TEST_USER_CONNECT"
stmt_with_user "select case when i > 100 then f2(f1(200)) else 100 end as c1 from t;" "$TEST_USER_CONNECT"
stmt_with_user "delete from t;" "$TEST_USER_CONNECT"

# error test need privielge f1
echo "=== Only Has Privilege on f1 ==="
stmt "grant usage on udf f1 to 'test-user';"
stmt "revoke usage on udf f2 from 'test-user';"
sleep 1;
stmt_with_user "select f2(f1(1))" "$TEST_USER_CONNECT"
stmt_with_user "select add(1, f1(1))" "$TEST_USER_CONNECT"
stmt_with_user "select max(f2(f1(1)));" "$TEST_USER_CONNECT"
stmt_with_user "select 1 from (select f2(f1(10)));" "$TEST_USER_CONNECT"
stmt_with_user "select * from system.one where f2(f1(1));" "$TEST_USER_CONNECT"
# bug need error
stmt_with_user "insert into t values (f2(f1(1)));" "$TEST_USER_CONNECT"
stmt_with_user "update t set i=f2(f1(2)) where i=f2(f1(1));" "$TEST_USER_CONNECT"
# bug need error
stmt_with_user "SELECT i,	nth_value(i, f2(f1(2))) OVER (PARTITION BY i) fv FROM t" "$TEST_USER_CONNECT"
stmt_with_user "delete from t where f2(f1(1));" "$TEST_USER_CONNECT"
stmt_with_user "select f2(f1(1)) union all select 2 order by \`f2(f1(1))\`" "$TEST_USER_CONNECT"
stmt_with_user "select 22, t.i as ti from t join (select f2(f1(1)), i from t) as t1 on false;" "$TEST_USER_CONNECT"
stmt_with_user "select f2(f1(1)), t.i as ti from t join (select 22, i from t) as t1 on false;" "$TEST_USER_CONNECT"
stmt_with_user "select 22, t.i as ti from t join (select 22, i from t) as t1 on f2(f1(false));" "$TEST_USER_CONNECT"
stmt_with_user "set enable_experimental_merge_into = 1; merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when matched then update set t3.i=f2(f1(13));" "$TEST_USER_CONNECT"
stmt_with_user "set enable_experimental_merge_into = 1; merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when not matched  then insert (i) values(f2(f1(100)));" "$TEST_USER_CONNECT"
# Same issue: https://github.com/datafuselabs/databend/issues/13727, better to fix it in other issue.
#echo "REPLACE INTO t ON(i) values (f2(f1(2))), (3), (4)" | "$TEST_USER_CONNECT"
#echo "insert into t select f2(f1(33))" | "$TEST_USER_CONNECT"
stmt_with_user "delete from t;" "$TEST_USER_CONNECT"
stmt_with_user "insert into t values (99),(199);" "$TEST_USER_CONNECT"
stmt_with_user "select case when i > f2(f1(100)) then 200 else 100 end as c1 from t;" "$TEST_USER_CONNECT"
stmt_with_user "select case when i > 100 then 200 else f2(f1(100)) end as c1 from t;" "$TEST_USER_CONNECT"
stmt_with_user "select case when i > 100 then f2(f1(200)) else 100 end as c1 from t;" "$TEST_USER_CONNECT"
stmt_with_user "delete from t;" "$TEST_USER_CONNECT"

echo "=== Has Privilege on f1, f2 ==="
stmt "grant usage on udf f1 to 'test-user';"
stmt "grant all on udf f2 to 'test-user';"
stmt_with_user "select f2(f1(1))" "$TEST_USER_CONNECT"
stmt_with_user "select add(1, f1(1))" "$TEST_USER_CONNECT"
stmt_with_user "select max(f2(f1(1)));" "$TEST_USER_CONNECT"
stmt_with_user "select 1 from (select f2(f1(10)));" "$TEST_USER_CONNECT"
stmt_with_user "select * from system.one where f2(f1(1));" "$TEST_USER_CONNECT"
# bug need error
stmt_with_user "insert into t values (f2(f1(1)));" "$TEST_USER_CONNECT"
stmt_with_user "update t set i=f2(f1(2)) where i=f2(f1(1));" "$TEST_USER_CONNECT"
# bug need error
stmt_with_user "SELECT i,	nth_value(i, f2(f1(2))) OVER (PARTITION BY i) fv FROM t" "$TEST_USER_CONNECT"
stmt_with_user "delete from t where f2(f1(1));" "$TEST_USER_CONNECT"
stmt_with_user "select f2(f1(1)) union all select 2 order by \`f2(f1(1))\`" "$TEST_USER_CONNECT"
stmt_with_user "select 22, t.i as ti from t join (select f2(f1(1)), i from t) as t1 on false;" "$TEST_USER_CONNECT"
stmt_with_user "select f2(f1(1)), t.i as ti from t join (select 22, i from t) as t1 on false;" "$TEST_USER_CONNECT"
stmt_with_user "select 22, t.i as ti from t join (select 22, i from t) as t1 on f2(f1(false));" "$TEST_USER_CONNECT"
stmt_with_user "set enable_experimental_merge_into = 1; merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when matched then update set t3.i=f2(f1(13));" "$TEST_USER_CONNECT"
stmt_with_user "set enable_experimental_merge_into = 1; merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when not matched  then insert (i) values(f2(f1(100)));" "$TEST_USER_CONNECT"
# Same issue: https://github.com/datafuselabs/databend/issues/13727, better to fix it in other issue.
#echo "REPLACE INTO t ON(i) values (f2(f1(2))), (3), (4)" | "$TEST_USER_CONNECT"
#echo "insert into t select f2(f1(33))" | "$TEST_USER_CONNECT"
stmt_with_user "delete from t;" "$TEST_USER_CONNECT"
stmt_with_user "insert into t values (99),(199);" "$TEST_USER_CONNECT"
stmt_with_user "select case when i > f2(f1(100)) then 200 else 100 end as c1 from t;" "$TEST_USER_CONNECT"
stmt_with_user "select case when i > 100 then 200 else f2(f1(100)) end as c1 from t;" "$TEST_USER_CONNECT"
stmt_with_user "select case when i > 100 then f2(f1(200)) else 100 end as c1 from t;" "$TEST_USER_CONNECT"
stmt_with_user "delete from t;" "$TEST_USER_CONNECT"

#udf server test
stmt "drop function if exists a;"
stmt "drop function if exists b;"
stmt "CREATE FUNCTION a (TINYINT, SMALLINT, INT, BIGINT) RETURNS BIGINT LANGUAGE python HANDLER = 'add_signed' ADDRESS = 'http://0.0.0.0:8815';"
stmt "CREATE FUNCTION b (TINYINT, SMALLINT, INT, BIGINT) RETURNS BIGINT LANGUAGE python HANDLER = 'add_signed' ADDRESS = 'http://0.0.0.0:8815';"

stmt "grant usage on udf a to 'test-user'"
stmt_with_user "select a(1,1,1,1)" "$TEST_USER_CONNECT"
stmt_with_user "select b(1,1,1,1)" "$TEST_USER_CONNECT"


echo "drop user if exists 'test-user'" | $BENDSQL_CLIENT_CONNECT
echo "DROP FUNCTION IF EXISTS f1;" |  $BENDSQL_CLIENT_CONNECT
echo "DROP FUNCTION IF EXISTS f2;" |  $BENDSQL_CLIENT_CONNECT
echo "drop function if exists a;" | $BENDSQL_CLIENT_CONNECT
echo "drop function if exists b;" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists default.t;" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists default.t2;" | $BENDSQL_CLIENT_CONNECT
echo "unset enable_experimental_rbac_check" | $BENDSQL_CLIENT_CONNECT
