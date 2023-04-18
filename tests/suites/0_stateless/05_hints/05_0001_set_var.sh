#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# mariadb mysql client has some bug, please use mysql offical client
# mysql --version
# mysql  Ver 8.0.32-0ubuntu0.20.04.2 for Linux on x86_64 ((Ubuntu))
echo "select /*+SET_VAR(timezone='Asia/Shanghai') SET_VAR(storage_read_buffer_size=200)*/ /*+SET_VAR(storage_read_buffer_size=100)*/name, /*+xx*/ value from system.settings where name in ('timezone', 'storage_read_buffer_size')" |  $MYSQL_CLIENT_CONNECT
