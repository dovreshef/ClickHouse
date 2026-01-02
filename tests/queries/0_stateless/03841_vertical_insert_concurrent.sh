#!/usr/bin/env bash
# Test: concurrent vertical inserts are safe and produce consistent results.

set -euo pipefail

CLIENT="${CLICKHOUSE_CLIENT} --host ${CLICKHOUSE_HOST:-127.0.0.1} --port ${CLICKHOUSE_PORT_TCP:-9000}"

$CLIENT --query "DROP TABLE IF EXISTS t_vi_concurrent"

$CLIENT --query "
CREATE TABLE t_vi_concurrent
(
    k UInt64,
    v String
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;"

for i in 0 1 2 3; do
  $CLIENT --query "INSERT INTO t_vi_concurrent SELECT number + ${i} * 1000, toString(number) FROM numbers(1000)" &
done
wait

$CLIENT --query "SELECT count() FROM t_vi_concurrent"

$CLIENT --query "DROP TABLE t_vi_concurrent"
