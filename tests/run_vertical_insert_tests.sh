#!/usr/bin/env bash

set -euo pipefail
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SERVER_BIN="$ROOT_DIR/build/programs/clickhouse-server"
CLIENT_BIN="$ROOT_DIR/build/programs/clickhouse-client"

if [[ ! -x "$SERVER_BIN" ]]; then
  echo "clickhouse-server not found at $SERVER_BIN" >&2
  exit 1
fi

if [[ ! -x "$CLIENT_BIN" ]]; then
  echo "clickhouse-client not found at $CLIENT_BIN" >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv not found in PATH; install it or adjust the test runner." >&2
  exit 1
fi

DATA_DIR="$ROOT_DIR/.tmp/vertical-insert-test"
LOG_FILE="$DATA_DIR/server.log"
rm -rf \
  "$DATA_DIR/data" \
  "$DATA_DIR/metadata" \
  "$DATA_DIR/tmp" \
  "$DATA_DIR/user_files" \
  "$DATA_DIR/format_schemas" \
  "$DATA_DIR/preprocessed_configs" \
  "$DATA_DIR/metadata_dropped" \
  "$DATA_DIR/access" \
  "$DATA_DIR/flags"
rm -f "$DATA_DIR/status" "$LOG_FILE"
mkdir -p "$DATA_DIR"
mkdir -p \
  "$DATA_DIR/data" \
  "$DATA_DIR/metadata" \
  "$DATA_DIR/tmp" \
  "$DATA_DIR/user_files" \
  "$DATA_DIR/format_schemas"

CONFIG_FILE="$DATA_DIR/embedded_with_trace.xml"
cat > "$CONFIG_FILE" <<'XML'
<!-- Local config for vertical insert tests. -->
<clickhouse>
    <logger>
        <level>trace</level>
        <console>true</console>
    </logger>

    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <mysql_port>9004</mysql_port>
    <postgresql_port>9005</postgresql_port>

    <path>./</path>

    <mlock_executable>true</mlock_executable>

    <send_crash_reports>
        <enabled>true</enabled>
        <send_logical_errors>true</send_logical_errors>
        <endpoint>https://crash.clickhouse.com/</endpoint>
    </send_crash_reports>

    <trace_log>
        <database>system</database>
        <table>trace_log</table>
        <flush_interval_milliseconds>2000</flush_interval_milliseconds>
    </trace_log>

    <query_log>
        <database>system</database>
        <table>query_log</table>
        <flush_interval_milliseconds>2000</flush_interval_milliseconds>
    </query_log>

    <http_options_response>
        <header>
            <name>Access-Control-Allow-Origin</name>
            <value>*</value>
        </header>
        <header>
            <name>Access-Control-Allow-Headers</name>
            <value>origin, x-requested-with, x-clickhouse-format, x-clickhouse-user, x-clickhouse-key, Authorization</value>
        </header>
        <header>
            <name>Access-Control-Allow-Methods</name>
            <value>POST, GET, OPTIONS</value>
        </header>
        <header>
            <name>Access-Control-Max-Age</name>
            <value>86400</value>
        </header>
    </http_options_response>

    <users>
        <default>
            <password></password>

            <networks>
                <ip>::/0</ip>
            </networks>

            <profile>default</profile>
            <quota>default</quota>

            <access_management>1</access_management>
            <named_collection_control>1</named_collection_control>
        </default>
    </users>

    <profiles>
        <default/>
    </profiles>

    <quotas>
        <default />
    </quotas>
</clickhouse>
XML

pick_free_port() {
  /usr/bin/python3 - <<'PY'
import socket, random
for _ in range(200):
    port = random.randrange(19000, 20000)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(("127.0.0.1", port)) != 0:
            print(port)
            raise SystemExit(0)
raise SystemExit(1)
PY
}

TCP_PORT=${CLICKHOUSE_PORT_TCP:-$(pick_free_port)}
HTTP_PORT=${CLICKHOUSE_PORT_HTTP:-$(pick_free_port)}
INTERSERVER_PORT=${CLICKHOUSE_PORT_INTERSERVER:-$(pick_free_port)}

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

(
  cd "$DATA_DIR"
  "$SERVER_BIN" \
    --config-file "$CONFIG_FILE" \
    -- \
    --listen_host=127.0.0.1 \
    --path="$DATA_DIR/" \
    --tmp_path="$DATA_DIR/tmp/" \
    --user_files_path="$DATA_DIR/user_files/" \
    --format_schema_path="$DATA_DIR/format_schemas/" \
    --mysql_port=0 \
    --postgresql_port=0 \
    --tcp_port="$TCP_PORT" \
    --http_port="$HTTP_PORT" \
    --interserver_http_port="$INTERSERVER_PORT" \
    >"$LOG_FILE" 2>&1
) &
SERVER_PID=$!

for _ in {1..120}; do
  if "$CLIENT_BIN" --host 127.0.0.1 --port "$TCP_PORT" --query "SELECT 1" >/dev/null 2>&1; then
    if kill -0 "$SERVER_PID" >/dev/null 2>&1; then
      break
    fi
  fi
  if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

if ! "$CLIENT_BIN" --host 127.0.0.1 --port "$TCP_PORT" --query "SELECT 1" >/dev/null 2>&1; then
  echo "clickhouse-server failed to start; see $LOG_FILE" >&2
  tail -n 200 "$LOG_FILE" >&2 || true
  exit 1
fi

export CLICKHOUSE_CLIENT="$CLIENT_BIN"
export CLICKHOUSE_HOST=127.0.0.1
export CLICKHOUSE_PORT_TCP="$TCP_PORT"
export CLICKHOUSE_PORT_HTTP="$HTTP_PORT"
export CLICKHOUSE_PORT_HTTP_PROTO="http"
export CLICKHOUSE_URL="http://127.0.0.1:$HTTP_PORT/"

TESTS=(
  03810_vertical_insert_basic
  03811_vertical_insert_permutation
  03812_vertical_insert_skip_index_single
  03813_vertical_insert_skip_index_multi
  03814_vertical_insert_nested_offsets
  03816_vertical_insert_ttl
  03817_vertical_insert_projections
  03818_vertical_insert_projection_aggregate
  03819_vertical_insert_projection_order
  03820_vertical_insert_projection_defaults
  03821_vertical_insert_projection_ttl
  03822_vertical_insert_projection_skip_index_stats
  03823_vertical_insert_text_index_order_by
  03824_vertical_insert_text_index_not_order_by
  03825_vertical_insert_text_index_multi
  03826_vertical_insert_text_index_multi_order_mix
  03827_vertical_insert_text_index_expression
  03828_vertical_insert_text_index_expression_lower
  03829_vertical_insert_delayed_streams_off
  03830_vertical_insert_delayed_streams_on
  03831_vertical_insert_statistics_merging
  03832_vertical_insert_statistics_gathering
  03833_vertical_insert_statistics_mixed
  03834_vertical_insert_skipped
  03835_vertical_insert_projection_results
  03836_vertical_insert_sparse_serialization
  03837_vertical_insert_lowcard_nullable
  03838_vertical_insert_defaults_missing_columns
  03839_vertical_insert_checksums
  03840_vertical_insert_row_count_consistency
  03841_vertical_insert_concurrent
  03842_vertical_insert_batch_sizes
  03843_vertical_insert_map_tuple
  03844_vertical_insert_materialized_alias
    03845_vertical_insert_nullable_many_nulls
    03846_vertical_insert_nested_sort_key
    03847_vertical_insert_multi_nested_sort_key
  03848_vertical_insert_lowcard_key
  03849_vertical_insert_nullable_key
  03850_vertical_insert_array_key
  03851_vertical_insert_tuple_key
  03852_vertical_insert_map_key
  03853_vertical_insert_lowcard_nullable_key
  03854_vertical_insert_decimal_key
  03855_vertical_insert_shuffled_insert_order
  03856_vertical_insert_activation_thresholds
  03857_vertical_insert_partition_key
  03858_vertical_insert_sample_primary_key
  03859_vertical_insert_versioned_collapsing
  03860_vertical_insert_order_by_expression
  03861_vertical_insert_default_alias_key
  03862_vertical_insert_enum_uuid_key
  03863_vertical_insert_replacing_is_deleted
  03864_vertical_insert_ipv4_ipv6_key
  03865_vertical_insert_collate_lowcard_key
  03866_vertical_insert_merge_behavior
  03867_vertical_insert_batch_bytes
)

TEST_RUNNER=(uv run --with jinja2 -- "$ROOT_DIR/tests/clickhouse-test")

for test_name in "${TESTS[@]}"; do
  echo "Running $test_name"
  "${TEST_RUNNER[@]}" --binary "$ROOT_DIR/build/programs/clickhouse" --testname "$test_name"
done
