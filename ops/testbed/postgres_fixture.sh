#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  echo "usage: ops/testbed/postgres_fixture.sh -- <command> [args...]" >&2
}

die() {
  echo "postgres_fixture: $*" >&2
  exit 77
}

find_pg_bin() {
  if [ -n "${PG_BIN:-}" ] && [ -x "$PG_BIN/initdb" ] && [ -x "$PG_BIN/pg_ctl" ]; then
    printf '%s\n' "$PG_BIN"
    return 0
  fi

  if command -v initdb >/dev/null 2>&1 && command -v pg_ctl >/dev/null 2>&1; then
    local_pg_bin="$(dirname "$(command -v initdb)")"
    if "$local_pg_bin/postgres" --version 2>/dev/null | grep -q ' 16\.'; then
      printf '%s\n' "$local_pg_bin"
      return 0
    fi
  fi

  if command -v brew >/dev/null 2>&1; then
    brew_pg_prefix="$(HOMEBREW_NO_AUTO_UPDATE=1 brew --prefix postgresql@16 2>/dev/null || true)"
    if [ -n "$brew_pg_prefix" ] && [ -x "$brew_pg_prefix/bin/initdb" ]; then
      printf '%s\n' "$brew_pg_prefix/bin"
      return 0
    fi
  fi

  if [ -x /usr/lib/postgresql/16/bin/initdb ]; then
    printf '%s\n' /usr/lib/postgresql/16/bin
    return 0
  fi

  return 1
}

find_psql() {
  if [ -n "${PG_BIN:-}" ] && [ -x "$PG_BIN/psql" ]; then
    printf '%s\n' "$PG_BIN/psql"
    return 0
  fi

  if command -v psql >/dev/null 2>&1; then
    command -v psql
    return 0
  fi

  if command -v brew >/dev/null 2>&1; then
    brew_pg_prefix="$(HOMEBREW_NO_AUTO_UPDATE=1 brew --prefix postgresql@16 2>/dev/null || true)"
    if [ -n "$brew_pg_prefix" ] && [ -x "$brew_pg_prefix/bin/psql" ]; then
      printf '%s\n' "$brew_pg_prefix/bin/psql"
      return 0
    fi
  fi

  if [ -x /usr/lib/postgresql/16/bin/psql ]; then
    printf '%s\n' /usr/lib/postgresql/16/bin/psql
    return 0
  fi

  return 1
}

free_port() {
  python3 - <<'PY'
import socket
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind(("127.0.0.1", 0))
    print(s.getsockname()[1])
PY
}

validate_dsn() {
  "$fixture_psql" "$1" -Atq -c 'SELECT 1' >/dev/null
}

cleanup() {
  cleanup_rc=0
  if [ "${fixture_owned_cluster:-0}" = "1" ]; then
    if [ -n "${fixture_data_dir:-}" ] && [ -x "${fixture_pg_ctl:-}" ]; then
      "$fixture_pg_ctl" -D "$fixture_data_dir" -m fast -w stop >&2 || cleanup_rc=$?
    fi
    if [ -n "${fixture_base_dir:-}" ] && [ -d "$fixture_base_dir" ]; then
      find "$fixture_base_dir" -depth -delete >&2 || cleanup_rc=$?
    fi
  fi
  if [ "$cleanup_rc" -ne 0 ]; then
    echo "postgres_fixture: cleanup failed with exit $cleanup_rc" >&2
  fi
}

if [ "${1:-}" = "--" ]; then
  shift
fi

if [ "$#" -eq 0 ]; then
  usage
  exit 2
fi

fixture_owned_cluster=0
fixture_base_dir=""
fixture_data_dir=""
fixture_pg_ctl=""

trap 'fixture_exit=$?; cleanup; exit "$fixture_exit"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [ -n "${TEST_POSTGRES_DSN:-}" ]; then
  fixture_psql="$(find_psql)" || die "TEST_POSTGRES_DSN was provided, but psql was not found"
  validate_dsn "$TEST_POSTGRES_DSN" || die "provided TEST_POSTGRES_DSN is not reachable"
else
  fixture_pg_bin="$(find_pg_bin)" || die "no native PostgreSQL 16 tooling found"
  fixture_initdb="$fixture_pg_bin/initdb"
  fixture_pg_ctl="$fixture_pg_bin/pg_ctl"
  fixture_psql="$fixture_pg_bin/psql"
  fixture_createdb="$fixture_pg_bin/createdb"
  fixture_postgres="$fixture_pg_bin/postgres"

  "$fixture_postgres" --version | grep -q ' 16\.' || die "PostgreSQL 16 is required: $("$fixture_postgres" --version 2>/dev/null || true)"

  fixture_port="$(free_port)"
  fixture_base_dir="$(mktemp -d "${TMPDIR:-/tmp}/mdfeed-pg16.XXXXXX")"
  fixture_data_dir="$fixture_base_dir/data"
  fixture_socket_dir="$fixture_base_dir/s"
  fixture_log_file="$fixture_base_dir/postgres.log"
  fixture_role="mdfeed_test"
  fixture_db="mdfeed_test"
  mkdir -p "$fixture_socket_dir"

  "$fixture_initdb" -D "$fixture_data_dir" --locale=C -E UTF8 -A trust --no-instructions >/dev/null
  {
    printf "listen_addresses = '127.0.0.1'\n"
    printf "port = %s\n" "$fixture_port"
    printf "unix_socket_directories = '%s'\n" "$fixture_socket_dir"
  } >> "$fixture_data_dir/postgresql.conf"
  {
    printf "local all all trust\n"
    printf "host all all 127.0.0.1/32 trust\n"
    printf "host all all ::1/128 reject\n"
  } > "$fixture_data_dir/pg_hba.conf"

  fixture_owned_cluster=1
  "$fixture_pg_ctl" -D "$fixture_data_dir" -l "$fixture_log_file" -w -t 30 start >/dev/null
  "$fixture_psql" -h 127.0.0.1 -p "$fixture_port" -U "$USER" -d postgres -v ON_ERROR_STOP=1 -q <<SQL
CREATE ROLE ${fixture_role} LOGIN CREATEDB;
SQL
  "$fixture_createdb" -h 127.0.0.1 -p "$fixture_port" -U "$USER" -O "$fixture_role" "$fixture_db"
  TEST_POSTGRES_DSN="postgresql://${fixture_role}@127.0.0.1:${fixture_port}/${fixture_db}"
  TEST_POSTGRES_DATA_DIR="$fixture_data_dir"
  TEST_POSTGRES_PORT="$fixture_port"
  export TEST_POSTGRES_DSN
  export TEST_POSTGRES_DATA_DIR
  export TEST_POSTGRES_PORT
fi

export MDFEED_STORAGE_PROFILE=test
export MDFEED_ADAPTERS=replay
export MDFEED_REPLAY_LOOP=0
export MDFEED_REPLAY_RESTAMP=0
export DATABASE_URL="$TEST_POSTGRES_DSN"

"$@"
