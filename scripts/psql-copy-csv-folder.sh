#!/bin/sh
#
# COPY every *.csv under a directory into PostgreSQL (server-side paths).
#
# Usage:
#   ./psql-copy-csv-folder.sh /data1
#   DRY_RUN=1 ./psql-copy-csv-folder.sh /data1

set -eu

DIR="${1:-}"
TABLE="${2:-fp_token_example}"
DRY_RUN="${DRY_RUN:-0}"

if [ -z "$DIR" ]; then
  echo "Usage: $0 <csv-directory-on-db-server> [table_name]" >&2
  exit 1
fi

if [ ! -d "$DIR" ]; then
  echo "ERROR: not a directory: $DIR" >&2
  exit 1
fi

DIR="$(cd "$DIR" && pwd)"

PSQL_CMD="psql -v ON_ERROR_STOP=1"
[ -n "${PGDATABASE:-}" ] && PSQL_CMD="$PSQL_CMD -d $PGDATABASE"
[ -n "${PGUSER:-}" ]     && PSQL_CMD="$PSQL_CMD -U $PGUSER"
[ -n "${PGHOST:-}" ]     && PSQL_CMD="$PSQL_CMD -h $PGHOST"
[ -n "${PGPORT:-}" ]     && PSQL_CMD="$PSQL_CMD -p $PGPORT"
[ -n "${PSQL_EXTRA_OPTS:-}" ] && PSQL_CMD="$PSQL_CMD $PSQL_EXTRA_OPTS"

COUNT=$(find "$DIR" -type f -name '*.csv' | wc -l | tr -d ' \t')
if [ "$COUNT" -eq 0 ]; then
  echo "ERROR: no *.csv under $DIR" >&2
  exit 1
fi

find "$DIR" -type f -name '*.csv' | sort | while IFS= read -r f; do
  [ -n "$f" ] || continue
  if [ "$DRY_RUN" = "1" ]; then
    echo "---------- $f ----------"
    printf '%s\n' '\timing on'
    printf "COPY %s(hashstr, txt1, txt2, txt3, txt4)\n" "$TABLE"
    printf "FROM '%s'\n" "$f"
    printf '%s\n' "WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');"
    echo
    continue
  fi
  echo "==> COPY $f"
  {
    printf '%s\n' '\timing on'
    printf "COPY %s(hashstr, txt1, txt2, txt3, txt4)\n" "$TABLE"
    printf "FROM '%s'\n" "$f"
    printf '%s\n' "WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');"
  } | eval "$PSQL_CMD"
done

echo "Done. Processed $COUNT file(s)."
