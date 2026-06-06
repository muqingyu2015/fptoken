#!/bin/sh
#
# Write one .sql with COPY for every *.csv under a directory.
# POSIX sh (dash/bash). Do not use "sh script.sh" on a CRLF copy — run dos2unix first.
#
# Usage:
#   ./psql-copy-csv-folder-batch.sh /data1 /data1/load_all.sql
#   psql -v ON_ERROR_STOP=1 -f /data1/load_all.sql

set -eu

DIR="${1:-}"
OUT_SQL="${2:-}"
TABLE="${3:-fp_token_example}"

if [ -z "$DIR" ] || [ -z "$OUT_SQL" ]; then
  echo "Usage: $0 <csv-directory> <output.sql> [table_name]" >&2
  exit 1
fi

if [ ! -d "$DIR" ]; then
  echo "ERROR: not a directory: $DIR" >&2
  exit 1
fi

DIR="$(cd "$DIR" && pwd)"

# POSIX: avoid bash-only "done < <(find ...)". Works with sh/bash.
COUNT=$(find "$DIR" -type f -name '*.csv' | wc -l | tr -d ' \t')

if [ "$COUNT" -eq 0 ]; then
  echo "ERROR: no *.csv under $DIR" >&2
  exit 1
fi

{
  printf '%s\n' '\timing on'
  find "$DIR" -type f -name '*.csv' | sort | while IFS= read -r f; do
    [ -n "$f" ] || continue
    printf '\n-- %s\n' "$(basename "$f")"
    printf "COPY %s(hashstr, txt1, txt2, txt3, txt4)\n" "$TABLE"
    printf "FROM '%s'\n" "$f"
    printf '%s\n' "WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');"
  done
} >"$OUT_SQL"

if [ ! -s "$OUT_SQL" ]; then
  echo "ERROR: failed to write SQL (empty output)" >&2
  rm -f "$OUT_SQL"
  exit 1
fi

echo "Wrote $COUNT COPY statements -> $OUT_SQL"
echo "Run: psql -v ON_ERROR_STOP=1 -f $OUT_SQL"
