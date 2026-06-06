#!/usr/bin/env python3
"""
Remove CSV rows where any column exceeds a byte/char limit (for PostgreSQL COPY).

Example:
  py -3 scripts/filter-csv-long-rows.py -i sample-data/web-corpus/fulltext_web_0001.csv
  py -3 scripts/filter-csv-long-rows.py -i data/*.csv -o data/cleaned --max-field-bytes 10485760
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path


def field_len(value: str, mode: str) -> int:
    if mode == "bytes":
        return len(value.encode("utf-8", errors="replace"))
    return len(value)


def process_file(
    src: Path,
    dst: Path,
    max_len: int,
    len_mode: str,
    check_columns: list[int] | None,
) -> tuple[int, int, int]:
    kept = 0
    dropped = 0
    max_seen = 0

    dst.parent.mkdir(parents=True, exist_ok=True)
    with src.open("r", encoding="utf-8", errors="replace", newline="") as fin, dst.open(
        "w", encoding="utf-8", newline=""
    ) as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout, lineterminator="\n")
        try:
            header = next(reader)
        except StopIteration:
            return 0, 0, 0
        writer.writerow(header)

        cols = check_columns
        if cols is None:
            cols = list(range(len(header)))

        for row in reader:
            if not row:
                dropped += 1
                continue
            too_long = False
            for i in cols:
                if i >= len(row):
                    continue
                n = field_len(row[i], len_mode)
                if n > max_seen:
                    max_seen = n
                if n > max_len:
                    too_long = True
                    break
            if too_long:
                dropped += 1
                continue
            writer.writerow(row)
            kept += 1
            if kept % 500_000 == 0:
                print(f"  {src.name}: kept={kept} dropped={dropped}", flush=True)

    return kept, dropped, max_seen


def main() -> int:
    ap = argparse.ArgumentParser(description="Drop CSV rows with overly long fields")
    ap.add_argument("-i", "--input", nargs="+", required=True, help="CSV file(s) or glob")
    ap.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory (default: <input>_cleaned next to file)",
    )
    ap.add_argument(
        "--inplace",
        action="store_true",
        help="Replace input (writes .bak backup first)",
    )
    ap.add_argument(
        "--max-field-bytes",
        type=int,
        default=10_485_760,
        help="Max UTF-8 bytes per checked column (default 10MB)",
    )
    ap.add_argument(
        "--max-field-chars",
        type=int,
        default=0,
        help="If >0, use char count instead of bytes with this limit",
    )
    ap.add_argument(
        "--only-txt4",
        action="store_true",
        help="Only check txt4 column (column index 4, name txt4)",
    )
    ap.add_argument(
        "--column",
        type=int,
        action="append",
        dest="columns",
        help="0-based column index to check (repeatable); default all columns",
    )
    args = ap.parse_args()

    if args.max_field_chars > 0:
        max_len = args.max_field_chars
        len_mode = "chars"
    else:
        max_len = args.max_field_bytes
        len_mode = "bytes"

    paths: list[Path] = []
    for pattern in args.input:
        p = Path(pattern)
        if "*" in pattern or "?" in pattern:
            paths.extend(sorted(Path().glob(pattern)))
        elif p.is_dir():
            paths.extend(sorted(p.glob("*.csv")))
        elif p.is_file():
            paths.append(p)
        else:
            paths.extend(sorted(Path(p.parent).glob(p.name)))

    if not paths:
        print("ERROR: no input files matched", file=sys.stderr)
        return 1

    check_cols = args.columns
    if args.only_txt4:
        check_cols = [4]

    total_kept = 0
    total_dropped = 0
    t0 = time.time()

    for src in paths:
        if args.inplace:
            dst = src.with_suffix(src.suffix + ".tmp")
            bak = src.with_suffix(src.suffix + ".bak")
        elif args.output_dir:
            dst = args.output_dir / src.name
        else:
            dst = src.parent / f"{src.stem}_cleaned{src.suffix}"

        print(f"Processing {src} -> {dst} (max {max_len} {len_mode})", flush=True)
        kept, dropped, max_seen = process_file(src, dst, max_len, len_mode, check_cols)
        total_kept += kept
        total_dropped += dropped
        print(f"  done kept={kept} dropped={dropped} max_field_{len_mode}={max_seen}", flush=True)

        if args.inplace:
            if bak.exists():
                bak.unlink()
            src.rename(bak)
            dst.rename(src)
            print(f"  replaced {src} (backup {bak})", flush=True)

    elapsed = time.time() - t0
    print(
        f"All files: kept={total_kept} dropped={total_dropped} elapsed={elapsed:.1f}s",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
