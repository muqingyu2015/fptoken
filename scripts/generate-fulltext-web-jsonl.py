#!/usr/bin/env python3
"""Convert sample-data/web/**/*.jsonl to fulltext CSV shards (100M rows each)."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path
from typing import Iterator, TextIO

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WEB = ROOT / "sample-data" / "web"
DEFAULT_OUT = ROOT / "sample-data" / "web-corpus"

CONTENT_KEYS = ("Content", "content", "text", "body", "Body", "TITLE", "title")


def java_string_hashcode(text: str) -> int:
    h = 0
    for ch in text:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    if h >= 0x80000000:
        h -= 0x100000000
    return h


def pick_txt4(obj: dict, raw_line: str) -> str:
    for key in CONTENT_KEYS:
        val = obj.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return raw_line.strip()


def iter_web_jsonl(web_root: Path) -> Iterator[tuple[str, str, str, str, str]]:
    paths = sorted(web_root.rglob("*.jsonl"))
    print(f"Found {len(paths)} jsonl files under {web_root}", flush=True)
    for path in paths:
        file_name = path.name
        print(f"Reading {path.relative_to(web_root)}", flush=True)
        with path.open(encoding="utf-8", errors="replace") as fh:
            for raw in fh:
                line = raw.rstrip("\n\r")
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        txt4 = pick_txt4(obj, line)
                    else:
                        txt4 = line
                except json.JSONDecodeError:
                    txt4 = line.strip()
                if not txt4:
                    continue
                h = str(java_string_hashcode(txt4))
                yield h, file_name, file_name, file_name, txt4


class ShardWriter:
    def __init__(self, out_dir: Path, rows_per_file: int) -> None:
        self.out_dir = out_dir
        self.rows_per_file = rows_per_file
        self.manifest_path = out_dir / "web_corpus_manifest.json"
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.part = 1
        self.rows_in_part = 0
        self.total_rows = 0
        self._fh: TextIO | None = None
        self._writer: csv.writer | None = None
        self._load_manifest()

    def _load_manifest(self) -> None:
        if not self.manifest_path.exists():
            return
        data = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.part = int(data.get("current_part", 1))
        self.rows_in_part = int(data.get("rows_in_current_part", 0))
        self.total_rows = int(data.get("total_rows", 0))
        if self.rows_in_part >= self.rows_per_file:
            self.part += 1
            self.rows_in_part = 0

    def _part_path(self, part: int) -> Path:
        return self.out_dir / f"fulltext_web_{part:04d}.csv"

    def _open_part(self) -> None:
        if self._fh:
            self._fh.close()
        path = self._part_path(self.part)
        new_file = not path.exists() or path.stat().st_size == 0
        self._fh = path.open("a", encoding="utf-8", newline="")
        self._writer = csv.writer(self._fh, lineterminator="\n")
        if new_file:
            self._writer.writerow(["hashstr", "txt1", "txt2", "txt3", "txt4"])

    def write_row(self, row: tuple[str, str, str, str, str]) -> None:
        if self._writer is None:
            self._open_part()
        assert self._writer is not None
        self._writer.writerow(row)
        self.rows_in_part += 1
        self.total_rows += 1
        if self.rows_in_part >= self.rows_per_file:
            self._close_part()
            self.part += 1
            self.rows_in_part = 0
            self._writer = None
        if self.total_rows % 500_000 == 0:
            self._save_manifest()
            print(f"  progress part={self.part} in_part={self.rows_in_part} total={self.total_rows}", flush=True)

    def _close_part(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None
        print(f"Finished shard {self.part}: {self._part_path(self.part)}", flush=True)

    def _save_manifest(self) -> None:
        self.manifest_path.write_text(
            json.dumps(
                {
                    "rows_per_file": self.rows_per_file,
                    "current_part": self.part,
                    "rows_in_current_part": self.rows_in_part,
                    "total_rows": self.total_rows,
                    "updated": time.strftime("%Y-%m-%dT%H:%M:%S"),
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    def close(self) -> None:
        if self._fh:
            self._fh.close()
        self._save_manifest()
        copy_path = self.out_dir / "fulltext_web_copy_all.sql"
        lines = [f"-- total_rows: {self.total_rows}\n\n"]
        for i in range(1, self.part + 1):
            p = self._part_path(i)
            if p.exists():
                lines.append(
                    f"\\copy fulltext(hashstr, txt1, txt2, txt3, txt4) FROM '{p.as_posix()}' "
                    "WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');\n"
                )
        copy_path.write_text("".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--web-dir", type=Path, default=DEFAULT_WEB)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--rows-per-file", type=int, default=100_000_000)
    args = ap.parse_args()

    if not args.web_dir.exists():
        print(f"ERROR: web dir not found: {args.web_dir}", flush=True)
        return 1

    w = ShardWriter(args.out_dir, args.rows_per_file)
    for row in iter_web_jsonl(args.web_dir):
        w.write_row(row)
    w.close()
    print(f"Done. total_rows={w.total_rows} shards={w.part}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
