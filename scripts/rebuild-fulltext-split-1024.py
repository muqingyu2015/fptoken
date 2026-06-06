#!/usr/bin/env python3
"""
Rebuild fulltext CSV: split txt4 into multiple rows when length > 1024 (chars).

Corpus:
  py -3 scripts/rebuild-fulltext-split-1024.py corpus

Web (from jsonl):
  py -3 scripts/rebuild-fulltext-split-1024.py web
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import time
from pathlib import Path
from typing import Iterator, TextIO


class _NulFilterReader(io.RawIOBase):
    """Binary reader that strips NUL so csv.reader can parse source files."""

    def __init__(self, raw: io.BufferedReader) -> None:
        self._raw = raw

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        data = self._raw.read(-1 if size < 0 else size)
        return data.replace(b"\x00", b"") if data else b""

ROOT = Path(__file__).resolve().parents[1]
CORPUS_IN = ROOT / "sample-data" / "corpus"
CORPUS_OUT = ROOT / "sample-data" / "corpus-split"
WEB_IN = ROOT / "sample-data" / "web"
WEB_OUT = ROOT / "sample-data" / "web-corpus-split"

MAX_TXT4_LEN = 1024
CONTENT_KEYS = ("Content", "content", "text", "body", "Body", "TITLE", "title")


def raise_csv_field_limit() -> None:
    """Source corpus may have multi-MB/GB txt4 fields before split."""
    limit = sys.maxsize
    while limit > 0:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10


def java_string_hashcode(text: str) -> int:
    h = 0
    for ch in text:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    if h >= 0x80000000:
        h -= 0x100000000
    return h


def sanitize_field(value: str) -> str:
    """PostgreSQL COPY rejects NUL in text fields."""
    if not value:
        return value
    return value.replace("\x00", "")


def split_txt4(text: str, max_len: int = MAX_TXT4_LEN) -> list[str]:
    text = sanitize_field(text)
    if not text:
        return []
    if len(text) <= max_len:
        return [text]
    return [text[i : i + max_len] for i in range(0, len(text), max_len)]


def expand_row(row: list[str], max_len: int) -> Iterator[list[str]]:
    if len(row) < 5:
        return
    txt1, txt2, txt3 = row[1], row[2], row[3]
    txt4 = row[4]
    chunks = split_txt4(txt4, max_len)
    if not chunks:
        return
    for chunk in chunks:
        yield [str(java_string_hashcode(chunk)), txt1, txt2, txt3, chunk]


def transform_csv(src: Path, dst: Path, max_len: int) -> tuple[int, int, int]:
    """Returns (input_rows, output_rows, split_source_rows)."""
    raise_csv_field_limit()
    dst.parent.mkdir(parents=True, exist_ok=True)
    in_rows = 0
    out_rows = 0
    split_sources = 0

    with src.open("rb") as fbin, dst.open("w", encoding="utf-8", newline="") as fout:
        text_in = io.TextIOWrapper(
            _NulFilterReader(fbin), encoding="utf-8", errors="replace", newline=""
        )
        reader = csv.reader(text_in)
        writer = csv.writer(fout, lineterminator="\n")
        header = next(reader, None)
        if header:
            writer.writerow(header)
        for row in reader:
            in_rows += 1
            row = [sanitize_field(c) for c in row]
            if len(row) >= 5 and len(row[4]) > max_len:
                split_sources += 1
                expanded = list(expand_row(row, max_len))
                for out_row in expanded:
                    writer.writerow(out_row)
                    out_rows += 1
            elif len(row) >= 5:
                writer.writerow(row)
                out_rows += 1
            if in_rows % 500_000 == 0:
                print(f"  {src.name}: in={in_rows} out={out_rows} split_src={split_sources}", flush=True)
    return in_rows, out_rows, split_sources


def pick_txt4(obj: dict, raw_line: str) -> str:
    for key in CONTENT_KEYS:
        val = obj.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return raw_line.strip()


def iter_web_rows(web_root: Path, max_len: int) -> Iterator[list[str]]:
    for path in sorted(web_root.rglob("*.jsonl")):
        file_name = path.name
        print(f"Reading {path.relative_to(web_root)}", flush=True)
        with path.open(encoding="utf-8", errors="replace") as fh:
            for raw in fh:
                line = raw.rstrip("\n\r")
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                    txt4 = pick_txt4(obj, line) if isinstance(obj, dict) else line
                except json.JSONDecodeError:
                    txt4 = sanitize_field(line.strip())
                else:
                    txt4 = sanitize_field(txt4)
                if not txt4:
                    continue
                for chunk in split_txt4(txt4, max_len):
                    yield [
                        str(java_string_hashcode(chunk)),
                        file_name,
                        file_name,
                        file_name,
                        chunk,
                    ]


def write_web_csv(web_root: Path, out_path: Path, max_len: int) -> tuple[int, int]:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    with out_path.open("w", encoding="utf-8", newline="") as fout:
        writer = csv.writer(fout, lineterminator="\n")
        writer.writerow(["hashstr", "txt1", "txt2", "txt3", "txt4"])
        for row in iter_web_rows(web_root, max_len):
            writer.writerow(row)
            rows += 1
            if rows % 500_000 == 0:
                print(f"  web out_rows={rows}", flush=True)
    return rows, 0


def write_copy_sql(out_dir: Path, csv_names: list[str], total_rows: int) -> None:
    lines = [f"-- total_rows: {total_rows}\n", f"-- max txt4 length: {MAX_TXT4_LEN}\n\n"]
    for name in csv_names:
        p = (out_dir / name).as_posix()
        lines.append(
            f"\\copy fulltext(hashstr, txt1, txt2, txt3, txt4) FROM '{p}' "
            "WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');\n"
        )
    (out_dir / "fulltext_copy_all.sql").write_text("".join(lines), encoding="utf-8")


def cmd_corpus(args: argparse.Namespace) -> int:
    CORPUS_OUT.mkdir(parents=True, exist_ok=True)
    sources = args.files or sorted(CORPUS_IN.glob("fulltext_corpus_*.csv"))
    if not sources:
        print(f"ERROR: no CSV under {CORPUS_IN}", file=sys.stderr)
        return 1

    manifest: dict = {"max_txt4_len": args.max_len, "files": []}
    total_in = total_out = 0

    for src in sources:
        src = Path(src)
        dst = CORPUS_OUT / src.name
        print(f"Corpus {src} -> {dst}", flush=True)
        t0 = time.time()
        in_rows, out_rows, split_src = transform_csv(src, dst, args.max_len)
        elapsed = time.time() - t0
        total_in += in_rows
        total_out += out_rows
        manifest["files"].append(
            {
                "source": str(src),
                "output": dst.name,
                "input_rows": in_rows,
                "output_rows": out_rows,
                "split_source_rows": split_src,
                "elapsed_sec": round(elapsed, 1),
            }
        )
        print(f"  done in={in_rows} out={out_rows} split_src={split_src} ({elapsed:.1f}s)", flush=True)

    manifest["total_input_rows"] = total_in
    manifest["total_output_rows"] = total_out
    (CORPUS_OUT / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    write_copy_sql(CORPUS_OUT, [f["output"] for f in manifest["files"]], total_out)
    print(f"Corpus finished: in={total_in} out={total_out} -> {CORPUS_OUT}", flush=True)
    return 0


def cmd_web(args: argparse.Namespace) -> int:
    web_root = Path(args.web_dir)
    out_path = WEB_OUT / "fulltext_web_0001.csv"
    print(f"Web {web_root} -> {out_path}", flush=True)
    t0 = time.time()
    out_rows, _ = write_web_csv(web_root, out_path, args.max_len)
    elapsed = time.time() - t0
    manifest = {
        "max_txt4_len": args.max_len,
        "output_rows": out_rows,
        "elapsed_sec": round(elapsed, 1),
    }
    (WEB_OUT / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    write_copy_sql(WEB_OUT, [out_path.name], out_rows)
    print(f"Web finished: out_rows={out_rows} ({elapsed:.1f}s) -> {WEB_OUT}", flush=True)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["corpus", "web", "all"])
    ap.add_argument("--max-len", type=int, default=MAX_TXT4_LEN)
    ap.add_argument("--files", nargs="*", help="Corpus CSV paths (default: corpus/fulltext_corpus_*.csv)")
    ap.add_argument("--web-dir", type=Path, default=WEB_IN)
    args = ap.parse_args()

    if args.mode in ("corpus", "all"):
        if cmd_corpus(args) != 0:
            return 1
    if args.mode in ("web", "all"):
        if cmd_web(args) != 0:
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
