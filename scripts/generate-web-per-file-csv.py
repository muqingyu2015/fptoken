#!/usr/bin/env python3
"""One CSV per jsonl under sample-data/web, txt4 split at 1024 chars."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WEB = ROOT / "sample-data" / "web"
DEFAULT_OUT = ROOT / "sample-data" / "web-csv-by-file"

MAX_TXT4_LEN = 1024
CONTENT_KEYS = ("Content", "content", "text", "body", "Body", "TITLE", "title")


def java_string_hashcode(text: str) -> int:
    h = 0
    for ch in text:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    if h >= 0x80000000:
        h -= 0x100000000
    return h


def sanitize_field(value: str) -> str:
    return value.replace("\x00", "") if value else value


def split_txt4(text: str, max_len: int) -> list[str]:
    text = sanitize_field(text)
    if not text:
        return []
    if len(text) <= max_len:
        return [text]
    return [text[i : i + max_len] for i in range(0, len(text), max_len)]


def pick_txt4(obj: dict, raw_line: str) -> str:
    for key in CONTENT_KEYS:
        val = obj.get(key)
        if isinstance(val, str) and val.strip():
            return sanitize_field(val.strip())
    return sanitize_field(raw_line.strip())


def convert_jsonl(jsonl_path: Path, csv_path: Path, max_len: int) -> dict:
    file_name = jsonl_path.name
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    in_records = 0
    out_rows = 0
    split_records = 0

    with jsonl_path.open(encoding="utf-8", errors="replace") as fin, csv_path.open(
        "w", encoding="utf-8", newline=""
    ) as fout:
        writer = csv.writer(fout, lineterminator="\n")
        writer.writerow(["hashstr", "txt1", "txt2", "txt3", "txt4"])

        for raw in fin:
            line = raw.rstrip("\n\r")
            if not line.strip():
                continue
            in_records += 1
            try:
                obj = json.loads(line)
                txt4 = pick_txt4(obj, line) if isinstance(obj, dict) else sanitize_field(line)
            except json.JSONDecodeError:
                txt4 = sanitize_field(line.strip())
            if not txt4:
                continue
            chunks = split_txt4(txt4, max_len)
            if len(chunks) > 1:
                split_records += 1
            for chunk in chunks:
                writer.writerow(
                    [str(java_string_hashcode(chunk)), file_name, file_name, file_name, chunk]
                )
                out_rows += 1

    return {
        "jsonl": str(jsonl_path),
        "input_records": in_records,
        "output_rows": out_rows,
        "split_records": split_records,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--web-dir", type=Path, default=DEFAULT_WEB)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--max-len", type=int, default=MAX_TXT4_LEN)
    args = ap.parse_args()

    if not args.web_dir.exists():
        print(f"ERROR: not found {args.web_dir}", file=sys.stderr)
        return 1

    jsonl_files = sorted(args.web_dir.rglob("*.jsonl"))
    if not jsonl_files:
        print(f"ERROR: no jsonl under {args.web_dir}", file=sys.stderr)
        return 1

    args.out_dir.mkdir(parents=True, exist_ok=True)
    manifest_files: list[dict] = []
    total_in = total_out = 0
    t0 = time.time()

    print(f"Converting {len(jsonl_files)} jsonl -> csv (max txt4={args.max_len})", flush=True)

    for i, jsonl_path in enumerate(jsonl_files, 1):
        rel = jsonl_path.relative_to(args.web_dir)
        csv_path = args.out_dir / rel.with_suffix(".csv")
        print(f"[{i}/{len(jsonl_files)}] {rel}", flush=True)
        t1 = time.time()
        stat = convert_jsonl(jsonl_path, csv_path, args.max_len)
        stat["elapsed_sec"] = round(time.time() - t1, 1)
        stat["output"] = str(csv_path.relative_to(args.out_dir))
        manifest_files.append(stat)
        total_in += stat["input_records"]
        total_out += stat["output_rows"]
        print(
            f"  records={stat['input_records']} rows={stat['output_rows']} "
            f"split={stat['split_records']} ({stat['elapsed_sec']}s)",
            flush=True,
        )

    manifest = {
        "max_txt4_len": args.max_len,
        "web_dir": str(args.web_dir),
        "out_dir": str(args.out_dir),
        "file_count": len(jsonl_files),
        "total_input_records": total_in,
        "total_output_rows": total_out,
        "elapsed_sec": round(time.time() - t0, 1),
        "files": manifest_files,
    }
    (args.out_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    copy_lines = [
        f"-- {len(jsonl_files)} CSV files, total_rows={total_out}, max_txt4_len={args.max_len}\n\n"
    ]
    for stat in manifest_files:
        p = (args.out_dir / stat["output"]).as_posix()
        copy_lines.append(
            f"\\copy fulltext(hashstr, txt1, txt2, txt3, txt4) FROM '{p}' "
            "WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');\n"
        )
    (args.out_dir / "fulltext_copy_all.sql").write_text("".join(copy_lines), encoding="utf-8")

    print(f"Done. {len(jsonl_files)} csv under {args.out_dir}, rows={total_out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
