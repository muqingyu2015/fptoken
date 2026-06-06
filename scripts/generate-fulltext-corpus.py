#!/usr/bin/env python3
"""
Build fulltext CSV corpus: hashstr, txt1, txt2, txt3, txt4.
- txt1/txt2/txt3 = file name only; txt4 = original line
- Rotates CSV every --rows-per-file (default 100_000_000)
- Stops after --min-parts full shards (default 10); resumable via manifest
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from typing import Iterator, TextIO
from urllib.request import urlretrieve

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "sample-data" / "corpus"
DEFAULT_REPOS = ROOT / "sample-data" / "github-repos-seed.txt"
DEFAULT_CLONE_DIR = ROOT / "sample-data" / "github-clones"
S3_CACHE = ROOT / "sample-data" / "codesearchnet-s3-cache"

SKIP_DIRS = {
    ".git", ".cursor", "node_modules", "build", "bin", "bin-test",
    "target", "dist", ".gradle", "__pycache__", "corpus",
    "codesearchnet-s3-cache",
}

TEXT_EXTENSIONS = {
    ".java", ".js", ".jsx", ".ts", ".tsx", ".css", ".scss", ".less",
    ".html", ".htm", ".md", ".txt", ".json", ".xml", ".vue", ".py",
    ".sql", ".yml", ".yaml", ".properties", ".sh", ".bat", ".ps1",
    ".gradle", ".kt", ".cpp", ".c", ".h", ".hpp", ".go", ".rs",
    ".rb", ".php", ".ini", ".conf", ".cfg", ".toml", ".svg",
}

STACK_LANG_DIRS = [
    "data/java", "data/javascript", "data/python", "data/go",
    "data/typescript", "data/cpp", "data/c", "data/c-sharp",
    "data/php", "data/ruby", "data/rust", "data/kotlin",
    "data/scala", "data/swift", "data/sql",
]

CODESEARCHNET_LANGS = ["java", "python", "javascript", "go", "php", "ruby"]

S3_CODESEARCHNET = [
    ("java", "https://s3.amazonaws.com/code-search-net/CodeSearchNet/v1/java.zip"),
    ("python", "https://s3.amazonaws.com/code-search-net/CodeSearchNet/v1/python.zip"),
    ("javascript", "https://s3.amazonaws.com/code-search-net/CodeSearchNet/v1/javascript.zip"),
    ("go", "https://s3.amazonaws.com/code-search-net/CodeSearchNet/v1/go.zip"),
    ("php", "https://s3.amazonaws.com/code-search-net/CodeSearchNet/v1/php.zip"),
    ("ruby", "https://s3.amazonaws.com/code-search-net/CodeSearchNet/v1/ruby.zip"),
]


def java_string_hashcode(text: str) -> int:
    h = 0
    for ch in text:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    if h >= 0x80000000:
        h -= 0x100000000
    return h


def should_skip(path: Path, scan_root: Path | None = None) -> bool:
    if any(part in SKIP_DIRS for part in path.parts):
        return True
    if scan_root is not None and scan_root.resolve() == ROOT.resolve():
        if "github-clones" in path.parts:
            return True
    return False


def load_repos(path: Path) -> list[str]:
    repos: list[str] = []
    if not path.exists():
        return repos
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        repos.append(line.split("#")[0].strip())
    return repos


def git_shallow_clone(repo: str, clone_root: Path) -> Path | None:
    dest = clone_root / repo.replace("/", "_")
    if dest.exists() and any(dest.iterdir()):
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://github.com/{repo}.git"
    print(f"Cloning {repo} -> {dest}", flush=True)
    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", "--single-branch", url, str(dest)],
            check=True,
            capture_output=True,
            timeout=7200,
        )
        return dest
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        print(f"WARN clone failed {repo}: {e}", flush=True)
        return dest if dest.exists() and any(dest.iterdir()) else None


def iter_local_files(roots: list[Path]) -> Iterator[tuple[str, Path]]:
    for root in roots:
        if not root.exists():
            print(f"WARN missing root: {root}", flush=True)
            continue
        for path in sorted(root.rglob("*")):
            if not path.is_file() or should_skip(path, root):
                continue
            if path.suffix.lower() not in TEXT_EXTENSIONS:
                continue
            yield path.name, path


def iter_file_lines(file_name: str, path: Path) -> Iterator[tuple[str, str, str, str, str]]:
    try:
        fh = path.open(encoding="utf-8", errors="replace")
    except OSError:
        return
    with fh:
        for raw in fh:
            txt4 = raw.rstrip("\n\r")
            if not txt4.strip():
                continue
            h = str(java_string_hashcode(txt4))
            yield h, file_name, file_name, file_name, txt4


def iter_codesearchnet_hf() -> Iterator[tuple[str, str, str, str, str]]:
    try:
        from datasets import load_dataset
    except ImportError:
        print("ERROR: pip install datasets", flush=True)
        return
    for lang in CODESEARCHNET_LANGS:
        print(f"CodeSearchNet HF: {lang}", flush=True)
        try:
            ds = load_dataset("code_search_net", lang, split="train", streaming=True)
        except Exception as e:
            print(f"WARN HF {lang}: {e}", flush=True)
            continue
        for sample in ds:
            path = sample.get("func_path_in_repository") or f"code.{lang}"
            file_name = Path(path).name or f"code.{lang}"
            for key in ("func_code_string", "whole_func_string"):
                for raw in (sample.get(key) or "").splitlines():
                    txt4 = raw.rstrip("\n\r")
                    if txt4.strip():
                        yield str(java_string_hashcode(txt4)), file_name, file_name, file_name, txt4


def ensure_s3_zip(lang: str, url: str, cache: Path) -> Path | None:
    cache.mkdir(parents=True, exist_ok=True)
    zpath = cache / f"{lang}.zip"
    if not zpath.exists() or zpath.stat().st_size < 1000:
        print(f"Downloading S3 CodeSearchNet {lang}...", flush=True)
        try:
            urlretrieve(url, zpath)
        except Exception as e:
            print(f"WARN S3 download {lang}: {e}", flush=True)
            return None
    return zpath


def iter_codesearchnet_s3(cache: Path) -> Iterator[tuple[str, str, str, str, str]]:
    import json as json_mod

    for lang, url in S3_CODESEARCHNET:
        zpath = ensure_s3_zip(lang, url, cache)
        if not zpath:
            continue
        print(f"CodeSearchNet S3 zip: {lang}", flush=True)
        with zipfile.ZipFile(zpath, "r") as zf:
            for name in zf.namelist():
                if not name.endswith(".jsonl") and not name.endswith(".jsonl.gz"):
                    continue
                raw_bytes = zf.read(name)
                if name.endswith(".gz"):
                    import gzip

                    raw_bytes = gzip.decompress(raw_bytes)
                for line in raw_bytes.decode("utf-8", errors="replace").splitlines():
                    if not line.strip():
                        continue
                    try:
                        obj = json_mod.loads(line)
                    except json_mod.JSONDecodeError:
                        continue
                    path = obj.get("path") or f"code.{lang}"
                    file_name = Path(path).name or f"code.{lang}"
                    for key in ("func_code_string", "whole_func_string", "code", "original_string"):
                        text = obj.get(key)
                        if not text:
                            continue
                        for raw in str(text).splitlines():
                            txt4 = raw.rstrip("\n\r")
                            if txt4.strip():
                                yield str(java_string_hashcode(txt4)), file_name, file_name, file_name, txt4


def iter_stack_lines(langs: list[str]) -> Iterator[tuple[str, str, str, str, str]]:
    try:
        from datasets import load_dataset
    except ImportError:
        print("ERROR: pip install datasets huggingface_hub", flush=True)
        return
    for lang_dir in langs:
        print(f"The Stack: {lang_dir}", flush=True)
        try:
            ds = load_dataset("bigcode/the-stack", data_dir=lang_dir, split="train", streaming=True)
        except Exception as e:
            print(f"WARN stack {lang_dir}: {e}", flush=True)
            continue
        ext = ".txt"
        if "java" in lang_dir:
            ext = ".java"
        elif "javascript" in lang_dir:
            ext = ".js"
        elif "python" in lang_dir:
            ext = ".py"
        for sample in ds:
            content = sample.get("content") or ""
            if (sample.get("max_line_length") or 0) > 50_000:
                continue
            file_name = Path(sample.get("max_stars_repo_path") or sample.get("path") or f"stack{ext}").name
            if not file_name or file_name == ".":
                file_name = f"stack{ext}"
            for raw in content.splitlines():
                txt4 = raw.rstrip("\n\r")
                if txt4.strip():
                    yield str(java_string_hashcode(txt4)), file_name, file_name, file_name, txt4


class CorpusWriter:
    def __init__(self, out_dir: Path, rows_per_file: int, min_parts: int) -> None:
        self.out_dir = out_dir
        self.rows_per_file = rows_per_file
        self.min_parts = min_parts
        self.manifest_path = out_dir / "fulltext_corpus_manifest.json"
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.part = 1
        self.rows_in_part = 0
        self.total_rows = 0
        self.files_written: list[dict] = []
        self.completed_phases: list[str] = []
        self.cloned_repos: list[str] = []
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
        self.files_written = list(data.get("files", []))
        self.completed_phases = list(data.get("completed_phases", []))
        self.cloned_repos = list(data.get("cloned_repos", []))
        if self.rows_in_part >= self.rows_per_file:
            self.part += 1
            self.rows_in_part = 0
        if not self.completed_phases and self.total_rows >= 50_000_000:
            self.completed_phases = ["local", "codesearchnet_hf"]
        elif not self.completed_phases and self.total_rows >= 5_000_000:
            self.completed_phases = ["local"]

    def mark_phase(self, name: str) -> None:
        if name not in self.completed_phases:
            self.completed_phases.append(name)
        self._save_manifest()

    def phase_done(self, name: str) -> bool:
        return name in self.completed_phases

    def _part_path(self, part: int) -> Path:
        return self.out_dir / f"fulltext_corpus_{part:04d}.csv"

    def _open_part(self) -> None:
        if self._fh:
            self._fh.close()
        path = self._part_path(self.part)
        new_file = not path.exists() or path.stat().st_size == 0
        self._fh = path.open("a", encoding="utf-8", newline="")
        self._writer = csv.writer(self._fh, lineterminator="\n")
        if new_file:
            self._writer.writerow(["hashstr", "txt1", "txt2", "txt3", "txt4"])
            self.files_written.append(
                {"part": self.part, "path": path.name, "rows": 0, "started": time.strftime("%Y-%m-%dT%H:%M:%S")}
            )

    def write_row(self, row: tuple[str, str, str, str, str]) -> bool:
        if self._writer is None:
            if self.part > self.min_parts:
                return False
            self._open_part()
        assert self._writer is not None
        self._writer.writerow(row)
        self.rows_in_part += 1
        self.total_rows += 1
        if self.rows_in_part >= self.rows_per_file:
            finished = self.part
            self._finalize_part()
            if finished >= self.min_parts:
                return False
            self.part += 1
            self.rows_in_part = 0
            self._writer = None
        if self.total_rows % 500_000 == 0:
            self._save_manifest()
            print(f"  progress part={self.part} in_part={self.rows_in_part} total={self.total_rows}", flush=True)
        return True

    def _finalize_part(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None
        if self.files_written:
            self.files_written[-1]["rows"] = self.rows_in_part
            self.files_written[-1]["finished"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        self._save_manifest()
        print(f"Finished shard {self.part}: {self._part_path(self.part)}", flush=True)

    def _save_manifest(self) -> None:
        if self.files_written:
            self.files_written[-1]["rows"] = self.rows_in_part
        self.manifest_path.write_text(
            json.dumps(
                {
                    "rows_per_file": self.rows_per_file,
                    "min_parts": self.min_parts,
                    "current_part": self.part,
                    "rows_in_current_part": self.rows_in_part,
                    "total_rows": self.total_rows,
                    "completed_phases": self.completed_phases,
                    "cloned_repos": self.cloned_repos,
                    "files": self.files_written,
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
        copy_path = self.out_dir / "fulltext_corpus_copy_all.sql"
        lines = [f"-- total_rows: {self.total_rows}\n\n"]
        for i in range(1, self.min_parts + 1):
            p = self._part_path(i)
            if p.exists():
                lines.append(
                    f"\\copy fulltext(hashstr, txt1, txt2, txt3, txt4) FROM '{p.as_posix()}' "
                    "WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');\n"
                )
        copy_path.write_text("".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--rows-per-file", type=int, default=100_000_000)
    ap.add_argument("--min-parts", type=int, default=10)
    ap.add_argument("--local", nargs="*", type=Path, default=None)
    ap.add_argument("--clone-repos", action="store_true")
    ap.add_argument("--repos-file", type=Path, default=DEFAULT_REPOS)
    ap.add_argument("--clone-dir", type=Path, default=DEFAULT_CLONE_DIR)
    ap.add_argument("--codesearchnet", action="store_true")
    ap.add_argument("--s3-codesearchnet", action="store_true")
    ap.add_argument("--stack", action="store_true")
    ap.add_argument("--stack-lang", nargs="*", default=None)
    ap.add_argument("--max-rows", type=int, default=0)
    ap.add_argument("--reset-phases", action="store_true", help="Ignore completed_phases (keeps CSV)")
    ap.add_argument("--rescan-clones", action="store_true", help="Re-scan github-clones only (fix skip bug)")
    args = ap.parse_args()

    local_roots = list(args.local) if args.local else [ROOT, Path(r"D:\开源项目源码")]
    w = CorpusWriter(args.out_dir, args.rows_per_file, args.min_parts)
    if args.reset_phases:
        w.completed_phases = []

    def feed_rows(it: Iterator[tuple[str, str, str, str, str]]) -> bool:
        for row in it:
            if not w.write_row(row):
                return False
            if args.max_rows and w.total_rows >= args.max_rows:
                return False
        return True

    def feed_files(it: Iterator[tuple[str, Path]]) -> bool:
        for fn, p in it:
            if not feed_rows(iter_file_lines(fn, p)):
                return False
        return True

    print(f"Output: {args.out_dir} | resume total_rows={w.total_rows} phases={w.completed_phases}", flush=True)

    if not w.phase_done("local"):
        print("Phase: local", flush=True)
        if not feed_files(iter_local_files(local_roots)):
            w.close()
            return 0
        w.mark_phase("local")

    if args.codesearchnet and not w.phase_done("codesearchnet_hf"):
        print("Phase: codesearchnet_hf", flush=True)
        if not feed_rows(iter_codesearchnet_hf()):
            w.close()
            return 0
        w.mark_phase("codesearchnet_hf")

    if args.s3_codesearchnet and not w.phase_done("codesearchnet_s3"):
        print("Phase: codesearchnet_s3", flush=True)
        if not feed_rows(iter_codesearchnet_s3(S3_CACHE)):
            w.close()
            return 0
        w.mark_phase("codesearchnet_s3")

    if args.rescan_clones:
        w.completed_phases = [p for p in w.completed_phases if p != "clone_repos"]
        print("Phase: rescan github-clones only", flush=True)
        if args.clone_dir.exists():
            for dest in sorted(args.clone_dir.iterdir()):
                if dest.is_dir() and any(dest.iterdir()):
                    print(f"Rescan: {dest.name}", flush=True)
                    if not feed_files(iter_local_files([dest])):
                        w.close()
                        return 0
        w.mark_phase("clone_repos")

    if args.clone_repos and not args.rescan_clones:
        print("Phase: github clones", flush=True)
        scanned_dirs: set[str] = set()
        if args.clone_dir.exists():
            for dest in sorted(args.clone_dir.iterdir()):
                if dest.is_dir() and any(dest.iterdir()):
                    key = dest.name
                    if key in scanned_dirs:
                        continue
                    scanned_dirs.add(key)
                    print(f"Scan existing clone dir: {dest.name}", flush=True)
                    if not feed_files(iter_local_files([dest])):
                        w.close()
                        return 0
        for repo in load_repos(args.repos_file):
            if repo in w.cloned_repos:
                dest = args.clone_dir / repo.replace("/", "_")
            else:
                dest = git_shallow_clone(repo, args.clone_dir)
                if dest:
                    w.cloned_repos.append(repo)
                    w._save_manifest()
            if dest and dest.exists():
                print(f"Scan clone: {repo}", flush=True)
                if not feed_files(iter_local_files([dest])):
                    w.close()
                    return 0
        w.mark_phase("clone_repos")

    if args.stack and not w.phase_done("stack"):
        print("Phase: the-stack (needs huggingface-cli login)", flush=True)
        langs = args.stack_lang or STACK_LANG_DIRS
        if not feed_rows(iter_stack_lines(langs)):
            w.close()
            return 0
        w.mark_phase("stack")

    w.close()
    full_shards = sum(1 for f in w.files_written if f.get("rows", 0) >= args.rows_per_file)
    print(f"Done total_rows={w.total_rows} full_shards={full_shards}/{args.min_parts}", flush=True)
    if full_shards < args.min_parts:
        print(
            "WARN: Need more data. Run: huggingface-cli login, then --stack\n"
            "  Or keep --clone-repos running (linux/vscode/flutter are huge).",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
