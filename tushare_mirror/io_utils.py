from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Iterable, Mapping, Any

import pyarrow as pa


def now_utc() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_jsonl_zst(path: Path, events: Iterable[Mapping[str, Any]]) -> int:
    ensure_dir(path.parent)
    count = 0
    with pa.output_stream(str(path)) as sink:
        with pa.CompressedOutputStream(sink, "zstd") as out:
            for event in events:
                line = json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
                out.write(line.encode("utf-8"))
                count += 1
    return count


def read_jsonl_zst(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    with pa.input_stream(str(path)) as source:
        with pa.CompressedInputStream(source, "zstd") as stream:
            data = stream.read().decode("utf-8")
    for line in data.splitlines():
        if line.strip():
            events.append(json.loads(line))
    return events


def atomic_move(src: Path, dst: Path) -> None:
    ensure_dir(dst.parent)
    os.replace(src, dst)


def move_tree_to_quarantine(src_dir: Path, quarantine_dir: Path) -> None:
    ensure_dir(quarantine_dir.parent)
    if quarantine_dir.exists():
        shutil.rmtree(quarantine_dir)
    if src_dir.exists():
        shutil.move(str(src_dir), str(quarantine_dir))
