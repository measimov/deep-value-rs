from __future__ import annotations

import shutil
from importlib import resources
from pathlib import Path
from typing import Any

import yaml

from .capabilities import normalize_endpoint_capability
from .hashing import partition_spec_id, table_id


def bundled_endpoint_files() -> list[Path]:
    root = resources.files("tushare_mirror.endpoint_configs")
    out: list[Path] = []
    for item in root.iterdir():
        if item.name.endswith(('.yaml', '.yml')):
            with resources.as_file(item) as path:
                out.append(Path(path))
    return out


def ensure_endpoint_files(root: Path) -> None:
    dst = root / "_catalog" / "endpoints"
    dst.mkdir(parents=True, exist_ok=True)
    for src in bundled_endpoint_files():
        target = dst / src.name
        if not target.exists():
            shutil.copyfile(src, target)


def load_endpoint_configs(root: Path) -> list[dict[str, Any]]:
    endpoint_dir = root / "_catalog" / "endpoints"
    if not endpoint_dir.exists():
        ensure_endpoint_files(root)
    configs: list[dict[str, Any]] = []
    for path in sorted(endpoint_dir.glob("*.y*ml")):
        data = yaml.safe_load(path.read_text()) or {}
        for cfg in data.get("endpoints", []):
            cfg = dict(cfg)
            cfg["_source_file"] = str(path)
            configs.append(cfg)
    return configs


def enrich_endpoint_config(cfg: dict[str, Any]) -> tuple[dict[str, Any], str, str]:
    cfg = normalize_endpoint_capability(cfg)
    namespace = cfg.get("namespace") or f"tushare.{cfg.get('family', 'unknown')}"
    cfg["namespace"] = namespace
    if not cfg.get("partition"):
        template = cfg.get("partition_template") or "year_month"
        primary_date_field = cfg.get("primary_date_field")
        partition: dict[str, Any] = {
            "name": f"{cfg['api_name']}_{template}_v1",
            "template": template,
        }
        if primary_date_field:
            partition["date_field"] = primary_date_field
        cfg["partition"] = partition
    tid = table_id(namespace, cfg["api_name"])
    part = cfg.get("partition", {})
    psid = partition_spec_id(part.get("name", f"{cfg['api_name']}_default"), part)
    return cfg, tid, psid


def load_into_catalog(root: Path, catalog) -> list[dict[str, Any]]:
    ensure_endpoint_files(root)
    configs = load_endpoint_configs(root)
    for cfg in configs:
        enriched, tid, psid = enrich_endpoint_config(cfg)
        catalog.upsert_endpoint(enriched, tid, psid)
    return configs
