from __future__ import annotations

import shutil
from importlib import resources
from pathlib import Path
from typing import Any

import yaml

from .capabilities import normalize_endpoint_capability
from .hashing import partition_spec_id, table_id

INVENTORY_REQUIRED_FIELDS = {
    "api_name",
    "endpoint_kind",
    "planner_kind",
    "execution_status",
    "reason_disabled",
    "required_infra",
    "risk_level",
    "notes",
}


def bundled_endpoint_files() -> list[Path]:
    root = resources.files("tushare_mirror.endpoint_configs")
    out: list[Path] = []
    for item in root.iterdir():
        if item.name.endswith(('.yaml', '.yml')):
            with resources.as_file(item) as path:
                out.append(Path(path))
    return out


def bundled_inventory_files() -> list[Path]:
    root = resources.files("tushare_mirror.endpoint_configs.inventory")
    out: list[Path] = []
    for item in root.iterdir():
        if item.name.endswith((".yaml", ".yml")):
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


def validate_inventory_config(cfg: dict[str, Any], source: str = "<inventory>") -> dict[str, Any]:
    missing = sorted(field for field in INVENTORY_REQUIRED_FIELDS if field not in cfg)
    if missing:
        raise ValueError(f"malformed inventory endpoint in {source}: missing {', '.join(missing)}")
    if cfg.get("execution_status") != "disabled":
        raise ValueError(f"inventory endpoint must be disabled in {source}: {cfg.get('api_name')}")
    return cfg


def load_inventory_configs() -> list[dict[str, Any]]:
    configs: list[dict[str, Any]] = []
    for path in sorted(bundled_inventory_files()):
        data = yaml.safe_load(path.read_text()) or {}
        for cfg in data.get("endpoints", []):
            item = validate_inventory_config(dict(cfg), str(path))
            item["_source_file"] = str(path)
            configs.append(item)
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
