from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .catalog import CatalogStore
from .client import TushareClient, classify_probe_response
from .endpoints import load_into_catalog
from .hashing import token_hash
from .reader import LakeReader
from .store import FileLakeStore
from .validation import Validator


def load_dotenv(path: Path = Path('.env')) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        os.environ.setdefault(key.strip(), value.strip())


def valid_until_for(status: str) -> str:
    now = datetime.now(timezone.utc)
    if status in {'accessible', 'empty_but_accessible'}:
        delta = timedelta(days=7)
    elif status in {'rate_limited', 'network_error', 'server_error', 'unknown_error'}:
        delta = timedelta(days=1)
    else:
        delta = timedelta(days=30)
    return (now + delta).isoformat().replace('+00:00', 'Z')


def require_token() -> str:
    load_dotenv()
    token = os.environ.get('TUSHARE_TOKEN')
    if not token:
        raise SystemExit('TUSHARE_TOKEN is required')
    return token


def cmd_init_catalog(args) -> int:
    root = Path(args.root)
    catalog = CatalogStore(root)
    catalog.init()
    configs = load_into_catalog(root, catalog)
    print(f'initialized catalog: {catalog.db_path}')
    print(f'loaded endpoints: {len(configs)}')
    return 0


def _ensure_catalog(root: Path) -> CatalogStore:
    catalog = CatalogStore(root)
    catalog.init()
    load_into_catalog(root, catalog)
    return catalog


def cmd_probe(args) -> int:
    root = Path(args.root)
    catalog = _ensure_catalog(root)
    token = require_token()
    client = TushareClient(token)
    endpoints = []
    if args.all:
        endpoints = [e['api_name'] for e in catalog.list_endpoints(args.family)]
    elif args.family:
        endpoints = [e['api_name'] for e in catalog.list_endpoints(args.family)]
    elif args.api:
        endpoints = [args.api]
    else:
        raise SystemExit('probe requires --api, --family, or --all')
    thash = token_hash(token)
    exit_code = 0
    for api_name in endpoints:
        cfg = catalog.get_endpoint_config(api_name)
        probe = cfg.get('probe') or {}
        params = probe.get('params') or {}
        fields = probe.get('fields') or cfg.get('default_fields') or []
        try:
            response = client.request(api_name, params, fields)
            status, error = classify_probe_response(response)
        except Exception as e:
            response = {'error': str(e)}
            status, error = 'network_error', str(e)
        if status == 'empty_but_accessible' and not probe.get('allow_empty_probe'):
            exit_code = 1
        if status not in {'accessible', 'empty_but_accessible'}:
            exit_code = 1
        catalog.record_probe(api_name, thash, status, params, fields, valid_until_for(status), error, response)
        print(f'{api_name}: {status}' + (f' ({error})' if error else ''))
    return exit_code


def cmd_fetch(args) -> int:
    root = Path(args.root)
    catalog = _ensure_catalog(root)
    token = require_token()
    params = json.loads(args.params)
    result = FileLakeStore(root, catalog).fetch(args.api, params, TushareClient(token))
    if result.skipped:
        print(f'skipped existing job: {result.job_key}')
    else:
        print(f'run_id={result.run_id}')
        print(f'job_key={result.job_key}')
        print(f'snapshot_id={result.snapshot_id}')
        print(f'record_count={result.record_count}')
    return 0 if result.snapshot_id or result.skipped else 1


def cmd_validate(args) -> int:
    root = Path(args.root)
    catalog = _ensure_catalog(root)
    validator = Validator(root, catalog)
    ok, validation_run_id = validator.validate_snapshot(args.snapshot, args.api)
    print(f'validation_run_id={validation_run_id}')
    print('status=succeeded' if ok else 'status=failed')
    return 0 if ok else 1


def cmd_list_files(args) -> int:
    root = Path(args.root)
    catalog = _ensure_catalog(root)
    files = LakeReader(root, catalog).list_active_files(args.api, args.snapshot)
    for f in files:
        print(f"{f['file_id']} {f['content_type']} {f['record_count']} {f['relative_path']}")
    print(f'total={len(files)}')
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='python -m tushare_mirror')
    parser.add_argument('--root', default='data/tushare')
    sub = parser.add_subparsers(dest='command', required=True)

    p = sub.add_parser('init-catalog')
    p.set_defaults(func=cmd_init_catalog)

    p = sub.add_parser('probe')
    p.add_argument('--api')
    p.add_argument('--family')
    p.add_argument('--all', action='store_true')
    p.set_defaults(func=cmd_probe)

    p = sub.add_parser('fetch')
    p.add_argument('--api', required=True)
    p.add_argument('--params', required=True)
    p.set_defaults(func=cmd_fetch)

    p = sub.add_parser('validate')
    p.add_argument('--snapshot', default='latest')
    p.add_argument('--api')
    p.add_argument('--all-active', action='store_true')
    p.set_defaults(func=cmd_validate)

    p = sub.add_parser('list-files')
    p.add_argument('--api', required=True)
    p.add_argument('--snapshot', default='latest')
    p.set_defaults(func=cmd_list_files)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)
