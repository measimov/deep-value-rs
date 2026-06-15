# Repository Guidelines

## Project Structure & Module Organization

Rust screening/backtest crate plus tools for reading a remote TuShare file lake.

- `src/`: Rust crate modules: `tushare/`, `data/`, `strategy/`, `backtest/`, `report/`.
- `tests/`: Rust/Python tests and fixtures in `tests/fixtures/`.
- `tushare_mirror/`: Python package and CLI; endpoint YAML lives in `tushare_mirror/endpoint_configs/`. Production execution happens on the LAN Linux server, not this Mac checkout.
- `docs/`, `ARCHITECTURE.md`, `scripts/`: runbooks, architecture, and operational helpers.

## Build, Test, and Development Commands

- `cargo build`: build the Rust binary.
- `cargo test`: run Rust unit and integration tests.
- `cargo run -- ping` checks Tushare API connectivity.
- `cargo run -- db ping` checks PostgreSQL via `DATABASE_URL`.
- `cargo run -- snapshot --date 20250515 --top 10` runs a sample stock screen.
- `python3 -m pytest tests/tushare_mirror` runs the Python mirror test suite.
- `python3 -m tushare_mirror --help` lists mirror CLI commands. Do not run write/sync jobs against the SMB mount.

## TuShare Mirror & File Lake Access

The TuShare file lake is exposed read-only at `smb://10.0.0.39/tushare`, backed by `/mnt/gw/TuShare`. Authenticate as `measimov`. On macOS:

```bash
mkdir -p ~/mnt/tushare
mount_smbfs //measimov@10.0.0.39/tushare ~/mnt/tushare
```

Read mainly from `lake/`. Use `_catalog/catalog.sqlite` only for snapshots and provenance. Do not write, delete, move files, or modify the catalog from clients. Use catalog snapshots for stable consumption. Prefer DuckDB, Polars, or PyArrow for Parquet reads.

## Coding Style & Naming Conventions

Use Rust 2021 idioms and keep code `rustfmt` clean with `cargo fmt`. Prefer `snake_case` for modules/functions, `PascalCase` for types, and explicit errors via `anyhow` or `thiserror`. Run `cargo clippy --all-targets --all-features` before broad Rust changes.

Python should follow PEP 8, use `snake_case`, keep CLI behavior deterministic, and prefer structured YAML/JSON parsing.

## Testing Guidelines

Add focused Rust tests near affected modules or in `tests/*.rs`. Python tests should be `test_*.py` under `tests/tushare_mirror/`. Mirror safety changes need regressions for read-only behavior, guarded bundles, and path validation.

## Commit & Pull Request Guidelines

Recent history uses short Conventional Commit-style subjects: `feat: ...`, `fix: ...`, `docs: ...`, `test: ...`. Keep subjects imperative. PRs should describe behavior, list commands run, note config or data-root assumptions, and link related issues or runbooks.

## Security & Configuration Tips

Keep `TUSHARE_TOKEN`, `DATABASE_URL`, and mirror roots in local env files or shell variables. Do not commit secrets, raw extracts, or generated command bundles. Writes to mirror or backup roots require explicit operator intent and documented safety checks.
