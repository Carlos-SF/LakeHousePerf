# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A Databricks Lakehouse data-engineering project (course material: "aula-02-engenharia-de-dados"). It ingests 10 source CSVs from a perfume-distribution business (`rotaperfume`) into Unity Catalog and builds a medallion (bronze → silver → gold) pipeline via a Databricks Asset Bundle (DAB).

Two moving parts:

- **`dados/`** — the raw source data: 10 CSVs, ~14.7 MB, ~313k rows, Portuguese column names.
  - `dados/erp/` — `produtos`, `pedidos`, `itens_pedido`, `pagamentos`, `estoque`
  - `dados/crm/` — `clientes`, `vendedores`, `carteira`, `oportunidades`, `visitas`
  - (The `crm-*/` and `erp-*/` directories being deleted in git are the original download; `dados/` is the working copy the pipeline reads.)
- **`rotaperfume/`** — the DAB itself (Python wheel + job/pipeline resources). **Still scaffolded from the `default-python` template** — `src/rotaperfume/taxis.py` and `resources/sample_job.job.yml` are placeholder examples (NYC taxi sample), not the real pipeline yet.

**`.llm/prompt_01.md`** is the design spec: it narrates the intended build-out (bronze/silver/gold schemas, a `raw` Volume, a `rotaperfume_pipeline` job that starts with an arrival-check task and grows across six deliverables). Read it to understand target architecture — but note it references an *example* workspace/catalog (`lakehouse_rotaperfume`, profile `projeto-dados-ia`). The **actual** target is defined in `rotaperfume/databricks.yml` (see below), which is the source of truth.

## Databricks workflow

**Before any Databricks CLI/bundle/data work, load the `databricks-core` skill first, then the matching product skill** (e.g. `databricks-dabs`, `databricks-jobs`). This is required by `rotaperfume/AGENTS.md` and repo convention.

**Always pass `--profile` explicitly — never rely on the implicit default.** Available profiles: `DEFAULT`, `dbc-8f60023a-5d91`. The bundle's workspace host is `https://dbc-8f60023a-5d91.cloud.databricks.com`.

All commands run from inside `rotaperfume/`:

```bash
uv sync --dev                                              # install deps (uv, not pip)
databricks bundle validate --target dev --profile <name>
databricks bundle deploy   --target dev --profile <name>  # builds wheel, deploys resources
databricks bundle run <job_name> --target dev --profile <name>
uv run pytest                                              # run tests
uv run pytest tests/sample_taxis_test.py::test_find_all_taxis   # single test
uv run ruff check .                                        # lint (line-length 120)
```

`dev` is the default target, so `--target dev` is optional. Deploying `--target prod` is a separate, explicit action.

## Bundle configuration (`rotaperfume/databricks.yml`)

- **Catalog:** `lakehouse_rouper` (both dev and prod).
- **Schema:** dev = `${workspace.current_user.short_name}` (per-user); prod = `prod`.
- **dev** uses `mode: development` (resources prefixed `[dev <user>]`, schedules paused). **prod** uses `mode: production`.
- Reference these in resource YAML as `${var.catalog}` / `${var.schema}`, and pass them to job code as `--catalog` / `--schema` args (see `sample_job.job.yml`).

## Architecture notes

- **Deployment is wheel-based.** `pyproject.toml` builds `rotaperfume` as a wheel (`uv build --wheel`); jobs reference it via `python_wheel_task` with `entry_point: main`. Job entry points live in `src/rotaperfume/main.py`, which parses `--catalog`/`--schema`, runs `USE CATALOG`/`USE SCHEMA`, then calls into modules like `taxis.py`. Add new pipeline logic as modules under `src/rotaperfume/` and wire them into `main`.
- **Everything is serverless.** No clusters are configured. `tests/conftest.py` initializes Databricks Connect and falls back to serverless compute automatically — so **`uv run pytest` requires valid Databricks auth**, it does not run purely local.
- **New resources** go in `rotaperfume/resources/*.yml` (auto-included via the `include:` glob in `databricks.yml`).
- **Test fixtures** load from `rotaperfume/fixtures/` (JSON or CSV) via the `load_fixture` pytest fixture in `conftest.py`.

## Bundle-specific agent instructions

The DABs convention file for the bundle is imported below (add bundle-specific notes there, under "Project Instructions"):

@rotaperfume/AGENTS.md
