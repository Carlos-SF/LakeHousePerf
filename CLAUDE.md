# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A teaching/demo project that builds a Databricks **Lakehouse (medallion) pipeline** for a
fictional perfume distributor, "rotaperfume". Source data (ERP + CRM CSVs) lands in Unity
Catalog, is ingested to a **bronze** layer as-is, then cleaned/typed in later layers.
Narration is in Portuguese; identifiers and code are a mix of Portuguese/English.

Two moving parts:

- **`dados/`** — the raw source CSVs (not committed as pipeline output; they are the input).
  - `dados/erp/`: `produtos` (292), `pedidos` (28 729), `itens_pedido` (197 724),
    `pagamentos` (27 772), `estoque` (8 400).
  - `dados/crm/`: `clientes` (3 040), `vendedores` (42), `carteira` (3 637),
    `oportunidades` (5 979), `visitas` (37 936).
  - **Total 313 551 rows** across 10 tables (generator seed 42). These counts are a
    contract: ingestion must reproduce them exactly (rows = file lines − header).
- **`rotaperfume/`** — a Databricks Asset Bundle (DABs, `default-python` template) that
  deploys and runs the pipeline. This is where all `databricks bundle` and `pytest`
  commands run from. `src/` and `resources/` are currently **empty scaffolding** — the
  pipeline code has not been written yet.

`.llm/prompt_01.md` is the authoritative spec for the next layer to build (the bronze
ingestion) — read it before writing ingestion code; it encodes the exact rules and the
expected verification SQL.

## Architecture: the medallion contract

The design intent (from `.llm/prompt_01.md`) is what requires reading multiple files to
grasp. Follow it:

- **raw** → CSVs uploaded to a UC Volume at `/Volumes/{catalog}/bronze/raw/{sistema}/{tabela}.csv`.
- **bronze** → one Delta table per CSV, `overwrite` mode. Strict rules:
  - Read **everything as string**. No `inferSchema` / `inferColumnTypes` — inferring
    turns CNPJs and CEPs into numbers (drops leading zeros on 309 rows) and BR dates
    (`dd/MM/yyyy`) into null, silently. Bronze must **preserve the dirt**, not fix it.
  - CSVs are CRLF with a header; do **not** use `multiLine`.
  - Add only two metadata columns: `_ingerido_em` (timestamp) and `_arquivo_origem`.
  - Write the ingestion function **once** and iterate over the list of 10 tables — never
    repeat a block per table. Add a `COMMENT` per table naming its source system.
  - After load, print each table's row count and assert it matches the source file; fail
    on divergence.
  - The file reader may add a `_rescued_data` column — drop it with `SELECT * EXCEPT
    (_rescued_data)`. Passing `rescuedDataColumn => ''` does **not** disable it.
- **silver** (later) → this is where cleaning/typing lives: `trim`, `regexp_replace`,
  `lpad` for CNPJ, `try_to_date` for dates, dedup (clientes 3 040 → 3 000). Conversion is
  written once, here, for everyone — never `CAST` a preserved-as-text value in bronze.

## Common commands

Run from **`rotaperfume/`** (where `databricks.yml` and `pyproject.toml` live).

```bash
uv sync --dev                      # install deps (uv, not pip)
uv run pytest                      # run all tests
uv run pytest tests/test_x.py::test_name   # run a single test
uv run ruff check                  # lint (line-length 120)
uv run ruff format                 # format

databricks bundle validate --target dev --profile <name>
databricks bundle deploy   --target dev --profile <name>
databricks bundle run <job_name> --target dev --profile <name>
```

`dev` is the default target (development mode; resources prefixed `[dev <user>]`,
schedules paused). `prod` deploys to a single copy under the owner's workspace path.

## Gotchas that will bite

- **Catalog name — canonical is `lakehouse_rotaperfume`.** The `catalog` variable in
  `databricks.yml` and the SQL in `.llm/prompt_01.md` both use it, and the bronze layer
  (all 10 tables + `_raw_arquivos`, 313 551 rows) is built and verified there. Earlier
  misnamed catalogs (`lakehouse_rotaperfime`, `lakehouse_rouper`) have been removed — don't
  reintroduce them.
- **Creating the catalog needs SQL, not the CLI.** This metastore uses Default Storage, so
  `databricks catalogs create` fails asking for a storage location. Use
  `CREATE CATALOG IF NOT EXISTS ...` via SQL (what `raw_conferencia` does).
- **Profile.** The bundle host is `dbc-8f60023a-5d91.cloud.databricks.com`, matching the
  `dbc-8f60023a-5d91` and `LakeHousePerf` profiles in `.databrickscfg`. The prompt's
  example commands reference `--profile projeto-dados-ia`, which does **not** exist here.
  Never auto-select a profile — pass `--profile` explicitly and let the user choose.
- **Tests need remote compute.** `tests/conftest.py` runs on Databricks Connect and falls
  back to serverless (`DATABRICKS_SERVERLESS_COMPUTE_ID=auto`) if no cluster/warehouse is
  configured. `databricks-connect` is pinned `>=15.4,<15.5` — the remote runtime must be
  compatible. `spark` and `load_fixture` fixtures are provided; put test data in `fixtures/`.

## Databricks tooling

Per `rotaperfume/AGENTS.md` and the session setup: **load the `databricks-core` skill
first** for any CLI, auth, profile, or bundle work, then the matching product skill
(e.g. `databricks-dabs`, `databricks-pipelines`, `databricks-jobs`). `rotaperfume/CLAUDE.md`
is intentionally thin — it just imports `AGENTS.md`.
