# spark-retail-etl

![CI](https://github.com/renatoaragon/spark-retail-etl/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C)
![License](https://img.shields.io/badge/license-MIT-green)

A small, production-shaped **batch ETL pipeline built with PySpark**. It takes raw
retail orders through a **raw → clean → curated** flow, enforces **data quality
gates** before writing, and produces a curated analytics mart. The pipeline is
**incremental**: each run processes only source rows newer than a persisted
high-watermark. Runs locally with committed synthetic data and is covered by a
unit test suite in CI.

> Built to demonstrate how I structure data pipelines: typed schemas, testable
> pure transformations, explicit quality checks, and reproducible sample data.
> No real or personal data is used.

## Architecture

```
                    ┌─────────────┐
 data/raw/*.csv ───▶│   extract   │  explicit schemas (no inferSchema)
                    └──────┬──────┘
                           ▼
                    ┌─────────────┐
 watermark ────────▶│  select_new │  keep only rows newer than last run
                    └──────┬──────┘
                           ▼
                    ┌─────────────┐
                    │   clean     │  drop invalid rows, cast types, dedupe
                    └──────┬──────┘
                           ▼
                    ┌─────────────┐
                    │  quality    │  not-null / unique / non-negative gates
                    └──────┬──────┘  (raises and stops on failure)
                           ▼
                    ┌─────────────┐
                    │ clean layer │  append batch → data/clean/orders (Parquet)
                    └──────┬──────┘  advance watermark to batch max(order_ts)
                           ▼
                    ┌─────────────┐
                    │   enrich    │  revenue = qty × price, join customer country
                    └──────┬──────┘  (rebuilt from the full clean layer)
                           ▼
                    ┌─────────────┐
                    │  curated    │  daily revenue / orders / units per category
                    └──────┬──────┘
                           ▼
            data/curated/daily_category_revenue (Parquet)
```

### Incremental load

The high-watermark is the maximum `order_ts` processed so far, stored as a plain
text ISO timestamp under `data/_state/`. On each run the pipeline reads only rows
strictly newer than it, appends the cleaned batch to an accumulating **clean fact
layer**, advances the watermark, then rebuilds the curated mart from the whole
clean layer — de-duplicating by `order_id` so a corrected order arriving in a
later batch supersedes the earlier one.

```bash
python -m etl.pipeline                 # incremental (default)
python -m etl.pipeline --full-refresh  # ignore watermark, rebuild from scratch
```

## Project layout

```
src/etl/
  extract.py      # typed readers for orders and customers
  transform.py    # clean_orders, latest_per_order, enrich_orders, daily_category_revenue
  incremental.py  # watermark read/write, select_new, high_watermark
  quality.py      # composable data quality checks + run_checks gate
  pipeline.py     # wires the stages together (entry point)
scripts/
  generate_data.py  # deterministic synthetic data generator
tests/            # pytest suite (transformations + quality checks)
```

## Quickstart

```bash
pip install -r requirements.txt

# (optional) regenerate the synthetic sample data
python scripts/generate_data.py

# run the pipeline
python -m etl.pipeline            # from the src/ dir, or:
PYTHONPATH=src python -m etl.pipeline
```

The pipeline writes the curated mart to `data/curated/daily_category_revenue`
as Parquet and prints a preview.

## Tests

```bash
pytest -q
```

The suite validates the cleaning rules (invalid-row filtering, deduplication),
the revenue/enrichment logic, the aggregation, and each data quality check. CI
runs it on every push and pull request via GitHub Actions.

## Design notes

- **Explicit schemas** on read, never `inferSchema`, so types are deterministic.
- **Pure transformations** — each step is a `DataFrame -> DataFrame` function,
  trivial to unit test without touching the filesystem.
- **Quality as a gate** — `run_checks` raises `DataQualityError`, so bad data
  never reaches the curated layer.
- **Incremental by watermark** — only new source rows are processed each run. The
  watermark is advanced from the *raw* batch (before cleaning drops rows), so an
  invalid row is never re-read just because it was filtered out. The curated mart
  is rebuilt from the clean layer rather than appended to, which keeps daily
  aggregates correct when a day's orders span more than one batch — trading a full
  mart recompute for guaranteed correctness. (Partitioning that mart rebuild to
  only touched days is the next step.)
- **Reproducible data** — the generator is seeded; the committed sample makes the
  repo runnable out of the box.

## License

MIT — see [LICENSE](LICENSE).
