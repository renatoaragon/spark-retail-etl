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
                    │  quality    │  not-null / unique / non-negative +
                    └──────┬──────┘  referential integrity + volume anomaly
                           │         (raises on failure)
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
                    │  curated    │  daily revenue / orders / customers /
                    └──────┬──────┘  units / avg order value per category
                           ▼
            data/curated/daily_category_revenue (Parquet)
```

### Incremental load

The high-watermark is the maximum `order_ts` processed so far, stored as a plain
text ISO timestamp under `data/_state/`. On each run the pipeline reads only rows
strictly newer than it, appends the cleaned batch to an accumulating **clean fact
layer**, then rebuilds the curated mart — de-duplicating by `order_id` so a
corrected order arriving in a later batch supersedes the earlier one. Both the
clean layer and the mart are **partitioned by `order_date`**, and an incremental
run recomputes only the day-partitions its batch touched (see below).

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
  summary.py      # JSON summary of the last successful run
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
the revenue/enrichment logic, the aggregation, each data quality check, and an
end-to-end run of the incremental pipeline across two batches. CI runs it on a
**Python 3.10 / 3.11 matrix** with a **coverage gate** on every push and pull
request via GitHub Actions. Tests that write Parquet skip automatically on
Windows without winutils and run on the Linux CI.

## Design notes

- **Explicit schemas** on read, never `inferSchema`, so types are deterministic.
- **Pure transformations** — each step is a `DataFrame -> DataFrame` function,
  trivial to unit test without touching the filesystem.
- **Quality as a gate** — `run_checks` raises `DataQualityError`, so bad data
  never reaches the curated layer.
- **Volume-anomaly detection** — beyond per-row checks, each run's row count is
  compared against the **median** of recent runs (recorded under `data/_state/`)
  and fails if it lands outside `median ± 50%`. This catches failures the row
  checks can't see — a truncated source (far too few rows) or a duplicated/fan-out
  load (far too many). The median (not mean) resists a single spike skewing the
  baseline; the check stays quiet until a few runs of history exist, so it never
  blocks a cold start. The count is recorded only after a run fully succeeds.
- **Incremental by watermark** — only new source rows are processed each run. The
  watermark is advanced from the *raw* batch (before cleaning drops rows), so an
  invalid row is never re-read just because it was filtered out. It is written
  **only after the mart write succeeds**, so a mid-run failure retries the whole
  batch instead of silently skipping it with a stale mart.
- **Partitioned by `order_date`** — both the clean layer and the mart partition on
  the day. `order_date` is naturally low-cardinality (one partition per day),
  which makes it a good partition key; something like `order_id` would explode
  into one tiny file per order. With dynamic partition overwrite, an incremental
  run rewrites only the day-partitions its batch touched and leaves every other
  day byte-for-byte untouched — turning a full-mart rewrite into a few-partitions
  rewrite while keeping aggregates correct.
- **Run summary** — every completed run writes `data/_state/last_run.json`: mode,
  row count, watermark before/after, the verdict of every quality check, and
  duration. It answers "what did the last run do?" without scraping logs, and is
  the natural hook for a scheduler or alerting to read. Written **only on
  success** (like the watermark), so after a failure it still describes the last
  run whose outputs can be trusted.
- **Reproducible data** — the generator is seeded; the committed sample makes the
  repo runnable out of the box.

## Architecture decisions

The non-obvious design choices are recorded as ADRs under
[`docs/adr/`](docs/adr/):

- [ADR 0001 — Incremental load with a file-based watermark](docs/adr/0001-incremental-load-with-watermark.md)
- [ADR 0002 — Partition output tables by `order_date`](docs/adr/0002-partition-output-by-order-date.md)
- [ADR 0003 — Volume anomaly gate on the median of recent runs](docs/adr/0003-volume-anomaly-gate.md)

## License

MIT — see [LICENSE](LICENSE).
