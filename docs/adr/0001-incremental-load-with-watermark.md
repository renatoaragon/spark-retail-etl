# 1. Incremental load with a file-based watermark

- Status: Accepted
- Date: 2025-01

## Context

The pipeline originally did a **full reload** on every run: it read the entire
`orders.csv`, reprocessed all of it, and overwrote the mart. That is simple and
always correct, but it does not reflect how a batch pipeline behaves as the source
grows — re-reading everything wastes compute and lengthens each run linearly with
total history, even when only a handful of new rows arrived.

We wanted incremental processing without pulling in a stateful table format
(Delta/Iceberg/Hudi) or an external metadata store, keeping the project runnable
out of the box with nothing but Spark and the local filesystem.

## Decision

Track a **high-watermark** — the maximum `order_ts` processed so far — persisted as
a plain-text ISO timestamp under `data/_state/`. Each run:

1. Reads only rows strictly newer than the watermark (`select_new`).
2. Cleans that batch and appends it to an accumulating **clean fact layer**.
3. Rebuilds the curated mart from the clean layer, de-duplicating by `order_id`.
4. Advances the watermark — **only after the mart write succeeds**.

The watermark is computed from the **raw** batch (before cleaning drops rows), and
`select_new` uses a strict `>` comparison.

## Consequences

**Positive**

- Each run's cost scales with *new* data, not total history.
- State is a human-readable file — trivial to inspect, reset, or version.
- No dependency on a table format or metastore; the repo still runs on plain Spark.
- Advancing the watermark last makes a mid-run failure **retry the whole batch**
  rather than silently skip it with a stale mart.

**Negative / trade-offs**

- Watermark taken from the raw batch means a permanently invalid row is dropped
  after its first sighting and never revisited. Acceptable — the quality gate
  already rejects it, and re-reading it forever would stall progress.
- Strict `>` skips a row whose timestamp exactly equals the watermark on the
  boundary run. `order_ts` carries sub-second resolution, so exact collisions are
  unlikely; the alternative (`>=`) would reprocess boundary rows every run.
- A crash between the clean-layer append and the mart write can leave a duplicate
  physical row in the clean layer on retry. The mart stays correct because
  `latest_per_order` de-duplicates by `order_id` on rebuild; only physical
  compaction is deferred.

## Alternatives considered

- **Delta Lake / Iceberg with MERGE** — the production-grade answer (ACID, true
  upserts, time travel). Rejected here as too heavy for a small, dependency-light
  portfolio project; it is the natural next step at real scale.
- **Store the watermark in a database** — unnecessary for a single-writer batch job
  and would add a service dependency.
