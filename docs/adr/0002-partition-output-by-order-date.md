# 2. Partition output tables by `order_date`

- Status: Accepted
- Date: 2025-01
- Builds on: [0001](0001-incremental-load-with-watermark.md)

## Context

With incremental load in place ([ADR 0001](0001-incremental-load-with-watermark.md)),
the pipeline still **rebuilt the whole curated mart** on every run and overwrote it
wholesale. As history grows, rewriting every day's aggregates to land a single new
day is wasteful, and it churns files that downstream consumers may be reading.

We needed the mart rebuild to touch only the days a batch actually changed, using
Spark's native capabilities rather than a table format.

## Decision

Partition both the clean layer and the curated mart by **`order_date`**, and enable
**dynamic partition overwrite** (`spark.sql.sources.partitionOverwriteMode=dynamic`).

On an incremental run the pipeline computes the mart only for the `order_date`s
present in the batch (`distinct_dates`) and writes with `mode("overwrite")`; under
dynamic mode this replaces just those day-partitions and leaves every other day
byte-for-byte untouched. A first load or `--full-refresh` clears the output and
writes all partitions.

## Consequences

**Positive**

- An incremental run rewrites a few day-partitions instead of the entire mart.
- Untouched days are left physically unchanged — stable for downstream readers and
  cheap to back up incrementally.
- `order_date` matches how analysts actually query and backfill (by day/range), so
  partition pruning speeds reads too.

**Negative / trade-offs**

- Partitioning assumes reasonably **even, bounded per-day volume**. A severe skew
  (one day dwarfing the rest) would create lopsided partitions; date is well-behaved
  for this domain but not universally.
- The mart rebuild still reads the full clean layer to de-duplicate across batches
  before filtering to touched days — correctness first; pruning that read further is
  a later optimisation.
- Dynamic overwrite only *replaces* partitions present in the written frame; it
  never deletes a day that disappears from the source. `--full-refresh` (which
  clears the directory) is the escape hatch for that case.

## Alternatives considered

- **Partition by `order_id`** — rejected outright: catastrophically high
  cardinality (one tiny file per order), destroying both write and read performance.
  Cardinality is the deciding factor in choosing a partition key, and `order_date`
  is naturally low-cardinality (one partition per calendar day).
- **Coarser partitioning (month/year)** — fewer, larger partitions, but an
  incremental run would rewrite a whole month to change one day. Day granularity
  matches the incremental batch grain.
- **No partitioning + full mart overwrite** — the previous behaviour; simple but
  rewrites everything every run and offers no read-side pruning.
