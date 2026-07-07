# Architecture Decision Records

This directory records the non-obvious design decisions behind the pipeline, in
[ADR](https://adr.github.io/) format — one short file per decision, capturing the
**context**, the **decision**, and its **consequences** so the reasoning survives
after the diff is forgotten.

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-incremental-load-with-watermark.md) | Incremental load with a file-based watermark | Accepted |
| [0002](0002-partition-output-by-order-date.md) | Partition output tables by `order_date` | Accepted |
