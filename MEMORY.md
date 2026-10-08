# 🧠 MEMORY.md — Persistent Repository Memory for delta-settlement-lakehouse

## 👤 1. Candidate & Project Context
- **Candidate:** Ronald Solares Chuquera, Industrial Civil Engineer (Top 6%, Universidad de Tarapacá).
- **Target Role:** Remote International Data Engineer (Fintech, B2B Payments, Modern Data Stack).
- **Aspirational Goal:** Digital nomad lifestyle traveling the world with high-yield remote engineering positions.
- **Language Standard:** 100% International Technical English across code, docstrings, tests, and documentation.

## 📍 2. Current Project State
- **Phase:** Spec 001 Fully Implemented & Tested (17 tests passing 100% in ~2.8s).
- **GitHub Repository:** `https://github.com/ronaldreighsrsc/delta-settlement-lakehouse`
- **Core Architecture:**
  1. **Bronze Layer (`bronze_raw_events`):** Append-only raw JSON stream ingestion partitioned by `ingestion_date` via Delta Lake.
  2. **Silver Conformance & DLQ (`silver_settlements`, `silver_quarantine_dlq`):** Data Contract Gate validating ISO 4217 currencies, SWIFT BICs, and non-positive amounts with automated quarantine routing.
  3. **Gold Layer (`gold_daily_reconciliation`, `gold_counterparty_exposure`):** Kimball star schema aggregated marts computed via DuckDB in-memory OLAP.
  4. **Time Travel & Audits:** Historical `versionAsOf` queries over Delta logs (`_delta_log`).
  5. **Interactive Typer CLI:** Commands `run-pipeline`, `inspect-dlq`, `reconcile`, and `audit-table` with Rich styling.

## ⚖️ 3. Key Decisions & Rationale
- **Delta Lake over Spark Cluster:** Using `deltalake` (Rust delta-rs engine) provides full ACID, partition pruning, and time travel without needing a 1 GB JVM runtime.
- **DuckDB Vectorized OLAP:** Zero-copy querying over PyArrow memory buffers delivers 10x faster local processing than Pandas.
- **Dead Letter Queue (DLQ):** Non-negotiable in financial engineering; corrupted transactions are preserved with structured error tags.
- **Deterministic Chaos Generation:** Allows repeatable automated tests and predictable demonstration runs for recruiters.

## ⚠️ 4. Lessons Learned & Guardrails
- **Post-Mortem Guardrail (PyArrow Timestamps):** Always specify `pa.timestamp("us", tz="UTC")` explicitly to prevent timezone conversion discrepancies across Windows/Linux runtimes.
- **Precision Floating Point:** Financial amounts in domain models use `Decimal` for business invariants, casting to `float64` only at the physical storage boundary for Parquet vectorization.
