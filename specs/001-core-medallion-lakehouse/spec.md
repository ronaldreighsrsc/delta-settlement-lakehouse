# Spec 001 — Core Medallion Lakehouse Engine & Batch Settlement Pipeline

**Status:** implemented  
**Author:** Ronald Solares  
**Date:** 2026-10-08  
**Project:** `delta-settlement-lakehouse` (`C:\Users\ronal\delta-settlement-lakehouse`)

---

## 1. Context and Objective
In international B2B commerce and cross-border financial operations, settlement engines must ingest high-volume, multi-currency payment feeds from fragmented clearing networks (SWIFT, SEPA, local ACHs, payment gateways). Traditional transactional databases struggle with analytical aggregation across millions of daily transactions, while naive data lakes suffer from lack of ACID transactions, dirty data propagation, and inability to perform historical regulatory audits.

**Spec 001** introduces a production-grade **Fintech Lakehouse Engine** built with **Delta Lake** and **DuckDB**, implementing the canonical **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)** with strict **Data Contract Gates**, automated **Dead Letter Queue (DLQ) quarantine**, and **historical Time Travel auditability**.

---

## 2. Actors & Target Personas
* **Settlement Operations Engineer / Quantitative Auditor:** Requires daily balance reconciliation, net bilateral settlement positions, and foreign exchange (FX) slippage monitoring with sub-cent precision.
* **Lead Data Engineer / Tech Recruiter:** Evaluates architectural design, ACID table guarantees, deterministic data transformations, and defensive data quality governance.

---

## 3. User Stories
* **US-1 (Raw Multi-Currency Ingestion):** As a data engineer, I want to ingest raw transaction payloads into an append-only Bronze Delta table with batch metadata so that the original source of truth is permanently preserved without schema drift friction.
* **US-2 (Strict Cleansing & DLQ Routing):** As an operations auditor, I want malformed, duplicate, or contract-violating transactions to be automatically filtered and redirected to a Silver Quarantine DLQ table, while valid transactions are standardized and stored in Silver with decimal precision.
* **US-3 (Dimensional Aggregation in Gold):** As a financial analyst, I want pre-aggregated daily settlement metrics and counterparty bilateral exposures modeled in a Kimball star schema in Gold, enabling instant queries across currencies.
* **US-4 (Historical Forensic Time Travel):** As a regulatory auditor, I want to query the state of any Delta table at a specific historical version or timestamp to inspect ledger changes before and after late-arriving data corrections.

---

## 4. Functional Requirements (in EARS Notation)

### Bronze Ingestion
* **RF-1:** **WHEN** a raw batch of payment records is ingested, **THE SYSTEM** persists the payloads as an append-only Delta table (`bronze_raw_events`) partitioned by `ingestion_date` (`YYYY-MM-DD`), appending lineage columns (`event_id`, `batch_id`, `ingested_at_utc`, `source_feed`, `raw_payload`).
* **RF-2:** **THE SYSTEM** guarantees atomic write operations to the Bronze Delta table using transactional logs (`_delta_log`).

### Silver Conformance & Data Contract Gates
* **RF-3:** **WHEN** transforming Bronze events into Silver (`silver_settlements`), **THE SYSTEM** evaluates incoming records against strict financial data contracts (valid ISO 4217 currency codes, valid 8/11-character SWIFT BICs, non-zero positive amounts, valid ISO timestamps).
* **RF-4:** **IF** a transaction violates the data contract or contains unparseable fields, **THEN THE SYSTEM** diverts the record to `silver_quarantine_dlq` with `rejection_reason`, `rejection_timestamp`, and `batch_id`, without terminating the valid pipeline execution.
* **RF-5:** **WHEN** writing valid records to `silver_settlements`, **THE SYSTEM** deduplicates records idempotently using `(transaction_id, batch_id)` and partitions the table by `settlement_currency` and `settlement_year_month`.

### Gold Business Dimensional Modeling
* **RF-6:** **WHEN** processing Silver records into the Gold layer, **THE SYSTEM** computes two dimensional analytical Delta tables:
  * `gold_daily_reconciliation`: Total volume per currency, total fees collected, net FX slippage, settled count, and rejected count per settlement date.
  * `gold_counterparty_exposure`: Bilateral net volume, gross exposure, and transaction counts between sender and receiver BICs.

### Time Travel & Auditability
* **RF-7:** **WHEN** an auditor requests a table state specifying `--as-of-version <V>` or `--as-of-date <TIMESTAMP>`, **THE SYSTEM** leverages Delta Lake transaction log history to return the exact historical snapshot.
* **RF-8:** **THE SYSTEM** provides an inspection command (`lakehouse dlq-inspect`) reporting summary metrics and error categorizations from the quarantine table.

### Synthetic Chaos Generator
* **RF-9:** **WHEN** executing `lakehouse generate-feed`, **THE SYSTEM** synthesizes realistic B2B cross-border transaction batches with deterministic random seeds, optionally injecting chaos (out-of-order timestamps, duplicate network retries, and invalid currency codes).

---

## 5. Non-Functional Requirements (NFR)
* **RNF-1 (Financial Precision):** Monies and FX exchange rates must be computed using fixed-point Decimal arithmetic to eliminate floating-point representation anomalies.
* **RNF-2 (Pipeline Execution Latency):** Ingestion and transformation of 100,000 transactions across Bronze, Silver, and Gold must execute locally in under 5.0 seconds using vectorization (DuckDB/PyArrow).
* **RNF-3 (Offline-First Zero-JVM Dependency):** Core Lakehouse operations must run natively in Python via `deltalake` (Rust engine) and `duckdb`, without requiring a live JVM cluster for unit tests.
* **RNF-4 (Test Coverage & Reproducibility):** 100% test pass rate in `pytest` with deterministic fixtures.

---

## 6. Out of Scope
* Real-time streaming with Kafka/Pulsar (micro-batch and scheduled batch processing is the focus of Spec 001).
* Front-office payment authorization gateways or credit card tokenization.

---

## 7. Definition of Done
1. Complete Medallion pipeline code implemented in `delta_settlement_lakehouse/`.
2. Delta Lake storage working with ACID transactions, schema enforcement, and time travel.
3. Data quality contract gate successfully routing corrupted records to DLQ.
4. Gold dimensional models correctly reconciling batch volumes and counterparty exposures.
5. Rich Typer CLI with `ingest`, `process`, `audit`, and `dlq-inspect` commands.
6. 100% green unit and integration tests running under 5 seconds.
