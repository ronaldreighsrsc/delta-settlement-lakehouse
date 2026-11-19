# Spec 005 — AWS Glue Data Catalog, Amazon Athena FinOps, EventBridge + Lambda Trigger & CloudWatch DLQ Observability

**Status:** implemented  
**Author:** Ronald Solares  
**Date:** 2026-11-14  
**Project:** `delta-settlement-lakehouse` (`C:\Users\ronal\delta-settlement-lakehouse`)  

---

## 1. Context and Objective
Following Spec 004 (Terraform Medallion S3 buckets, IAM least-privilege, and dual `s3://` / local `DeltaLakeStorage`), **Spec 005** completes the enterprise AWS Native Lakehouse control plane and observability layer at **\$0 local verification cost**:
1. **Deterministic Schema-as-Code (`AWS Glue Data Catalog`):** Defines `aws_glue_catalog_database` and `aws_glue_catalog_table` resources in Terraform over the Silver (`silver_settlements`) and Gold (`fct_daily_settlements`) S3 Parquet/Delta zones, preventing schema drift from blind Glue Crawlers.
2. **Serverless SQL Analytics with FinOps Guardrails (`Amazon Athena`):** Provisions an `aws_athena_workgroup` with a hard `bytes_scanned_cutoff_per_query` ceiling and an `aws_athena_named_query` for partition-pruned corridor settlement reconciliation.
3. **Event-Driven Ingestion (`Amazon EventBridge` + `AWS Lambda`):** Configures an `aws_cloudwatch_event_rule` capturing S3 Bronze `Object Created` events and routing them via `aws_cloudwatch_event_target` to an `aws_lambda_function` ingestion trigger.
4. **Pipeline Observability & DLQ Alerting (`Amazon CloudWatch` + `aws_observability.py`):** Provisions an `aws_cloudwatch_log_group` and `aws_cloudwatch_metric_alarm` on `QuarantineRecordCount > 0` in namespace `SettlementLakehouse/DataQuality`, backed by a strictly typed Python module (`src/lakehouse/aws_observability.py`) that generates CloudWatch Embedded Metric Format (EMF) / `put_metric_data` payloads (`QuarantineRecordCount`, `SilverValidRecordCount`, `PipelineLatencyMs`) and Glue/Athena partition registration payloads.

---

## 2. Functional Requirements (EARS Notation)

### AWS Glue Data Catalog & Amazon Athena FinOps (`infra/terraform/main.tf`)
* **RF-1 (Glue Catalog Database & Tables):** **THE SYSTEM** shall declare an `aws_glue_catalog_database` (`settlement_lakehouse_db`) and `aws_glue_catalog_table` definitions (`silver_settlements` and `fct_daily_settlements`) pointing to the Silver and Gold Medallion S3 buckets using Parquet SerDe (`org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe`), typed columns, and partition keys (`settlement_currency`, `settlement_date`).
* **RF-2 (Athena FinOps Workgroup & Named Query):** **THE SYSTEM** shall declare an `aws_athena_workgroup` enforcing `bytes_scanned_cutoff_per_query` (FinOps scan guardrail) with S3 output configuration, alongside an `aws_athena_named_query` querying the Glue Catalog tables with partition pruning.

### Event-Driven S3 Ingestion (`Amazon EventBridge` + `AWS Lambda`)
* **RF-3 (EventBridge Bronze Rule & Lambda Target):** **WHEN** a raw settlement batch object is created in the Bronze S3 bucket (`Object Created`), **THE SYSTEM** shall match the event via `aws_cloudwatch_event_rule` and invoke the `aws_lambda_function` ingestion trigger via `aws_cloudwatch_event_target`.

### CloudWatch Observability & Partition Registration (`src/lakehouse/aws_observability.py`)
* **RF-4 (CloudWatch Log Group & DLQ Metric Alarm):** **THE SYSTEM** shall provision an `aws_cloudwatch_log_group` with explicit retention and an `aws_cloudwatch_metric_alarm` monitoring `QuarantineRecordCount` in namespace `SettlementLakehouse/DataQuality` that transitions to `ALARM` whenever `QuarantineRecordCount > 0`.
* **RF-5 (Deterministic EMF & Partition Payloads in Python):** **WHEN** a Medallion batch completes, **THE SYSTEM** (`src/lakehouse/aws_observability.py`) shall deterministically build:
  1. The CloudWatch Embedded Metric Format (EMF) payload and `put_metric_data` structure containing `QuarantineRecordCount` (`Count`), `SilverValidRecordCount` (`Count`), and `PipelineLatencyMs` (`Milliseconds`) in namespace `SettlementLakehouse/DataQuality`.
  2. The AWS Glue `BatchCreatePartition` input payload and Amazon Athena `ALTER TABLE ... ADD IF NOT EXISTS PARTITION (...) LOCATION 's3://...'` DDL statement.

---

## 3. Non-Functional Requirements
* **RNF-1 (Zero-Regression & Offline Determinism):** All 44 pre-existing tests plus the new Spec 005 Terraform HCL and `aws_observability` unit tests (`tests/test_terraform_s3.py`) must pass 100% offline in `<5s` without requiring live AWS credentials.
* **RNF-2 (Strict IAM Least-Privilege):** No IAM policy in `infra/terraform/*.tf` may use wildcard `Resource = "*"`; all permissions must remain scoped to explicit bucket, log group, or function ARNs.
