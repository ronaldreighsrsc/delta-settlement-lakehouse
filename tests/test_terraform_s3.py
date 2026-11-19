"""TDD tests for Spec 004 & Spec 005: Terraform S3, Glue Data Catalog, Athena FinOps, EventBridge, Lambda & CloudWatch Observability."""

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TF_DIR = PROJECT_ROOT / "infra" / "terraform"
MAIN_TF = TF_DIR / "main.tf"


def _read_main_tf() -> str:
    return MAIN_TF.read_text(encoding="utf-8")


def _read_all_tf() -> str:
    return "\n".join(
        p.read_text(encoding="utf-8") for p in sorted(TF_DIR.glob("*.tf"))
    )


class TestSpec005TerraformAwsLakehouseStack:
    """Contract tests verifying Spec 005 AWS Glue Catalog, Athena, EventBridge, Lambda, and CloudWatch HCL."""

    def test_glue_catalog_database_and_tables_defined(self) -> None:
        tf_text = _read_main_tf()

        assert 'resource "aws_glue_catalog_database"' in tf_text
        assert "settlement_lakehouse_db" in tf_text

        assert 'resource "aws_glue_catalog_table"' in tf_text
        assert "silver_settlements" in tf_text
        assert "fct_daily_settlements" in tf_text
        assert "ParquetHiveSerDe" in tf_text
        assert "MapredParquetInputFormat" in tf_text
        assert "MapredParquetOutputFormat" in tf_text
        assert 'aws_s3_bucket.medallion["silver"].bucket' in tf_text
        assert 'aws_s3_bucket.medallion["gold"].bucket' in tf_text

    def test_athena_workgroup_finops_guardrail_and_named_query_defined(self) -> None:
        tf_text = _read_main_tf()

        assert 'resource "aws_athena_workgroup"' in tf_text
        assert "bytes_scanned_cutoff_per_query" in tf_text
        assert "enforce_workgroup_configuration = true" in tf_text or "enforce_workgroup_configuration" in tf_text

        assert 'resource "aws_athena_named_query"' in tf_text
        assert "fct_daily_settlements" in tf_text

    def test_cloudwatch_log_group_and_dlq_metric_alarm_defined(self) -> None:
        tf_text = _read_main_tf()

        assert 'resource "aws_cloudwatch_log_group"' in tf_text
        assert "retention_in_days" in tf_text

        assert 'resource "aws_cloudwatch_metric_alarm"' in tf_text
        assert "QuarantineRecordCount" in tf_text
        assert "SettlementLakehouse/DataQuality" in tf_text
        assert "GreaterThanThreshold" in tf_text
        assert re.search(r"threshold\s*=\s*0", tf_text)

    def test_eventbridge_s3_bronze_rule_lambda_trigger_and_target_defined(self) -> None:
        tf_text = _read_main_tf()

        assert 'resource "aws_cloudwatch_event_rule"' in tf_text
        assert "Object Created" in tf_text or "ObjectCreated" in tf_text
        assert 'aws_s3_bucket.medallion["bronze"].bucket' in tf_text

        assert 'resource "aws_lambda_function"' in tf_text
        assert "python3.12" in tf_text
        assert 'resource "aws_cloudwatch_event_target"' in tf_text


class TestSpec005AwsObservabilityAndGlueCatalogPayloads:
    """Unit tests for src/lakehouse/aws_observability.py deterministic offline payloads."""

    def test_build_cloudwatch_emf_and_put_metric_data_payloads(self) -> None:
        from src.lakehouse.aws_observability import (
            CLOUDWATCH_NAMESPACE,
            PipelineTelemetrySnapshot,
            build_cloudwatch_emf_payload,
            build_put_metric_data_payload,
        )

        snapshot = PipelineTelemetrySnapshot(
            batch_id="batch_20261114_01",
            batch_date="2026-11-14",
            environment="prod",
            silver_valid_record_count=940,
            quarantine_record_count=12,
            pipeline_latency_ms=1420.5,
            recorded_at_utc=datetime(2026, 11, 14, 3, 30, tzinfo=timezone.utc),
        )

        assert snapshot.has_dlq_anomaly is True
        assert CLOUDWATCH_NAMESPACE == "SettlementLakehouse/DataQuality"

        emf = build_cloudwatch_emf_payload(snapshot)
        assert emf["_aws"]["CloudWatchMetrics"][0]["Namespace"] == "SettlementLakehouse/DataQuality"
        metric_names = {
            m["Name"]: m["Unit"]
            for m in emf["_aws"]["CloudWatchMetrics"][0]["Metrics"]
        }
        assert metric_names == {
            "QuarantineRecordCount": "Count",
            "SilverValidRecordCount": "Count",
            "PipelineLatencyMs": "Milliseconds",
        }
        assert emf["QuarantineRecordCount"] == 12
        assert emf["SilverValidRecordCount"] == 940
        assert emf["PipelineLatencyMs"] == 1420.5
        assert emf["Environment"] == "prod"
        assert emf["PipelineName"] == "settlement-medallion-lakehouse"
        # Must serialize cleanly to JSON for CloudWatch Logs EMF ingestion
        serialized = json.dumps(emf)
        assert "SettlementLakehouse/DataQuality" in serialized

        boto_payload = build_put_metric_data_payload(snapshot)
        assert boto_payload["Namespace"] == "SettlementLakehouse/DataQuality"
        by_name = {m["MetricName"]: m for m in boto_payload["MetricData"]}
        assert set(by_name.keys()) == {
            "QuarantineRecordCount",
            "SilverValidRecordCount",
            "PipelineLatencyMs",
        }
        assert by_name["QuarantineRecordCount"]["Value"] == 12.0
        assert by_name["QuarantineRecordCount"]["Unit"] == "Count"
        assert by_name["SilverValidRecordCount"]["Value"] == 940.0
        assert by_name["PipelineLatencyMs"]["Value"] == 1420.5
        assert by_name["PipelineLatencyMs"]["Unit"] == "Milliseconds"

    def test_build_glue_partition_payload_and_athena_ddl(self) -> None:
        from src.lakehouse.aws_observability import (
            PartitionRegistrationSpec,
            build_athena_partition_ddl,
            build_glue_partition_payload,
        )

        spec = PartitionRegistrationSpec(
            database_name="settlement_lakehouse_db",
            table_name="silver_settlements",
            s3_bucket="settlement-lakehouse-silver-prod",
            s3_table_prefix="silver_settlements",
            partition_values={"settlement_currency": "USD", "settlement_date": "2026-11-14"},
        )

        glue_payload = build_glue_partition_payload(spec)
        assert glue_payload["DatabaseName"] == "settlement_lakehouse_db"
        assert glue_payload["TableName"] == "silver_settlements"
        partition_input = glue_payload["PartitionInputList"][0]
        assert partition_input["Values"] == ["USD", "2026-11-14"]
        assert (
            partition_input["StorageDescriptor"]["Location"]
            == "s3://settlement-lakehouse-silver-prod/silver_settlements/settlement_currency=USD/settlement_date=2026-11-14/"
        )
        assert (
            partition_input["StorageDescriptor"]["SerdeInfo"]["SerializationLibrary"]
            == "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        )

        ddl = build_athena_partition_ddl(spec)
        assert "ALTER TABLE settlement_lakehouse_db.silver_settlements ADD IF NOT EXISTS" in ddl
        assert "PARTITION (settlement_currency = 'USD', settlement_date = '2026-11-14')" in ddl
        assert (
            "LOCATION 's3://settlement-lakehouse-silver-prod/silver_settlements/settlement_currency=USD/settlement_date=2026-11-14/'"
            in ddl
        )
