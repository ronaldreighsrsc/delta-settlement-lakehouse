"""
AWS CloudWatch Observability (EMF / PutMetricData), Glue Data Catalog Partition Registration,
Amazon Athena DDL Generation, and EventBridge -> Lambda S3 Bronze Trigger Handler (Spec 005).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

CLOUDWATCH_NAMESPACE: str = "SettlementLakehouse/DataQuality"
PIPELINE_NAME: str = "settlement-medallion-lakehouse"

PARQUET_SERDE_LIBRARY: str = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
PARQUET_INPUT_FORMAT: str = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
PARQUET_OUTPUT_FORMAT: str = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"


@dataclass(frozen=True)
class PipelineTelemetrySnapshot:
    """Immutable telemetry snapshot emitted at the end of a Medallion batch execution."""

    batch_id: str
    batch_date: str
    silver_valid_record_count: int
    quarantine_record_count: int
    pipeline_latency_ms: float
    environment: str = "dev"
    pipeline_name: str = PIPELINE_NAME
    recorded_at_utc: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if self.silver_valid_record_count < 0:
            raise ValueError("silver_valid_record_count cannot be negative")
        if self.quarantine_record_count < 0:
            raise ValueError("quarantine_record_count cannot be negative")
        if self.pipeline_latency_ms < 0:
            raise ValueError("pipeline_latency_ms cannot be negative")

    @property
    def has_dlq_anomaly(self) -> bool:
        """Returns True when one or more corrupted records were routed to the Quarantine DLQ."""
        return self.quarantine_record_count > 0

    @property
    def total_processed_records(self) -> int:
        """Total records evaluated by the Silver Data Contract gate."""
        return self.silver_valid_record_count + self.quarantine_record_count


@dataclass(frozen=True)
class PartitionRegistrationSpec:
    """Specification for registering a Hive-style S3 Parquet partition in AWS Glue & Amazon Athena."""

    database_name: str
    table_name: str
    s3_bucket: str
    s3_table_prefix: str
    partition_values: dict[str, str]

    def __post_init__(self) -> None:
        if not self.database_name or not self.table_name:
            raise ValueError("database_name and table_name must be non-empty")
        if not self.partition_values:
            raise ValueError("partition_values must contain at least one partition key-value pair")

    @property
    def normalized_bucket(self) -> str:
        bucket = self.s3_bucket.strip()
        if bucket.startswith("s3://"):
            bucket = bucket[len("s3://") :]
        return bucket.strip("/")

    @property
    def normalized_prefix(self) -> str:
        return self.s3_table_prefix.strip("/")

    @property
    def partition_path_suffix(self) -> str:
        return "/".join(f"{k}={v}" for k, v in self.partition_values.items())

    @property
    def s3_partition_location(self) -> str:
        return (
            f"s3://{self.normalized_bucket}/{self.normalized_prefix}/"
            f"{self.partition_path_suffix}/"
        )


def build_cloudwatch_emf_payload(
    snapshot: PipelineTelemetrySnapshot,
    namespace: str = CLOUDWATCH_NAMESPACE,
) -> dict[str, Any]:
    """
    Builds a deterministic Amazon CloudWatch Embedded Metric Format (EMF) JSON payload
    for zero-latency structured log metric extraction.
    """
    timestamp_ms = int(snapshot.recorded_at_utc.timestamp() * 1000)
    return {
        "_aws": {
            "Timestamp": timestamp_ms,
            "CloudWatchMetrics": [
                {
                    "Namespace": namespace,
                    "Dimensions": [["Environment", "PipelineName"]],
                    "Metrics": [
                        {"Name": "QuarantineRecordCount", "Unit": "Count"},
                        {"Name": "SilverValidRecordCount", "Unit": "Count"},
                        {"Name": "PipelineLatencyMs", "Unit": "Milliseconds"},
                    ],
                }
            ],
        },
        "Environment": snapshot.environment,
        "PipelineName": snapshot.pipeline_name,
        "BatchId": snapshot.batch_id,
        "BatchDate": snapshot.batch_date,
        "QuarantineRecordCount": snapshot.quarantine_record_count,
        "SilverValidRecordCount": snapshot.silver_valid_record_count,
        "PipelineLatencyMs": round(float(snapshot.pipeline_latency_ms), 2),
        "HasDlqAnomaly": snapshot.has_dlq_anomaly,
    }


def build_put_metric_data_payload(
    snapshot: PipelineTelemetrySnapshot,
    namespace: str = CLOUDWATCH_NAMESPACE,
) -> dict[str, Any]:
    """
    Builds the exact keyword arguments dictionary expected by `boto3.client('cloudwatch').put_metric_data(...)`.
    """
    dimensions: list[dict[str, str]] = [
        {"Name": "Environment", "Value": snapshot.environment},
        {"Name": "PipelineName", "Value": snapshot.pipeline_name},
    ]
    return {
        "Namespace": namespace,
        "MetricData": [
            {
                "MetricName": "QuarantineRecordCount",
                "Dimensions": dimensions,
                "Timestamp": snapshot.recorded_at_utc,
                "Value": float(snapshot.quarantine_record_count),
                "Unit": "Count",
            },
            {
                "MetricName": "SilverValidRecordCount",
                "Dimensions": dimensions,
                "Timestamp": snapshot.recorded_at_utc,
                "Value": float(snapshot.silver_valid_record_count),
                "Unit": "Count",
            },
            {
                "MetricName": "PipelineLatencyMs",
                "Dimensions": dimensions,
                "Timestamp": snapshot.recorded_at_utc,
                "Value": round(float(snapshot.pipeline_latency_ms), 2),
                "Unit": "Milliseconds",
            },
        ],
    }


def build_glue_partition_payload(spec: PartitionRegistrationSpec) -> dict[str, Any]:
    """
    Builds the deterministic `boto3.client('glue').batch_create_partition(...)` payload
    registering a Hive-partitioned Parquet slice in the AWS Glue Data Catalog.
    """
    return {
        "DatabaseName": spec.database_name,
        "TableName": spec.table_name,
        "PartitionInputList": [
            {
                "Values": list(spec.partition_values.values()),
                "StorageDescriptor": {
                    "Location": spec.s3_partition_location,
                    "InputFormat": PARQUET_INPUT_FORMAT,
                    "OutputFormat": PARQUET_OUTPUT_FORMAT,
                    "Compressed": True,
                    "SerdeInfo": {
                        "Name": f"{spec.table_name}_parquet_serde",
                        "SerializationLibrary": PARQUET_SERDE_LIBRARY,
                        "Parameters": {
                            "serialization.format": "1",
                            "parquet.compression": "SNAPPY",
                        },
                    },
                },
                "Parameters": {
                    "classification": "parquet",
                    "registered_by": PIPELINE_NAME,
                },
            }
        ],
    }


def build_athena_partition_ddl(spec: PartitionRegistrationSpec) -> str:
    """
    Generates an idempotent Amazon Athena / Hive DDL statement registering a partition slice.
    """
    partition_clause = ", ".join(
        f"{key} = '{val}'" for key, val in spec.partition_values.items()
    )
    return (
        f"ALTER TABLE {spec.database_name}.{spec.table_name} "
        f"ADD IF NOT EXISTS PARTITION ({partition_clause}) "
        f"LOCATION '{spec.s3_partition_location}';"
    )


def lambda_bronze_event_handler(
    event: dict[str, Any],
    context: Any = None,
) -> dict[str, Any]:
    """
    Lightweight AWS Lambda handler invoked by Amazon EventBridge upon S3 Bronze `Object Created` events.
    Extracts the S3 bucket and object key deterministically for downstream Silver contract validation.
    """
    detail: dict[str, Any] = event.get("detail", {})
    bucket_name: str = detail.get("bucket", {}).get("name", "")
    object_key: str = detail.get("object", {}).get("key", "")
    return {
        "status": "TRIGGERED",
        "source": event.get("source", "aws.s3"),
        "detail_type": event.get("detail-type", "Object Created"),
        "bronze_bucket": bucket_name,
        "object_key": object_key,
        "s3_uri": f"s3://{bucket_name}/{object_key}" if bucket_name and object_key else "",
    }
