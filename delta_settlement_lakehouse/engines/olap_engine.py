"""Vectorized OLAP Transformation Engine powered by DuckDB over PyArrow / Delta datasets."""

from datetime import datetime, timezone
from decimal import Decimal
import duckdb
import pyarrow as pa

from delta_settlement_lakehouse.domain.models import CounterpartyExposureSummary, DailyReconciliationSummary


class OlapEngine:
    """Performs in-memory, zero-copy analytical aggregations over Arrow tables using DuckDB."""

    def __init__(self):
        self.con = duckdb.connect(database=":memory:")

    def compute_daily_reconciliation(
        self,
        silver_table: pa.Table,
        settlement_date: str,
    ) -> list[DailyReconciliationSummary]:
        if silver_table.num_rows == 0:
            return []

        # DuckDB queries Arrow table in-memory directly by variable name
        query = """
        SELECT
            settlement_date,
            settlement_currency as currency,
            CAST(SUM(settlement_amount) AS DOUBLE) as total_settled_amount,
            COUNT(*) as transaction_count,
            CAST(SUM(settlement_amount * 0.0015) AS DOUBLE) as total_fees_collected,
            CAST(AVG(booked_fx_rate) AS DOUBLE) as avg_fx_rate
        FROM silver_table
        WHERE settlement_date = ?
        GROUP BY settlement_date, settlement_currency
        ORDER BY currency ASC
        """
        results = self.con.execute(query, [settlement_date]).fetchall()
        now = datetime.now(timezone.utc)

        summaries: list[DailyReconciliationSummary] = []
        for row in results:
            summaries.append(
                DailyReconciliationSummary(
                    settlement_date=str(row[0]),
                    currency=str(row[1]),
                    total_settled_amount=Decimal(str(round(row[2], 2))),
                    transaction_count=int(row[3]),
                    total_fees_collected=Decimal(str(round(row[4], 2))),
                    avg_fx_rate=Decimal(str(round(row[5], 6))),
                    computed_at_utc=now,
                )
            )

        return summaries

    def compute_counterparty_exposure(
        self,
        silver_table: pa.Table,
        as_of_date: str,
    ) -> list[CounterpartyExposureSummary]:
        if silver_table.num_rows == 0:
            return []

        query = """
        SELECT
            sender_bic,
            receiver_bic,
            settlement_currency as currency,
            CAST(SUM(settlement_amount) AS DOUBLE) as net_volume,
            CAST(SUM(settlement_amount) AS DOUBLE) as gross_volume,
            COUNT(*) as settled_tx_count
        FROM silver_table
        WHERE settlement_date = ?
        GROUP BY sender_bic, receiver_bic, settlement_currency
        ORDER BY net_volume DESC
        """
        results = self.con.execute(query, [as_of_date]).fetchall()

        exposures: list[CounterpartyExposureSummary] = []
        for row in results:
            exposures.append(
                CounterpartyExposureSummary(
                    sender_bic=str(row[0]),
                    receiver_bic=str(row[1]),
                    currency=str(row[2]),
                    net_volume=Decimal(str(round(row[3], 2))),
                    gross_volume=Decimal(str(round(row[4], 2))),
                    settled_tx_count=int(row[5]),
                    as_of_date=as_of_date,
                )
            )

        return exposures
