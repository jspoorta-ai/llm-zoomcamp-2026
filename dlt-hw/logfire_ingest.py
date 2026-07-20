import os
from datetime import datetime, timedelta, timezone
from typing import Any

import dlt
from logfire.query_client import LogfireQueryClient


def normalize_rows(columns: list[dict[str, Any]], rows: list[list[Any]]) -> list[dict[str, Any]]:
    """Convert Logfire column metadata and row values into a list of dicts."""
    if not columns:
        return []

    field_names = [column["name"] for column in columns]
    return [dict(zip(field_names, row)) for row in rows]


def fetch_logfire_rows(
    read_token: str | None = None,
    sql: str = "SELECT * FROM records_all",
    lookback_days: int = 30,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Fetch rows from Logfire and return both the raw column metadata and normalized rows."""
    token = read_token or os.getenv("LOGFIRE_READ_TOKEN")
    if not token:
        raise ValueError("LOGFIRE_READ_TOKEN is not set")

    client = LogfireQueryClient(read_token=token)
    min_timestamp = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    result = client.query_json_rows(sql=sql, min_timestamp=min_timestamp, limit=1000)

    return result["columns"], normalize_rows(result["columns"], result["rows"])


@dlt.source
def logfire_source(read_token: str | None = None):
    """Create a dlt source that reads rows from the Logfire records view."""
    token = read_token or os.getenv("LOGFIRE_READ_TOKEN")
    if not token:
        raise ValueError("LOGFIRE_READ_TOKEN is not set")

    @dlt.resource(name="records_all", write_disposition="append")
    def records_all():
        client = LogfireQueryClient(read_token=token)
        min_timestamp = datetime.now(timezone.utc) - timedelta(days=30)
        result = client.query_json_rows(
            sql="SELECT * FROM records_all",
            min_timestamp=min_timestamp,
            limit=1000,
        )
        for row in normalize_rows(result["columns"], result["rows"]):
            yield row

    yield records_all


def load_logfire_to_duckdb(
    read_token: str | None = None,
    pipeline_name: str = "logfire_pipeline",
    dataset_name: str = "logfire_data",
) -> dlt.pipeline:
    """Fetch Logfire data through dlt and write it into a DuckDB destination."""
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="duckdb",
        dataset_name=dataset_name,
    )
    load_info = pipeline.run(logfire_source(read_token=read_token))
    print(load_info)
    return pipeline


if __name__ == "__main__":
    load_logfire_to_duckdb()
