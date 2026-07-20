import os
from pathlib import Path
from typing import Any

import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dotenv import load_dotenv

load_dotenv()


def normalize_rows(columns: list[dict[str, Any]], rows: list[list[Any]]) -> list[dict[str, Any]]:
    """Convert Logfire column metadata and row values into a list of dicts."""
    if not columns:
        return []

    field_names = [column["name"] for column in columns]
    return [dict(zip(field_names, row)) for row in rows]


def rows_from_columnar_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert Logfire's column-oriented query response into row dicts.

    The Logfire Query API returns `{"columns": [{"name", "values": [...]}, ...]}`
    (one values array per column) rather than a separate row-oriented `rows` list,
    so this transposes it before handing off to `normalize_rows`.
    """
    columns = payload.get("columns", [])
    rows = list(zip(*(column.get("values", []) for column in columns))) if columns else []
    return normalize_rows(columns, rows)


@dlt.source(name="logfire")
def logfire_source(
    access_token: str = None,
    base_url: str = dlt.config.value,
    sql: str = "SELECT * FROM records_all",
    lookback_days: int = 30,
    limit: int = 1000,
) -> Any:
    """Load rows from the Logfire Query API into a `records` resource.

    Args:
        access_token: Logfire read token. Falls back to the `LOGFIRE_READ_TOKEN`
            environment variable (e.g. from a `.env` file), then to secrets.toml
            (`sources.logfire.access_token`).
        base_url: Logfire Query API base URL, e.g.
            "https://logfire-eu.pydantic.dev/v1/". Auto-loaded from config.toml
            (`sources.logfire.base_url`).
        sql: SQL query executed against Logfire's `records_all` view.
        lookback_days: How many days back `min_timestamp` should start.
        limit: Max rows returned per request (Logfire caps this at 10000).
    """
    token = (
        access_token
        or os.getenv("LOGFIRE_READ_TOKEN")
        or dlt.secrets.get("sources.logfire.access_token")
    )
    if not token:
        raise ValueError(
            "Logfire access token not found. Set LOGFIRE_READ_TOKEN in .env, "
            "or sources.logfire.access_token in secrets.toml."
        )

    client = RESTClient(base_url=base_url, auth=BearerTokenAuth(token))

    @dlt.resource(name="records", write_disposition="replace")
    def records():
        min_timestamp = pendulum.now("UTC").subtract(days=lookback_days).to_iso8601_string()
        response = client.get(
            "query",
            params={"sql": sql, "min_timestamp": min_timestamp, "limit": limit},
        )
        response.raise_for_status()
        payload = response.json()
        yield rows_from_columnar_payload(payload)

    yield records


def load_logfire_to_duckdb(
    pipeline_name: str = "logfire_pipeline",
    dataset_name: str = "agent_traces",
) -> dlt.Pipeline:
    """Fetch Logfire query results through dlt and write them into a DuckDB destination."""
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.duckdb(
            credentials=str(Path(__file__).parent / "logfire.duckdb")
        ),
        dataset_name=dataset_name,
    )
    load_info = pipeline.run(logfire_source())
    print(load_info)
    return pipeline


if __name__ == "__main__":
    load_logfire_to_duckdb()
