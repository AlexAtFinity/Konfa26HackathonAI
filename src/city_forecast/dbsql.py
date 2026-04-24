from __future__ import annotations

from dataclasses import dataclass
import json
import os
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class DbSqlConfig:
    warehouse_id: str


def _require_databricks_sdk():
    try:
        from databricks.sdk import WorkspaceClient  # noqa: F401
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "databricks-sdk is required. Run: UV_CACHE_DIR=/tmp/uv-cache uv sync"
        ) from exc


def get_workspace_client():
    _require_databricks_sdk()
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def execute_sql(sql: str, *, warehouse_id: str) -> None:
    client = get_workspace_client()
    client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
    )


def query_to_pandas(sql: str, *, warehouse_id: str) -> pd.DataFrame:
    client = get_workspace_client()
    from databricks.sdk.service.sql import Disposition, Format

    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )
    if resp.manifest is None or resp.result is None:
        return pd.DataFrame()

    columns = [c.name for c in resp.manifest.schema.columns]
    df = pd.DataFrame(resp.result.data_array or [], columns=columns)

    # Basic dtype coercion (JSON_ARRAY returns everything as strings).
    type_by_col = {c.name: (c.type_name.value if c.type_name else "") for c in resp.manifest.schema.columns}
    for col, type_name in type_by_col.items():
        t = (type_name or "").upper()
        if col not in df.columns:
            continue
        if t in {"INT", "LONG", "SHORT", "BYTE", "BIGINT", "SMALLINT", "TINYINT"}:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif t in {"DOUBLE", "FLOAT", "DECIMAL"}:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif t in {"BOOLEAN"}:
            df[col] = df[col].map(lambda x: None if x is None else str(x).lower() == "true")
        elif t in {"DATE"}:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    return df


def has_databricks_creds() -> bool:
    # WorkspaceClient reads credentials from env or ~/.databrickscfg (via Databricks CLI configure).
    # We just check for some common envs to decide default execution mode.
    env_markers = ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"]
    return any(os.getenv(k) for k in env_markers) or os.path.exists(os.path.expanduser("~/.databrickscfg"))


def write_dataframe_as_values_table(
    df: pd.DataFrame,
    *,
    warehouse_id: str,
    full_table_name: str,
    column_types_sql: dict[str, str],
) -> None:
    if df.empty:
        raise ValueError("Refusing to write empty dataframe to Databricks.")

    cols = list(column_types_sql.keys())
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Dataframe missing required columns: {missing}")

    def sql_literal(value: Any, sql_type: str) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "NULL"
        if sql_type.upper() in ("STRING", "VARCHAR"):
            s = str(value).replace("'", "''")
            return f"'{s}'"
        if sql_type.upper() == "DATE":
            s = str(value)[:10]
            return f"DATE '{s}'"
        if sql_type.upper() in ("DOUBLE", "FLOAT", "DECIMAL", "BIGINT", "INT"):
            return str(float(value)) if sql_type.upper() in ("DOUBLE", "FLOAT") else str(int(value))
        # default: stringify
        return f"'{json.dumps(value)}'"

    values_sql_rows: list[str] = []
    for _, row in df[cols].iterrows():
        values_sql_rows.append(
            "(" + ", ".join(sql_literal(row[c], column_types_sql[c]) for c in cols) + ")"
        )

    create_sql = (
        f"CREATE OR REPLACE TABLE {full_table_name} "
        f"AS SELECT * FROM VALUES\n  "
        + ",\n  ".join(values_sql_rows)
        + "\nAS t("
        + ", ".join(cols)
        + ")"
    )

    execute_sql(create_sql, warehouse_id=warehouse_id)
