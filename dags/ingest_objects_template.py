"""Airflow DAG: CSV → Postgres (stage + merge) template

GOVERNANCE
- DAG files should be THIN orchestrators.
- Business logic should be kept small and explicit here; if it grows, move into a dedicated module
  (e.g., src/ or include/ in a real repo) and import.
- No hard-coded secrets in code. Use Airflow Connections / Variables.
- Prefer snake_case column names in warehouse tables.

HOW TO ADAPT FOR A NEW OBJECT
- Update OBJECT_CONFIG below: source_url, file_basename, target_table, staging_table, and column_map.
- column_map is a list of mappings:
    {"source": <csv header>, "target": <db column>, "type": <postgres type>}

This DAG is intentionally "boring": download → stage (COPY) → merge (upsert).
"""

import datetime
import os
from dataclasses import dataclass
from typing import List

import pendulum
import requests
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dataclass(frozen=True)
class ColumnMap:
    source: str  # CSV header
    target: str  # Postgres column name (snake_case)
    type: str    # Postgres type for target table


# -----------------------------------------------------------------------------
# OBJECT CONFIG (this is the only section you should change per dataset/object)
# -----------------------------------------------------------------------------
OBJECT_CONFIG = {
    # NOTE: rename this when you decide the new “object” name.
    "dag_id": "ingest_object_template",

    # Run daily at midnight UTC (adjust to your needs)
    "schedule": "0 0 * * *",

    # Connection id configured in Airflow UI (Admin → Connections)
    "postgres_conn_id": "tutorial_pg_conn",

    # Source CSV
    "source_url": "https://raw.githubusercontent.com/apache/airflow/main/airflow-core/docs/tutorial/pipeline_example.csv",

    # Local landing path inside the Airflow environment
    "landing_dir": "/opt/airflow/dags/files",
    "file_basename": "object.csv",

    # Target + staging tables
    "target_table": "employees",        # <-- replace with your real target
    "staging_table": "employees_stage", # <-- replace with your real staging

    # Primary key column (in TARGET)
    "primary_key": "serial_number",

    # Mapping from CSV headers → warehouse columns
    # NOTE: the current mapping matches the tutorial CSV.
    "column_map": [
        ColumnMap(source="Serial Number", target="serial_number", type="NUMERIC"),
        ColumnMap(source="Company Name", target="company_name", type="TEXT"),
        ColumnMap(source="Employee Markme", target="employee_markme", type="TEXT"),
        ColumnMap(source="Description", target="description", type="TEXT"),
        ColumnMap(source="Leave", target="leave", type="INTEGER"),
    ],
}


def _q_ident(name: str) -> str:
    """Quote an identifier (table/column) safely for Postgres."""
    # Very small helper; for full safety you’d use psycopg2.sql, but this works for controlled names.
    return '"' + name.replace('"', '""') + '"'


def _render_target_ddl(target_table: str, cols: List[ColumnMap], pk: str) -> str:
    col_lines = []
    for c in cols:
        col_lines.append(f"{_q_ident(c.target)} {c.type}")
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {_q_ident(target_table)} (\n"
        + ",\n".join(col_lines)
        + f",\nPRIMARY KEY ({_q_ident(pk)})\n);"
    )
    return ddl


def _render_stage_ddl(stage_table: str, cols: List[ColumnMap]) -> str:
    # Stage table is TEXT-only to make ingestion resilient to bad typing.
    # Casting happens during merge.
    col_lines = []
    for c in cols:
        col_lines.append(f"{_q_ident(c.source)} TEXT")
    ddl = (
        f"DROP TABLE IF EXISTS {_q_ident(stage_table)};\n"
        f"CREATE TABLE {_q_ident(stage_table)} (\n"
        + ",\n".join(col_lines)
        + "\n);"
    )
    return ddl


def _render_merge_sql(target_table: str, stage_table: str, cols: List[ColumnMap], pk: str) -> str:
    # Build SELECT list from stage -> target with casts.
    select_exprs = []
    for c in cols:
        # Stage col is quoted by source header; target is snake_case
        select_exprs.append(
            f"CAST(s.{_q_ident(c.source)} AS {c.type}) AS {_q_ident(c.target)}"
        )

    insert_cols = ", ".join(_q_ident(c.target) for c in cols)
    select_cols = ", ".join(select_exprs)

    # Update all columns except PK
    update_sets = []
    for c in cols:
        if c.target == pk:
            continue
        update_sets.append(f"{_q_ident(c.target)} = EXCLUDED.{_q_ident(c.target)}")

    update_sql = ",\n    ".join(update_sets) if update_sets else ""

    sql = f"""
INSERT INTO {_q_ident(target_table)} ({insert_cols})
SELECT {select_cols}
FROM {_q_ident(stage_table)} s
ON CONFLICT ({_q_ident(pk)}) DO UPDATE
SET
    {update_sql};
""".strip()

    return sql


@dag(
    dag_id=OBJECT_CONFIG["dag_id"],
    schedule=OBJECT_CONFIG["schedule"],
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["ingest", "template"],
)
def ingest_object_template():
    cols: List[ColumnMap] = OBJECT_CONFIG["column_map"]

    create_target_table = SQLExecuteQueryOperator(
        task_id="create_target_table",
        conn_id=OBJECT_CONFIG["postgres_conn_id"],
        sql=_render_target_ddl(
            target_table=OBJECT_CONFIG["target_table"],
            cols=cols,
            pk=OBJECT_CONFIG["primary_key"],
        ),
    )

    create_stage_table = SQLExecuteQueryOperator(
        task_id="create_stage_table",
        conn_id=OBJECT_CONFIG["postgres_conn_id"],
        sql=_render_stage_ddl(
            stage_table=OBJECT_CONFIG["staging_table"],
            cols=cols,
        ),
    )

    @task
    def extract_and_stage() -> dict:
        """Download CSV to local disk and COPY into a TEXT-only staging table."""
        landing_dir = OBJECT_CONFIG["landing_dir"]
        os.makedirs(landing_dir, exist_ok=True)

        data_path = os.path.join(landing_dir, OBJECT_CONFIG["file_basename"])
        url = OBJECT_CONFIG["source_url"]

        resp = requests.get(url, timeout=60)
        resp.raise_for_status()

        with open(data_path, "w", encoding="utf-8") as f:
            f.write(resp.text)

        postgres_hook = PostgresHook(postgres_conn_id=OBJECT_CONFIG["postgres_conn_id"])
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # COPY expects the staging columns to match the CSV HEADER names.
        copy_sql = (
            f"COPY {_q_ident(OBJECT_CONFIG['staging_table'])} "
            "FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'"
        )

        with open(data_path, "r", encoding="utf-8") as f:
            cur.copy_expert(copy_sql, f)

        conn.commit()

        return {
            "data_path": data_path,
            "source_url": url,
        }

    @task
    def merge_to_target() -> dict:
        """Upsert staged rows into the target table with deterministic typing."""
        merge_sql = _render_merge_sql(
            target_table=OBJECT_CONFIG["target_table"],
            stage_table=OBJECT_CONFIG["staging_table"],
            cols=cols,
            pk=OBJECT_CONFIG["primary_key"],
        )

        postgres_hook = PostgresHook(postgres_conn_id=OBJECT_CONFIG["postgres_conn_id"])
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        cur.execute(merge_sql)
        rowcount = cur.rowcount  # note: rowcount semantics vary for upsert
        conn.commit()

        return {
            "target_table": OBJECT_CONFIG["target_table"],
            "staging_table": OBJECT_CONFIG["staging_table"],
            "rowcount": rowcount,
        }

    [create_target_table, create_stage_table] >> extract_and_stage() >> merge_to_target()


dag = ingest_object_template()