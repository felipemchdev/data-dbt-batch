from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lakehouse_olist_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline batch Lakehouse (Bronze, Silver, Gold) para dataset Olist",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "olist", "batch"],
) as dag:
    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command="cd /app && python -m src.ingest.check_raw_files",
    )

    bronze_load = BashOperator(
        task_id="bronze_load",
        bash_command="cd /app && python -m src.ingest.load_bronze",
    )

    quality_check = BashOperator(
        task_id="quality_check",
        bash_command="cd /app && python -m src.ge.validate_bronze",
    )

    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command="cd /app/dbt && /home/airflow/.local/bin/dbt run --select staging --profiles-dir /app/dbt",
    )

    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command="cd /app/dbt && /home/airflow/.local/bin/dbt run --select marts --profiles-dir /app/dbt",
    )

    dbt_tests = BashOperator(
        task_id="dbt_tests",
        bash_command="cd /app/dbt && /home/airflow/.local/bin/dbt test --profiles-dir /app/dbt",
    )

    analytics = BashOperator(
        task_id="analytics",
        bash_command="cd /app && python -m src.analytics.generate_report",
    )

    ingest_raw >> bronze_load >> quality_check >> dbt_silver >> dbt_gold >> dbt_tests >> analytics
