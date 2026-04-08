from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="olist_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "dbt"],
) as dag:
    ingest = BashOperator(
        task_id="ingest",
        bash_command="echo ingest placeholder",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /app/dbt && /home/airflow/.local/bin/dbt run --profiles-dir /app/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /app/dbt && /home/airflow/.local/bin/dbt test --profiles-dir /app/dbt",
    )

    ingest >> dbt_run >> dbt_test
