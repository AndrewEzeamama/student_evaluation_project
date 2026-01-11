from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from src.pipeline.ingest import ingest_excel
from src.pipeline.transform import transform_silver
from src.quality.ge_runner import run_quality_checks
from src.pipeline.analytics import build_gold

# -------------------------------
# Default DAG arguments
# -------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -------------------------------
# DAG Definition
# -------------------------------
with DAG(
    dag_id="student_evaluation_pipeline",
    default_args=default_args,
    description="Student Evaluation Analytics Pipeline (Bronze → Gold)",
    start_date=days_ago(1),
    schedule_interval=None,      # Manual / triggered execution
    catchup=False,
    tags=["assessment", "analytics", "duckdb"],
) as dag:

    bronze_ingest = PythonOperator(
        task_id="bronze_ingest",
        python_callable=ingest_excel,
        op_kwargs={"run_id": "{{ run_id }}"},
        sla=timedelta(minutes=2),
    )

    silver_transform = PythonOperator(
        task_id="silver_transform",
        python_callable=transform_silver,
        op_kwargs={"run_id": "{{ run_id }}"},
        sla=timedelta(minutes=5),
    )

    data_quality = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_quality_checks,
        op_kwargs={"run_id": "{{ run_id }}"},
        sla=timedelta(minutes=3),
    )

    gold_build = PythonOperator(
        task_id="gold_build",
        python_callable=build_gold,
        op_kwargs={"run_id": "{{ run_id }}"},
        sla=timedelta(minutes=5),
    )

    # Task dependencies
    bronze_ingest >> silver_transform >> data_quality >> gold_build
