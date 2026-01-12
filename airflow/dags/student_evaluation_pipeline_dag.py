from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from src.pipeline.ingest import ingest_excel
from src.pipeline.transform import transform_silver
from src.pipeline.analytics import build_gold_layer
from src.data_quality.ge_check import run_ge_checks   

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="student_etl_pipeline",
    default_args=default_args,
    description="Student Evaluation Analytics Pipeline (Bronze → Gold)",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["assessment", "analytics", "duckdb", "data-quality"],
) as dag:

    bronze_ingest = PythonOperator(
        task_id="bronze_ingest",
        python_callable=ingest_excel,
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    silver_transform = PythonOperator(
        task_id="silver_transform",
        python_callable=transform_silver,
        op_kwargs={"run_id": "{{ run_id }}"},
    )
    
    data_quality_ge = PythonOperator(
    task_id="data_quality_ge",
    python_callable=run_ge_checks,
    op_kwargs={"run_id": "{{ run_id }}"},
    # sla=timedelta(minutes=3),
    )

    gold_task = PythonOperator(
        task_id="gold_materialization",
        python_callable=build_gold_layer,
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    bronze_ingest >> silver_transform >> data_quality_ge >> gold_task







# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago
# from datetime import timedelta

# from src.pipeline.ingest import ingest_excel
# from src.pipeline.transform import transform_silver
# from src.pipeline.analytics import build_gold_layer

# # -------------------------------
# # Default DAG arguments
# # -------------------------------
# default_args = {
#     "owner": "data-engineering",
#     "depends_on_past": False,
#     "retries": 0,
#     "retry_delay": timedelta(minutes=5),
# }

# # -------------------------------
# # DAG Definition
# # -------------------------------
# with DAG(
#     dag_id="student_evaluation_pipeline",
#     default_args=default_args,
#     description="Student Evaluation Analytics Pipeline (Bronze → Gold)",
#     start_date=days_ago(1),
#     schedule_interval=None,      # Manual / triggered execution
#     catchup=False,
#     tags=["assessment", "analytics", "duckdb"],
# ) as dag:

#     bronze_ingest = PythonOperator(
#         task_id="bronze_ingest",
#         python_callable=ingest_excel,
#         op_kwargs={"run_id": "{{ run_id }}"},
#         sla=timedelta(minutes=2),
#     )

#     silver_transform = PythonOperator(
#         task_id="silver_transform",
#         python_callable=transform_silver,
#         op_kwargs={"run_id": "{{ run_id }}"},
#         sla=timedelta(minutes=5),
#     )
    
#     gold_task = PythonOperator(
#     task_id="gold_materialization",
#     python_callable=build_gold_layer,
#     op_kwargs={"run_id": "{{ run_id }}"},
#     )

#     # Task dependencies
#     bronze_ingest >> silver_transform >> gold_task
