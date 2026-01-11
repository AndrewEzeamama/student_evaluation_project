import pandas as pd
from datetime import datetime

from src.core.config import SILVER_DIR, GOLD_DIR
from src.core.logging import get_logger
from src.core.audit import write_audit_record
from src.core.duckdb_conn import get_duckdb_connection

logger = get_logger("GOLD_ANALYTICS")


def build_gold_layer(run_id: str):
    """
    Build analytics-ready Gold layer (Star Schema) and persist to DuckDB + Parquet.
    """

    try:
        con = get_duckdb_connection()

        # -------------------------
        # Load Silver datasets
        # -------------------------
        students = pd.read_parquet(SILVER_DIR / "students.parquet")
        teachers = pd.read_parquet(SILVER_DIR / "teachers.parquet")
        schools = pd.read_parquet(SILVER_DIR / "schools.parquet")   
        tests = pd.read_parquet(SILVER_DIR / "tests.parquet")
        test_details = pd.read_parquet(SILVER_DIR / "test_details.parquet")
        grading_groups = pd.read_parquet(SILVER_DIR / "grading_groups.parquet")

        # -------------------------
        # Dimensions
        # -------------------------
        dim_student = students.assign(student_key=lambda df: df.index + 1)
        dim_teacher = teachers.assign(teacher_key=lambda df: df.index + 1)
        dim_school = schools.assign(school_key=lambda df: df.index + 1)
        dim_test = tests.assign(test_key=lambda df: df.index + 1)
        dim_grading_group = grading_groups.assign(
            grading_group_key=lambda df: df.index + 1
        )

        # -------------------------
        # Fact table
        # Grain: one test result
        # -------------------------
        fact_test_results = (
            test_details
            .merge(dim_test, on="test_id", how="left")
            .merge(dim_grading_group, on="grading_group_id", how="left")
            .loc[:, [
                "test_key",
                "grading_group_key",
                "score",
                "exam_date"
            ]]
        )

        # -------------------------
        # DuckDB Materialization
        # -------------------------
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")

        con.execute("DROP TABLE IF EXISTS gold.dim_student")
        con.execute("DROP TABLE IF EXISTS gold.dim_teacher")
        con.execute("DROP TABLE IF EXISTS gold.dim_school")
        con.execute("DROP TABLE IF EXISTS gold.dim_test")
        con.execute("DROP TABLE IF EXISTS gold.dim_grading_group")
        con.execute("DROP TABLE IF EXISTS gold.fact_test_results")

        con.register("dim_student_df", dim_student)
        con.register("dim_teacher_df", dim_teacher)
        con.register("dim_school_df", dim_school)
        con.register("dim_test_df", dim_test)
        con.register("dim_grading_group_df", dim_grading_group)
        con.register("fact_df", fact_test_results)

        con.execute("CREATE TABLE gold.dim_student AS SELECT * FROM dim_student_df")
        con.execute("CREATE TABLE gold.dim_teacher AS SELECT * FROM dim_teacher_df")
        con.execute("CREATE TABLE gold.dim_school AS SELECT * FROM dim_school_df")
        con.execute("CREATE TABLE gold.dim_test AS SELECT * FROM dim_test_df")
        con.execute("CREATE TABLE gold.dim_grading_group AS SELECT * FROM dim_grading_group_df")
        con.execute("CREATE TABLE gold.fact_test_results AS SELECT * FROM fact_df")

        # -------------------------
        # Parquet Output
        # -------------------------
        dim_student.to_parquet(GOLD_DIR / "dim_student.parquet", index=False)
        dim_teacher.to_parquet(GOLD_DIR / "dim_teacher.parquet", index=False)
        dim_school.to_parquet(GOLD_DIR / "dim_school.parquet", index=False)
        dim_test.to_parquet(GOLD_DIR / "dim_test.parquet", index=False)
        dim_grading_group.to_parquet(GOLD_DIR / "dim_grading_group.parquet", index=False)
        fact_test_results.to_parquet(
            GOLD_DIR / "fact_test_results.parquet", index=False
        )

        # -------------------------
        # Audit
        # -------------------------
        write_audit_record(
            run_id=run_id,
            stage="gold_materialization",
            status="SUCCESS",
            row_count=len(fact_test_results)
        )

        logger.info("Gold layer successfully materialized")

    except Exception as e:
        logger.exception("Gold layer failed")

        write_audit_record(
            run_id=run_id,
            stage="gold_materialization",
            status="FAILED",
            error_message=str(e)
        )
        raise
