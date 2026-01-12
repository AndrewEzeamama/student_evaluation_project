import pandas as pd
from src.core.config import SILVER_DIR, GOLD_DIR
from src.core.logging import get_logger
from src.core.audit import write_audit_record
from src.core.duckdb_conn import get_duckdb_connection

logger = get_logger("GOLD_ANALYTICS")

def build_gold_layer(run_id: str):
    """
    Build analytics-ready Gold layer (Dimensions only).
    Dataset contains no transactional facts.
    """

    try:
        con = get_duckdb_connection()

        # -------------------------
        # Load Silver datasets
        # -------------------------
        students = pd.read_parquet(SILVER_DIR / "students.parquet")
        teachers = pd.read_parquet(SILVER_DIR / "teachers.parquet")
        schools = pd.read_parquet(SILVER_DIR / "schools.parquet")
        grading_groups = pd.read_parquet(SILVER_DIR / "grading_groups.parquet")
        tests = pd.read_parquet(SILVER_DIR / "tests.parquet")
        test_details = pd.read_parquet(SILVER_DIR / "test_details.parquet")

        # -------------------------
        # Dimensions (Surrogate keys)
        # -------------------------
        dim_student = students.assign(student_key=lambda df: df.index + 1)
        dim_teacher = teachers.assign(teacher_key=lambda df: df.index + 1)
        dim_school = schools.assign(school_key=lambda df: df.index + 1)

        dim_grading_group = grading_groups.assign(
            grading_group_key=lambda df: df.index + 1
        )

        fact_test = tests.assign(test_key=lambda df: df.index + 1)

        dim_test_details = test_details.assign(
            test_detail_key=lambda df: df.index + 1
        )

        # -------------------------
        # DuckDB materialization
        # -------------------------
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")

        tables = [
            "dim_student",
            "dim_teacher",
            "dim_school",
            "dim_grading_group",
            "fact_test",
            "dim_test_details",
        ]

        for table in tables:
            con.execute(f"DROP TABLE IF EXISTS gold.{table}")

        con.register("dim_student_df", dim_student)
        con.register("dim_teacher_df", dim_teacher)
        con.register("dim_school_df", dim_school)
        con.register("dim_grading_group_df", dim_grading_group)
        con.register("fact_test_df", fact_test)
        con.register("dim_test_details_df", dim_test_details)

        con.execute("CREATE TABLE gold.dim_student AS SELECT * FROM dim_student_df")
        con.execute("CREATE TABLE gold.dim_teacher AS SELECT * FROM dim_teacher_df")
        con.execute("CREATE TABLE gold.dim_school AS SELECT * FROM dim_school_df")
        con.execute("CREATE TABLE gold.dim_grading_group AS SELECT * FROM dim_grading_group_df")
        con.execute("CREATE TABLE gold.fact_test AS SELECT * FROM fact_test_df")
        con.execute("CREATE TABLE gold.dim_test_details AS SELECT * FROM dim_test_details_df")

        # -------------------------
        # Parquet outputs
        # -------------------------
        dim_student.to_parquet(GOLD_DIR / "dim_student.parquet", index=False)
        dim_teacher.to_parquet(GOLD_DIR / "dim_teacher.parquet", index=False)
        dim_school.to_parquet(GOLD_DIR / "dim_school.parquet", index=False)
        dim_grading_group.to_parquet(GOLD_DIR / "dim_grading_group.parquet", index=False)
        fact_test.to_parquet(GOLD_DIR / "fact_test.parquet", index=False)
        dim_test_details.to_parquet(GOLD_DIR / "dim_test_details.parquet", index=False)

        # -------------------------
        # Audit
        # -------------------------
        write_audit_record(
            run_id=run_id,
            stage="gold_materialization",
            status="SUCCESS",
            row_count=(
                len(dim_student)
                + len(dim_teacher)
                + len(dim_school)
                + len(dim_grading_group)
                + len(fact_test)
                + len(dim_test_details)
            )
        )

        logger.info("Gold layer successfully materialized (dimensions only)")

    except Exception as e:
        logger.exception("Gold layer failed")

        write_audit_record(
            run_id=run_id,
            stage="gold_materialization",
            status="FAILED",
            error_message=str(e)
        )
        raise
