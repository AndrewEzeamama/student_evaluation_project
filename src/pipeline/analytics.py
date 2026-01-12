import pandas as pd
from pathlib import Path

from src.core.config import SILVER_DIR, GOLD_DIR
from src.core.logging import get_logger
from src.core.audit import write_audit_record
from src.core.duckdb_conn import get_duckdb_connection

logger = get_logger("GOLD_ANALYTICS")


def build_gold_layer(run_id: str):
    """
    Build analytics-ready Gold layer.
    Produces:
      - Dimensions: student, teacher, school, grading_group
      - Fact table: fact_tests
    """

    try:
        logger.info("Starting Gold layer materialization")

        # ------------------------------------------------------------------
        # Load Silver datasets
        # ------------------------------------------------------------------
        students = pd.read_parquet(SILVER_DIR / "students.parquet")
        teachers = pd.read_parquet(SILVER_DIR / "teachers.parquet")
        schools = pd.read_parquet(SILVER_DIR / "schools.parquet")
        grading_groups = pd.read_parquet(SILVER_DIR / "grading_groups.parquet")
        tests = pd.read_parquet(SILVER_DIR / "tests.parquet")
        test_details = pd.read_parquet(SILVER_DIR / "test_details.parquet")

        # ------------------------------------------------------------------
        # Dimensions (surrogate keys, no column loss)
        # ------------------------------------------------------------------
        dim_student = students.copy()
        dim_student["student_key"] = dim_student.index + 1

        dim_teacher = teachers.copy()
        dim_teacher["teacher_key"] = dim_teacher.index + 1

        dim_school = schools.copy()
        dim_school["school_key"] = dim_school.index + 1

        dim_grading_group = grading_groups.copy()
        dim_grading_group["grading_group_key"] = dim_grading_group.index + 1

        # ------------------------------------------------------------------
        # Fact table (ONLY required fact)
        # ---------------------------------------------------------------------
        
        required_col = "assessement_level_id"
        if required_col not in dim_grading_group.columns:
            raise ValueError(
                f"{required_col} not found in grading_groups.parquet. "
                f"Available columns: {list(dim_grading_group.columns)}"
            )

        fact_tests = (
            tests
            .merge(
                test_details,
                on="assessment_type",
                how="left"
            )
            .merge(
                # dim_grading_group,
                dim_grading_group[["assessement_level_id", "grading_group_key"]],
                on="assessement_level_id",
                how="left"
            )
        )

        fact_tests["fact_test_key"] = fact_tests.index + 1

        # ------------------------------------------------------------------
        # DuckDB materialization
        # ------------------------------------------------------------------
        con = get_duckdb_connection()
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")

        for table in [
            "fact_tests",
            "dim_student",
            "dim_teacher",
            "dim_school",
            "dim_grading_group",
        ]:
            con.execute(f"DROP TABLE IF EXISTS gold.{table}")

        con.register("fact_tests_df", fact_tests)
        con.register("dim_student_df", dim_student)
        con.register("dim_teacher_df", dim_teacher)
        con.register("dim_school_df", dim_school)
        con.register("dim_grading_group_df", dim_grading_group)

        con.execute("CREATE TABLE gold.fact_tests AS SELECT * FROM fact_tests_df")
        con.execute("CREATE TABLE gold.dim_student AS SELECT * FROM dim_student_df")
        con.execute("CREATE TABLE gold.dim_teacher AS SELECT * FROM dim_teacher_df")
        con.execute("CREATE TABLE gold.dim_school AS SELECT * FROM dim_school_df")
        con.execute(
            "CREATE TABLE gold.dim_grading_group AS SELECT * FROM dim_grading_group_df"
        )

        # ------------------------------------------------------------------
        # Parquet outputs
        # ------------------------------------------------------------------
        GOLD_DIR.mkdir(parents=True, exist_ok=True)

        fact_tests.to_parquet(GOLD_DIR / "fact_tests.parquet", index=False)
        dim_student.to_parquet(GOLD_DIR / "dim_student.parquet", index=False)
        dim_teacher.to_parquet(GOLD_DIR / "dim_teacher.parquet", index=False)
        dim_school.to_parquet(GOLD_DIR / "dim_school.parquet", index=False)
        dim_grading_group.to_parquet(
            GOLD_DIR / "dim_grading_group.parquet", index=False
        )

        # ------------------------------------------------------------------
        # Audit
        # ------------------------------------------------------------------
        write_audit_record(
            run_id=run_id,
            stage="gold_materialization",
            status="SUCCESS",
            row_count=(
                len(fact_tests)
                + len(dim_student)
                + len(dim_teacher)
                + len(dim_school)
                + len(dim_grading_group)
            ),
        )

        logger.info("Gold layer successfully materialized")

    except Exception as exc:
        logger.exception("Gold layer materialization failed")

        write_audit_record(
            run_id=run_id,
            stage="gold_materialization",
            status="FAILED",
            error_message=str(exc),
        )
        raise


