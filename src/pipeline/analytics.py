import duckdb
import pandas as pd
from datetime import datetime

from src.core.config import SILVER_DIR, DUCKDB_PATH
from src.core.logging import get_logger

logger = get_logger("GOLD_ANALYTICS")


def build_gold_layer(run_id: str):
    start_time = datetime.utcnow()
    con = duckdb.connect(DUCKDB_PATH)

    try:
        logger.info("Starting Gold layer materialization")

        # -------------------------
        # Load Silver Data
        # -------------------------
        students = pd.read_parquet(SILVER_DIR / "students.parquet")
        teachers = pd.read_parquet(SILVER_DIR / "teachers.parquet")
        schools = pd.read_parquet(SILVER_DIR / "schools.parquet")
        tests = pd.read_parquet(SILVER_DIR / "tests.parquet")
        test_details = pd.read_parquet(SILVER_DIR / "test_details.parquet")

        # -------------------------
        # Register DataFrames
        # -------------------------
        con.register("students_df", students)
        con.register("teachers_df", teachers)
        con.register("schools_df", schools)
        con.register("tests_df", tests)
        con.register("test_details_df", test_details)

        # -------------------------
        # Dimensions
        # -------------------------
        con.execute("""
            INSERT OR REPLACE INTO dim_student
            SELECT
                ROW_NUMBER() OVER () AS student_key,
                student_id,
                student_name
            FROM students_df
        """)

        con.execute("""
            INSERT OR REPLACE INTO dim_teacher
            SELECT
                ROW_NUMBER() OVER () AS teacher_key,
                teacher_id,
                teacher_name
            FROM teachers_df
        """)

        con.execute("""
            INSERT OR REPLACE INTO dim_school
            SELECT
                ROW_NUMBER() OVER () AS school_key,
                school_id,
                school_name,
                municipality
            FROM schools_df
        """)

        con.execute("""
            INSERT OR REPLACE INTO dim_test
            SELECT
                ROW_NUMBER() OVER () AS test_key,
                test_id,
                test_name,
                assessment_type
            FROM tests_df
        """)

        # -------------------------
        # Fact Table
        # -------------------------
        con.execute("""
            INSERT OR REPLACE INTO fact_test_results
            SELECT
                ROW_NUMBER() OVER () AS test_result_key,
                s.student_key,
                t.teacher_key,
                sc.school_key,
                te.test_key,
                td.exam_date,
                td.score
            FROM test_details_df td
            JOIN dim_student s ON td.student_id = s.student_id
            JOIN dim_teacher t ON td.teacher_id = t.teacher_id
            JOIN dim_school sc ON td.school_id = sc.school_id
            JOIN dim_test te ON td.test_id = te.test_id
        """)

        row_count = con.execute(
            "SELECT COUNT(*) FROM fact_test_results"
        ).fetchone()[0]

        # -------------------------
        # AUDIT: SUCCESS
        # -------------------------
        con.execute("""
            INSERT INTO audit_pipeline_runs
            VALUES (
                nextval('audit_pipeline_runs_seq'),
                ?, 'gold_layer', 'SUCCESS', ?, NULL, ?, ?
            )
        """, (
            run_id,
            row_count,
            start_time,
            datetime.utcnow()
        ))

        logger.info("Gold layer materialized successfully")

    except Exception as e:
        logger.error("Gold layer failed", exc_info=True)

        # -------------------------
        # AUDIT: FAILURE
        # -------------------------
        con.execute("""
            INSERT INTO audit_pipeline_runs
            VALUES (
                nextval('audit_pipeline_runs_seq'),
                ?, 'gold_layer', 'FAILED', 0, ?, ?, ?
            )
        """, (
            run_id,
            str(e),
            start_time,
            datetime.utcnow()
        ))

        raise

    finally:
        con.close()
