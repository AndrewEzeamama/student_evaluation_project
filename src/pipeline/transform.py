from datetime import datetime
import pandas as pd
from pathlib import Path

from src.core.config import (
    BRONZE_DIR,
    SILVER_DIR,
    QUARANTINE_DIR
)
from src.core.logging import get_logger
from src.core.audit import write_audit_record
from src.core.duckdb_conn import get_duckdb_connection

logger = get_logger("SILVER_TRANSFORM")


# -------------------------------
# Helpers
# -------------------------------
def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df


def _parse_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(
        series,
        format="%d/%m/%Y",
        errors="coerce"
    )


# -------------------------------
# Main Silver Transform
# -------------------------------
def transform_silver(run_id: str):
    start_ts = datetime.utcnow()

    raw_path = BRONZE_DIR / "student_evaluation_raw.xlsx"
    quarantine_path = QUARANTINE_DIR / f"invalid_records_{run_id}.parquet"

    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Reading Bronze Excel file")
    sheets = pd.read_excel(raw_path, sheet_name=None)

    dq_failures = []
    total_rows = 0

    # -------------------------------
    # SCHOOLS
    # -------------------------------
    schools = _standardize_columns(sheets["schools"])
    total_rows += len(schools)

    schools["school_id"] = schools["school_id"].astype(str)

    invalid_schools = schools[
        schools["school_id"].isna()
        | schools["school_id"].duplicated()
    ].assign(
        dq_reason="school_id_null_or_duplicate"
    )

    schools = schools.drop(index=invalid_schools.index)

    schools.to_parquet(SILVER_DIR / "schools.parquet", index=False)
    dq_failures.append(invalid_schools)

    # -------------------------------
    # TEACHERS
    # -------------------------------
    teachers = _standardize_columns(sheets["teachers"])
    total_rows += len(teachers)

    teachers["teacher_id"] = teachers["teacher_id"].astype(str)

    invalid_teachers = teachers[
        teachers["teacher_id"].isna()
        | teachers["teacher_id"].duplicated()
    ].assign(
        dq_reason="teacher_id_null_or_duplicate"
    )

    teachers = teachers.drop(index=invalid_teachers.index)

    teachers.to_parquet(SILVER_DIR / "teachers.parquet", index=False)
    dq_failures.append(invalid_teachers)

    # -------------------------------
    # STUDENTS
    # -------------------------------
    students = _standardize_columns(sheets["students"])
    total_rows += len(students)

    students["student_id"] = students["student_id"].astype(str)

    invalid_students = students[
        students["student_id"].isna()
        | students["student_id"].duplicated()
    ].assign(
        dq_reason="student_id_null_or_duplicate"
    )

    students = students.drop(index=invalid_students.index)

    students.to_parquet(SILVER_DIR / "students.parquet", index=False)
    dq_failures.append(invalid_students)

    # -------------------------------
    # TEST DETAILS
    # -------------------------------
    test_details = _standardize_columns(sheets["test_details"])
    total_rows += len(test_details)

    test_details["assessment_date"] = _parse_date(
        test_details["assessment_date"]
    )

    invalid_tests = test_details[
        test_details["assessment_type"].isna()
        | test_details["assessment_date"].isna()
        | (test_details["assessment_date"] > datetime.utcnow())
    ].assign(
        dq_reason="invalid_assessment_type_or_date"
    )

    test_details = test_details.drop(index=invalid_tests.index)

    test_details.to_parquet(
        SILVER_DIR / "test_details.parquet", index=False
    )
    dq_failures.append(invalid_tests)

    # -------------------------------
    # QUARANTINE
    # -------------------------------
    if dq_failures:
        quarantine_df = pd.concat(dq_failures, ignore_index=True)
        quarantine_df.to_parquet(quarantine_path, index=False)

        logger.warning(
            f"Quarantined {len(quarantine_df)} invalid records"
        )

    # -------------------------------
    # AUDIT + DQ METRICS (DuckDB)
    # -------------------------------
    conn = get_duckdb_connection()

    conn.execute(
        """
        INSERT INTO data_quality_results
        (run_id, stage, check_name, column_name, success, failed_rows, total_rows)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            "silver",
            "critical_dq_rules",
            "multiple",
            len(quarantine_df) == 0,
            len(quarantine_df),
            total_rows,
        ),
    )

    conn.close()

    # -------------------------------
    # PIPELINE AUDIT
    # -------------------------------
    write_audit_record(
        run_id=run_id,
        stage="silver_transform",
        status="SUCCESS",
        row_count=total_rows,
        task_id="silver_transform"
    )

    duration = (datetime.utcnow() - start_ts).total_seconds()
    logger.info(
        f"Silver transformation completed in {duration:.2f}s"
    )
