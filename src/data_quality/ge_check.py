import pandas as pd
import great_expectations as ge
from datetime import datetime
from pathlib import Path

from src.core.config import SILVER_DIR, QUARANTINE_DIR
from src.core.logging import get_logger

logger = get_logger("DATA_QUALITY")

# -----------------------------------------------------
# Critical columns per dataset (Completeness + Uniqueness)
# -----------------------------------------------------
CRITICAL_COLUMNS = {
    "schools": ["school_id"],
    "teachers": ["teacher_id"],
    "students": ["student_id"],
    "grading_groups": ["assessment_level_id"],
    "test_details": ["assessment_type"],
}

# -----------------------------------------------------
# Airflow entry point
# -----------------------------------------------------
def run_ge_checks(run_id: str):
    """
    Non-blocking Data Quality checks.
    - Writes failed records per dataset to quarantine/
    - Never raises exception (pipeline continues)
    """

    context = ge.get_context()
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    datasets = {
        "schools": SILVER_DIR / "schools.parquet",
        "teachers": SILVER_DIR / "teachers.parquet",
        "students": SILVER_DIR / "students.parquet",
        "grading_groups": SILVER_DIR / "grading_groups.parquet",
        "test_details": SILVER_DIR / "test_details.parquet",
        "tests": SILVER_DIR / "tests.parquet",  # date checks only
    }

    for dataset_name, parquet_path in datasets.items():
        if not parquet_path.exists():
            logger.warning(f"[DQ] Skipping missing file: {parquet_path.name}")
            continue

        logger.info(f"[DQ] Checking dataset: {dataset_name}")

        df = pd.read_parquet(parquet_path)
        failed_mask = pd.Series(False, index=df.index)

        # -------------------------------------------------
        # Initialize GE validator (reporting only)
        # -------------------------------------------------
        validator = context.sources.pandas_default.read_dataframe(
            df,
            asset_name=dataset_name,
        )

        # =================================================
        # 1. Completeness & Uniqueness Checks
        # =================================================
        for col in CRITICAL_COLUMNS.get(dataset_name, []):
            if col not in df.columns:
                logger.warning(f"[DQ] Column '{col}' missing in {dataset_name}")
                continue

            # GE expectations (metrics & documentation)
            validator.expect_column_values_to_not_be_null(col)
            validator.expect_column_values_to_be_unique(col)

            # Pandas logic (row-level quarantine)
            failed_mask |= df[col].isnull()
            failed_mask |= df[col].duplicated(keep=False)

        # =================================================
        # 2. Consistency & Validity – assessment_date (tests only)
        # =================================================
        if dataset_name == "tests" and "assessment_date" in df.columns:
            today = datetime.today().date()

            parsed_dates = pd.to_datetime(
                df["assessment_date"],
                format="%d/%m/%Y",
                errors="coerce",
            )

            invalid_date_mask = (
                parsed_dates.isna() |
                (parsed_dates.dt.date > today)
            )

            failed_mask |= invalid_date_mask

            logger.info(
                f"[DQ] tests.assessment_date → "
                f"{invalid_date_mask.sum()} invalid rows detected"
            )

        # -------------------------------------------------
        # Execute GE validation (non-blocking, report only)
        # -------------------------------------------------
        _ = validator.validate()

        # -------------------------------------------------
        # Quarantine failed records per dataset
        # -------------------------------------------------
        if failed_mask.any():
            failed_df = df.loc[failed_mask]

            quarantine_file = (
                QUARANTINE_DIR /
                f"{dataset_name}_dq_failed_{run_id}.parquet"
            )

            failed_df.to_parquet(quarantine_file, index=False)

            logger.error(
                f"[DQ FAILED] {dataset_name}: "
                f"{len(failed_df)} rows quarantined → {quarantine_file.name}"
            )
        else:
            logger.info(f"[DQ PASSED] {dataset_name}")

    logger.info("[DQ] Data Quality checks completed (non-blocking)")
