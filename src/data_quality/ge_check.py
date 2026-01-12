import pandas as pd
import great_expectations as ge
from datetime import datetime
from pathlib import Path

from src.core.config import SILVER_DIR, GOLD_DIR, QUARANTINE_DIR
from src.core.logging import get_logger

logger = get_logger("DATA_QUALITY")

# -----------------------------------------------------
# Completeness & Uniqueness Rules
# -----------------------------------------------------
CRITICAL_COLUMNS = {
    "schools": ["school_id"],
    "students": ["student_id"],
    "grading_groups": ["assessment_level_id"],
    "test_details": ["assessment_type"],
}

# Composite key rule
COMPOSITE_KEYS = {
    "teachers": ["teacher_id", "school_id", "school_year", "course_name", "course_no"],
}

# -----------------------------------------------------
# Airflow entry point
# -----------------------------------------------------
def run_ge_checks(run_id: str):
    """
    Non-blocking Data Engineering + Data Quality checks.
    Failed rows are quarantined per dataset.
    """

    context = ge.get_context()
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    datasets = {
        "schools": SILVER_DIR / "schools.parquet",
        "teachers": SILVER_DIR / "teachers.parquet",
        "students": SILVER_DIR / "students.parquet",
        "grading_groups": SILVER_DIR / "grading_groups.parquet",
        "test_details": SILVER_DIR / "test_details.parquet",
        "tests": SILVER_DIR / "tests.parquet",
        "fact_test_results": GOLD_DIR / "fact_test_results.parquet",
    }

    grading_groups = None
    if (SILVER_DIR / "grading_groups.parquet").exists():
        grading_groups = pd.read_parquet(
            SILVER_DIR / "grading_groups.parquet"
        )

    for dataset, path in datasets.items():
        if not path.exists():
            logger.warning(f"[DQ] Skipping missing dataset: {dataset}")
            continue

        logger.info(f"[DQ] Processing {dataset}")
        df = pd.read_parquet(path)
        failed_mask = pd.Series(False, index=df.index)

        validator = context.sources.pandas_default.read_dataframe(
            df, asset_name=dataset
        )

        # -------------------------------------------------
        # 1. Completeness & Uniqueness (Single Column)
        # -------------------------------------------------
        for col in CRITICAL_COLUMNS.get(dataset, []):
            if col not in df.columns:
                continue

            validator.expect_column_values_to_not_be_null(col)
            validator.expect_column_values_to_be_unique(col)

            failed_mask |= df[col].isnull()
            failed_mask |= df[col].duplicated(keep=False)

        # -------------------------------------------------
        # 2. Teachers Composite Key Rule
        # -------------------------------------------------
        if dataset == "teachers":
            keys = COMPOSITE_KEYS["teachers"]

            validator.expect_compound_columns_to_be_unique(keys)

            failed_mask |= df[keys].isnull().any(axis=1)
            failed_mask |= df.duplicated(subset=keys, keep=False)

        # -------------------------------------------------
        # 3. Assessment Date Validity (tests only)
        # -------------------------------------------------
        if dataset == "tests" and "assessment_date" in df.columns:
            today = datetime.today().date()

            parsed = pd.to_datetime(
                df["assessment_date"],
                format="%d/%m/%Y",
                errors="coerce"
            )

            invalid_dates = parsed.isna() | (parsed.dt.date > today)
            failed_mask |= invalid_dates

            validator.expect_column_values_to_not_be_null("assessment_date")

        # -------------------------------------------------
        # 4. DE Rule – Valid Score Ranges
        # -------------------------------------------------
        if dataset == "fact_test_results" and grading_groups is not None:
            if {"standard_score", "assessment_level_id"}.issubset(df.columns):
                merged = df.merge(
                    grading_groups[
                        ["assessment_level_id", "score_min", "score_max"]
                    ],
                    on="assessment_level_id",
                    how="left"
                )

                invalid_scores = (
                    merged["standard_score"].isnull() |
                    (merged["standard_score"] < merged["score_min"]) |
                    (merged["standard_score"] > merged["score_max"])
                )

                failed_mask |= invalid_scores

                validator.expect_column_values_to_be_between(
                    "standard_score",
                    min_value=merged["score_min"].min(),
                    max_value=merged["score_max"].max(),
                )

                logger.info(
                    f"[DQ] Invalid score rows: {invalid_scores.sum()}"
                )

        # -------------------------------------------------
        # GE validation (reporting only)
        # -------------------------------------------------
        validator.validate()

        # -------------------------------------------------
        # Quarantine
        # -------------------------------------------------
        if failed_mask.any():
            out = (
                QUARANTINE_DIR /
                f"{dataset}_dq_failed_{run_id}.parquet"
            )

            df.loc[failed_mask].to_parquet(out, index=False)

            logger.error(
                f"[DQ FAILED] {dataset}: "
                f"{failed_mask.sum()} rows → {out.name}"
            )
        else:
            logger.info(f"[DQ PASSED] {dataset}")

    logger.info("[DQ] All Data Quality checks completed (non-blocking)")
