import great_expectations as ge
from datetime import date
from src.core.config import SILVER_DIR, QUARANTINE_DIR
from src.core.logging import get_logger
from src.core.audit import write_audit_record

logger = get_logger("QUALITY")

def run_quality_checks(run_id: str):
    df = ge.read_parquet(
        SILVER_DIR / "student_evaluation_cleaned.parquet"
    )

    # Completeness
    df.expect_column_values_to_not_be_null("student_id")
    df.expect_column_values_to_not_be_null("test_id")
    df.expect_column_values_to_not_be_null("score")

    # Validity
    df.expect_column_values_to_be_between(
        "score", min_value=0, max_value=100
    )

    df.expect_column_values_to_be_between(
        "exam_date", max_value=date.today().isoformat()
    )

    # Uniqueness
    df.expect_compound_columns_to_be_unique(
        ["student_id", "test_id", "exam_date"]
    )

    results = df.validate()

    if not results["success"]:
        failed = df[
            (df["score"] < 0) | (df["score"] > 100)
        ]
        quarantine_path = (
            QUARANTINE_DIR / f"invalid_records_{run_id}.parquet"
        )
        failed.to_parquet(quarantine_path, index=False)

        write_audit_record(
            run_id=run_id,
            stage="data_quality",
            status="FAILED",
            row_count=len(failed)
        )

        raise ValueError("Great Expectations validation failed")

    write_audit_record(
        run_id=run_id,
        stage="data_quality",
        status="SUCCESS",
        row_count=len(df)
    )

    logger.info("Great Expectations checks passed")
