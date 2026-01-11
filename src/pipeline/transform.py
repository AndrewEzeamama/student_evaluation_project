import pandas as pd
from src.core.config import SILVER_DIR
from src.core.logging import get_logger
from src.core.audit import write_audit_record

logger = get_logger("TRANSFORM")

def transform_silver(run_id: str):
    source_path = SILVER_DIR / "student_evaluation_cleaned.parquet"

    df = pd.read_parquet(source_path)

    students = (
        df[["student_id", "student_name"]]
        .dropna()
        .drop_duplicates()
    )

    teachers = (
        df[["teacher_id", "teacher_name"]]
        .dropna()
        .drop_duplicates()
    )

    schools = (
        df[["school_id", "school_name"]]
        .dropna()
        .drop_duplicates()
    )

    tests = (
        df[["test_id", "test_name"]]
        .dropna()
        .drop_duplicates()
    )

    test_details = df[
        ["test_id", "exam_date", "score"]
    ].dropna()

    students.to_parquet(SILVER_DIR / "students.parquet", index=False)
    teachers.to_parquet(SILVER_DIR / "teachers.parquet", index=False)
    schools.to_parquet(SILVER_DIR / "schools.parquet", index=False)
    tests.to_parquet(SILVER_DIR / "tests.parquet", index=False)
    test_details.to_parquet(
        SILVER_DIR / "test_details.parquet", index=False
    )

    write_audit_record(
        run_id=run_id,
        stage="silver_transform",
        status="SUCCESS",
        row_count=len(df)
    )

    logger.info("Silver transformations completed")
