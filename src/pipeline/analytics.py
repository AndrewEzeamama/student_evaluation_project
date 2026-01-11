import duckdb
import pandas as pd
from src.core.config import (
    DUCKDB_PATH,
    SILVER_DIR,
    GOLD_DIR
)
from src.core.logging import get_logger
from src.core.audit import write_audit_record

logger = get_logger("ANALYTICS")

def build_gold(run_id: str):
    conn = duckdb.connect(DUCKDB_PATH)

    dim_student = pd.read_parquet(SILVER_DIR / "students.parquet")
    dim_teacher = pd.read_parquet(SILVER_DIR / "teachers.parquet")
    dim_school = pd.read_parquet(SILVER_DIR / "schools.parquet")
    dim_test = pd.read_parquet(SILVER_DIR / "tests.parquet")

    fact = pd.read_parquet(
        SILVER_DIR / "student_evaluation_cleaned.parquet"
    )

    dim_student.to_parquet(GOLD_DIR / "dim_student.parquet", index=False)
    dim_teacher.to_parquet(GOLD_DIR / "dim_teacher.parquet", index=False)
    dim_school.to_parquet(GOLD_DIR / "dim_school.parquet", index=False)
    dim_test.to_parquet(GOLD_DIR / "dim_test.parquet", index=False)
    fact.to_parquet(GOLD_DIR / "fact_test_results.parquet", index=False)

    write_audit_record(
        run_id=run_id,
        stage="gold_build",
        status="SUCCESS",
        row_count=len(fact)
    )

    conn.close()

    logger.info("Gold layer successfully built")
