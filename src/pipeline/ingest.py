import pandas as pd
from src.core.config import BRONZE_DIR, SILVER_DIR
from src.core.logging import get_logger
from src.core.audit import write_audit_record
from src.core.idempotency import already_exists

logger = get_logger("INGEST")

def ingest_excel(run_id: str):
    source_path = BRONZE_DIR / "student_evaluation_raw.xlsx"
    target_path = SILVER_DIR / "student_evaluation_cleaned.parquet"

    if already_exists(target_path):
        logger.info("Bronze ingestion skipped (already exists)")
        return

    df = pd.read_excel(source_path)

    df.columns = [c.strip().lower() for c in df.columns]

    df.to_parquet(target_path, index=False)

    write_audit_record(
        run_id=run_id,
        stage="bronze_ingestion",
        status="SUCCESS",
        row_count=len(df)
    )

    logger.info(f"Bronze ingestion completed: {len(df)} rows")
