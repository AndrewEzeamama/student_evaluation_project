import duckdb
from datetime import datetime
from src.core.config import DUCKDB_PATH
from src.core.logging import get_logger

logger = get_logger("AUDIT")

def init_audit_tables():
    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_run_audit (
            run_id TEXT,
            pipeline_stage TEXT,
            status TEXT,
            row_count INTEGER,
            executed_at TIMESTAMP
        )
        """
    )
    conn.close()


def write_audit_record(
    run_id: str,
    stage: str,
    status: str,
    row_count: int
):
    init_audit_tables()
    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute(
        """
        INSERT INTO pipeline_run_audit
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            run_id,
            stage,
            status,
            row_count,
            datetime.utcnow()
        )
    )
    conn.close()

    logger.info(
        f"AUDIT | run_id={run_id} | stage={stage} | "
        f"status={status} | rows={row_count}"
    )
