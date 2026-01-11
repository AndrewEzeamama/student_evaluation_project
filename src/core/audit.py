from datetime import datetime
from src.core.duckdb_conn import get_duckdb_connection

def init_audit_tables():
    con = get_duckdb_connection()
    con.execute("""
        CREATE TABLE IF NOT EXISTS audit_pipeline_runs (
            run_id STRING,
            stage STRING,
            status STRING,
            row_count BIGINT,
            error_message STRING,
            created_at TIMESTAMP
        )
    """)
    con.close()

def write_audit_record(
    run_id: str,
    stage: str,
    status: str,
    row_count: int = 0,
    error_message: str | None = None
):
    init_audit_tables()

    con = get_duckdb_connection()
    con.execute(
        """
        INSERT INTO audit_pipeline_runs
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            stage,
            status,
            row_count,
            error_message,
            datetime.utcnow(),
        ),
    )
    con.close()
