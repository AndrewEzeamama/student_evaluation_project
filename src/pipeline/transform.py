import pandas as pd
from pathlib import Path
from datetime import datetime

from src.core.config import BRONZE_DIR, SILVER_DIR, QUARANTINE_DIR
from src.core.logging import get_logger
from src.core.audit import write_audit_record

logger = get_logger("SILVER_TRANSFORM")

CRITICAL_COLUMNS = {
    "students": ["student_id"],
    "teachers": ["teacher_id"],
    "schools": ["school_id"],
}

def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df

def _split_valid_invalid(df: pd.DataFrame, required_cols: list):
    invalid = df[df[required_cols].isnull().any(axis=1)]
    valid = df.dropna(subset=required_cols)
    return valid, invalid

def transform_silver(run_id: str):
    source_file = BRONZE_DIR / "student_evaluation_raw.xlsx"

    if not source_file.exists():
        raise FileNotFoundError(f"Missing source file: {source_file}")

    invalid_records = []

    try:
        # Load all sheets dynamically
        sheets = pd.read_excel(source_file, sheet_name=None)

        for sheet_name, df in sheets.items():
            df = _standardize_columns(df)

            if sheet_name.lower() not in CRITICAL_COLUMNS:
                continue

            valid, invalid = _split_valid_invalid(
                df,
                CRITICAL_COLUMNS[sheet_name.lower()]
            )

            if not invalid.empty:
                invalid["source_sheet"] = sheet_name
                invalid_records.append(invalid)

            valid = valid.drop_duplicates()

            output_path = SILVER_DIR / f"{sheet_name.lower()}.parquet"
            valid.to_parquet(output_path, index=False)

            logger.info(
                f"Written {len(valid)} rows to {output_path.name}"
            )

        # Write quarantine
        if invalid_records:
            quarantine_df = pd.concat(invalid_records)
            quarantine_path = (
                QUARANTINE_DIR / f"invalid_records_{run_id}.parquet"
            )
            quarantine_df.to_parquet(quarantine_path, index=False)

        write_audit_record(
            run_id=run_id,
            stage="silver_transform",
            status="SUCCESS",
            row_count=sum(len(df) for df in sheets.values())
        )

    except Exception as e:
        logger.exception("Silver transform failed")

        write_audit_record(
            run_id=run_id,
            stage="silver_transform",
            status="FAILED",
            error_message=str(e)
        )
        raise
