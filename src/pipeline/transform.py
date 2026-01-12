import pandas as pd
from pathlib import Path

from src.core.config import BRONZE_DIR, SILVER_DIR, QUARANTINE_DIR
from src.core.logging import get_logger
from src.core.audit import write_audit_record

logger = get_logger("SILVER_TRANSFORM")

# ----------------------------------------
# Worksheet → Silver entity mapping
# ----------------------------------------
SHEET_ENTITY_MAP = {
    "student": "students",
    "students": "students",
    "teacher": "teachers",
    "teachers": "teachers",
    "school": "schools",
    "schools": "schools",
    "test": "tests",
    "tests": "tests",
    "test details": "test_details",
    "test_details": "test_details",
    "grading group": "grading_groups",
    "grading groups": "grading_groups",
    "grading_groups": "grading_groups",
}

# ----------------------------------------
# Critical columns for DQ
# ----------------------------------------
CRITICAL_COLUMNS = {
    "students": ["student_id"],
    "teachers": ["teacher_id"],
    "schools": ["school_id"],
    "grading_groups": ["assessement_level_id"],
    "test_details": ["assessment_type"],
}

def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df

def _split_valid_invalid(df: pd.DataFrame, required_cols: list):
    # Only enforce DQ if all required columns exist
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        logger.warning(
            f"Skipping DQ check, missing columns: {missing_cols}"
        )
        return df, pd.DataFrame()

    invalid = df[df[required_cols].isnull().any(axis=1)]
    valid = df.dropna(subset=required_cols)
    return valid, invalid

def transform_silver(run_id: str):
    source_file = BRONZE_DIR / "student_evaluation_raw.xlsx"

    if not source_file.exists():
        raise FileNotFoundError(f"Missing source file: {source_file}")

    invalid_records = []
    total_rows = 0

    try:
        sheets = pd.read_excel(source_file, sheet_name=None)

        for raw_sheet_name, df in sheets.items():
            sheet_key = raw_sheet_name.strip().lower()

            if sheet_key not in SHEET_ENTITY_MAP:
                logger.warning(f"Skipping unmapped worksheet: {raw_sheet_name}")
                continue

            entity = SHEET_ENTITY_MAP[sheet_key]

            df = _standardize_columns(df)
            total_rows += len(df)

            if entity in CRITICAL_COLUMNS:
                valid, invalid = _split_valid_invalid(
                    df, CRITICAL_COLUMNS[entity]
                )

                if not invalid.empty:
                    invalid["source_entity"] = entity
                    invalid_records.append(invalid)

                df = valid

            df = df.drop_duplicates()

            output_path = SILVER_DIR / f"{entity}.parquet"
            df.to_parquet(output_path, index=False)

            logger.info(
                f"Silver written: {output_path.name} ({len(df)} rows)"
            )

        # ----------------------------------------
        # Quarantine invalid records
        # ----------------------------------------
        if invalid_records:
            quarantine_df = pd.concat(invalid_records, ignore_index=True)
            quarantine_path = (
                QUARANTINE_DIR / f"invalid_records_{run_id}.parquet"
            )
            quarantine_df.to_parquet(quarantine_path, index=False)

            logger.warning(
                f"Quarantined {len(quarantine_df)} invalid records"
            )

        write_audit_record(
            run_id=run_id,
            stage="silver_transform",
            status="SUCCESS",
            row_count=total_rows,
        )

    except Exception as e:
        logger.exception("Silver transform failed")

        write_audit_record(
            run_id=run_id,
            stage="silver_transform",
            status="FAILED",
            error_message=str(e),
        )
        raise
