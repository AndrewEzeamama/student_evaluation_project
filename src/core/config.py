from pathlib import Path

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
QUARANTINE_DIR = DATA_DIR / "quarantine"

# DuckDB
DUCKDB_DIR = DATA_DIR / "db" / "duckdb"
DUCKDB_PATH = DUCKDB_DIR / "analytics.duckdb"

# Logs
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "pipeline.log"

# Ensure required directories exist
for path in [
    SILVER_DIR,
    GOLD_DIR,
    QUARANTINE_DIR,
    DUCKDB_DIR,
    LOG_DIR,
]:
    path.mkdir(parents=True, exist_ok=True)
