import duckdb
from src.core.config import DUCKDB_PATH

def get_duckdb_connection():
    return duckdb.connect(str(DUCKDB_PATH))
