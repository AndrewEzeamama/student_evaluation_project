from pathlib import Path

def already_exists(path: Path) -> bool:
    """
    Prevents unnecessary recomputation.
    """
    return path.exists() and path.stat().st_size > 0
