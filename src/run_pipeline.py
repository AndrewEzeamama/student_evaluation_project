import uuid
from src.pipeline.ingest import ingest_excel
from src.pipeline.transform import transform_silver
from src.quality.ge_runner import run_quality_checks
from src.pipeline.analytics import build_gold
from src.core.logging import get_logger

logger = get_logger("PIPELINE")

def run():
    run_id = str(uuid.uuid4())

    logger.info(f"Pipeline run started: {run_id}")

    ingest_excel(run_id)
    transform_silver(run_id)
    run_quality_checks(run_id)
    build_gold(run_id)

    logger.info(f"Pipeline run completed: {run_id}")
