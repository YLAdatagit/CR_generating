"""Loads environment variables & builds a runtime configuration dictionary."""
from pathlib import Path
from datetime import datetime, timedelta
import logging, os
from dotenv import load_dotenv

# ---------- Logging ---------- #
LOG_DIR = Path(__file__).resolve().parent / "log"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "process.log"),
        logging.StreamHandler()
    ]
)

# ---------- .env ---------- #
load_dotenv(Path(__file__).resolve().parent / ".env")

# ---------- Helpers ---------- #

def latest_week_from_db():
    """Query the DB for the newest WEEK_NUM in lte_wk table."""
    import db_utils
    sql = "SELECT DISTINCT week FROM lte_wk ORDER BY week DESC LIMIT 1"
    return db_utils.run_query(sql).iloc[0, 0]


def build_cfg(args) -> dict:
    """Return runtime configuration dict respected by the pipeline."""
    if args.auto:
        week = latest_week_from_db()
        end = datetime.today()
        start = end - timedelta(days=14)
    else:
        week = args.week or os.getenv("WEEK_NUM")
        start = datetime.strptime(args.start, "%Y-%m-%d")
        end = datetime.strptime(args.end, "%Y-%m-%d")

    cfg = {
        "WEEK_NUM": week,
        "CLUSTER_NAME": args.cluster or os.getenv("CLUSTER_NAME"),
        "START_DATE": start.strftime("%Y-%m-%d"),
        "END_DATE": end.strftime("%Y-%m-%d")
    }
    logging.info("Runtime config: %s", cfg)
    return cfg