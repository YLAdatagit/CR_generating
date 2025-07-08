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
    """Return the newest week based on the latest `lte_<WEEK>` table name."""
    import db_utils
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name LIKE 'lte_%'
        ORDER BY table_name DESC
        LIMIT 1
    """
    df = db_utils.run_query(sql)
    if df.empty:
        raise RuntimeError("No LTE week tables found")
    table = df.iloc[0, 0]
    return table.split('_', 1)[1]  # extracts week (e.g., 'WK2525')


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