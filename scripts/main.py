"""CLI entry‑point.  Usage examples:

Auto mode (latest week + last 14 days):
  python -m scripts.main --auto --cluster BMA0001_R1

Manual mode:
  python -m scripts.main --week WK2525 --start 2025-06-01 --end 2025-06-14 \
         --cluster BMA0001_R1

List tables only:
  python -m scripts.main --show-tables
"""
import argparse, logging
from config import build_cfg
import db_utils
import importlib
import os
from pathlib import Path
import zipfile

from ret_utils.io_helper import fetch_data


def run_pipeline(cfg):
    """Execute the DB queries and export CSV files."""
    # ensure query templates pick up runtime values
    os.environ["CLUSTER_NAME"] = cfg["CLUSTER_NAME"]
    os.environ["WEEK_NUM"] = cfg["WEEK_NUM"]

    qdb = importlib.import_module("scripts.query_db")

    output_dir = Path(qdb.OUTPUT_BASE_DIR) / qdb.folder_name
    output_dir.mkdir(parents=True, exist_ok=True)

    queries = {
        "lte": qdb.query_lte,
        "nr": qdb.query_nr,
        "air": qdb.query_air,
        "non_air": qdb.query_non_air,
        "hw": qdb.query_hw,
        "hw_no_map": qdb.query_hw_no_map,
        "air_no_map": qdb.query_air_no_map,
        "non_air_no_map": qdb.query_non_air_no_map,
        "bfant_tilt": qdb.query_bfant_tilt,
        "nr_tilt": qdb.query_nr_tilt,
        "split_tilt": qdb.query_split_tilt,
    }

    csv_paths = []
    for name, sql in queries.items():
        df = db_utils.run_query(sql)
        path = output_dir / f"{cfg['CLUSTER_NAME']}_{name}.csv"
        df.to_csv(path, index=False)
        csv_paths.append(path)

    zip_path = output_dir / f"{cfg['CLUSTER_NAME']}_files.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in csv_paths:
            zf.write(p, p.name)
            p.unlink()


def cli():
    p = argparse.ArgumentParser(description="CR Generating CLI")
    p.add_argument("--auto", action="store_true", help="Auto mode (latest week & last 14 days)")
    p.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    p.add_argument("--end", type=str, help="End date YYYY-MM-DD")
    p.add_argument("--week", type=str, help="Week number e.g. WK2525")
    p.add_argument("--cluster", type=str, help="Cluster name e.g. BMA0001_R1")
    p.add_argument("--show-tables", action="store_true", help="Only list DB tables & exit")
    args = p.parse_args()

    if args.show_tables:
        print("\n📋 Tables in schema 'public':")
        print(db_utils.list_tables().to_string(index=False))
        return

    cfg = build_cfg(args)
    print(f"\n▶️ Running week {cfg['WEEK_NUM']} for cluster {cfg['CLUSTER_NAME']}...")

    # --- Run main pipeline --- #
    run_pipeline(cfg)
    print("🏁 Done!")

if __name__ == "__main__":
    cli()
