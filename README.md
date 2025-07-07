# CR Generating

This project bundles a set of SQL queries for RET data collection and exposes a simple command line interface.

The pipeline extracts LTE and NR information from PostgreSQL tables and produces several CSV files zipped into an archive.

## Setup

1. Create a Python environment and install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Create a `.env` file in the repository root providing database credentials:
   ```ini
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=postgres
   DB_USER=postgres
   DB_PASSWORD=yourpassword
   ```
   Optional defaults for `CLUSTER_NAME` and `WEEK_NUM` may also be placed here.

Input tuning lists are expected at:
`D:/D&T Project/CR Preparing/<cluster_prefix>/Tuning_cell_list_<cluster>.csv`

## Usage

Run in *auto* mode to fetch the latest week and last two weeks of data:
```bash
python -m scripts.main --auto --cluster BMA00001_R1
```

The resulting CSV files are stored under `D:/D&T Project/CR Preparing/<cluster_prefix>` and zipped into `<cluster>_files.zip`.

Manual mode lets you provide an explicit date range and week:
```bash
python -m scripts.main --start 2024-07-01 --end 2024-07-08 --week WK2525 --cluster BMA00001_R1
```

Dates are supplied as `YYYY-MM-DD` and control the `BETWEEN` filter applied to the database queries.

