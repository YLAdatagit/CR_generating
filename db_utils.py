"""PostgreSQL helpers for connection and simple queries."""
from sqlalchemy import create_engine
import pandas as pd
import os

# Re‑use .env settings that config.py already loads
DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(DB_URL, pool_pre_ping=True)
    return _engine


def list_tables(schema: str = "public") -> pd.DataFrame:
    """Return DataFrame of table names in the given schema."""
    q = (
        "SELECT table_name\n"
        "FROM information_schema.tables\n"
        "WHERE table_schema = :schema AND table_type = 'BASE TABLE'\n"
        "ORDER BY 1;"
    )
    return pd.read_sql(q, get_engine(), params={"schema": schema})


def run_query(sql: str, **params) -> pd.DataFrame:
    """Run parameterised SQL and return a DataFrame.

    Pandas/SQLAlchemy will raise a ``TypeError`` when an empty dictionary is
    passed as ``params`` even for queries that do not use parameters.  To avoid
    this, ``params`` is only supplied when it contains values.
    """
    if params:
        return pd.read_sql(sql, get_engine(), params=params)
    return pd.read_sql(sql, get_engine())
