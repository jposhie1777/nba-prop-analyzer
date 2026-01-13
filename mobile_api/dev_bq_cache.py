# dev_bq_cache.py
from datetime import datetime, timedelta
from typing import Dict, Any

CACHE_TTL = timedelta(hours=2)

_cache: Dict[str, Any] = {
    "tables": None,
    "schemas": {},
    "last_refresh": None,
}

def is_stale():
    if not _cache["last_refresh"]:
        return True
    return datetime.utcnow() - _cache["last_refresh"] > CACHE_TTL

def set_tables(dataset: str, tables: list[str]):
    _cache["tables"] = {
        "dataset": dataset,
        "tables": tables,
    }
    _cache["last_refresh"] = datetime.utcnow()

def get_tables():
    return _cache["tables"]

def set_schema(dataset: str, table: str, schema: list[dict]):
    _cache["schemas"][f"{dataset}.{table}"] = schema

def get_schema(dataset: str, table: str):
    return _cache["schemas"].get(f"{dataset}.{table}")