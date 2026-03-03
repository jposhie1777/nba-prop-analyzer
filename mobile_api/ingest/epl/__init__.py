from __future__ import annotations

from typing import Any, Dict

__all__ = ["run_full_ingestion", "ingest_yesterday_refresh", "run_backfill"]


def run_full_ingestion(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    from .ingest import run_full_ingestion as _run_full_ingestion

    return _run_full_ingestion(*args, **kwargs)


def ingest_yesterday_refresh(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    from .ingest import ingest_yesterday_refresh as _ingest_yesterday_refresh

    return _ingest_yesterday_refresh(*args, **kwargs)


def run_backfill(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    from .ingest import run_backfill as _run_backfill

    return _run_backfill(*args, **kwargs)
