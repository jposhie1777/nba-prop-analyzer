# mobile_api/ingest/common/batch.py

from typing import Iterable, List, TypeVar

T = TypeVar("T")

def chunked(items: List[T], size: int = 25) -> Iterable[List[T]]:
    """
    Yield successive chunks from a list.

    Safe default for Ball Don't Lie player_ids batching.
    """
    for i in range(0, len(items), size):
        yield items[i : i + size]