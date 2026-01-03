# services/edges.py

from typing import List, Dict
from datetime import datetime
import pytz

def get_live_edges(limit: int = 25) -> List[Dict]:
    """
    Returns ranked live betting edges.
    UI-agnostic. JSON-safe.
    """
    raise NotImplementedError