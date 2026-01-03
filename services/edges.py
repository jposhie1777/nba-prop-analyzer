# services/edges.py

from typing import List, Dict
import pandas as pd

def get_edges_dataframe(
    *,
    props_df: pd.DataFrame,
    window_col: str,
    min_hit_rate: float,
    books: list[str] | None = None,
    odds_range: tuple[int, int] | None = None,
    ev_only: bool = False,
) -> pd.DataFrame:
    """
    Core edge computation.
    NO Streamlit.
    NO rendering.
    """
    df = props_df.copy()

    # -----------------------------
    # Bet type normalization
    # -----------------------------
    if "bet_type" in df.columns:
        df["bet_type"] = (
            df["bet_type"]
            .astype(str)
            .str.lower()
            .replace({
                "count": "over",
                "binary": "over",
                "yes": "over",
                "over": "over",
                "under": "under",
            })
        )

    # -----------------------------
    # Min hit rate
    # -----------------------------
    if window_col in df.columns:
        df = df[df[window_col] >= min_hit_rate]

    # -----------------------------
    # Odds range
    # -----------------------------
    if odds_range and "price" in df.columns:
        lo, hi = odds_range
        df = df[(df["price"] >= lo) & (df["price"] <= hi)]

    # -----------------------------
    # Books
    # -----------------------------
    if books and "bookmaker" in df.columns:
        df = df[df["bookmaker"].isin(books)]

    # -----------------------------
    # EV only
    # -----------------------------
    if ev_only and window_col in df.columns and "implied_prob" in df.columns:
        df = df[df[window_col] > df["implied_prob"]]

    # -----------------------------
    # Sort by edge signal
    # -----------------------------
    if window_col in df.columns and "price" in df.columns:
        df = df.sort_values([window_col, "price"], ascending=[False, True])

    return df