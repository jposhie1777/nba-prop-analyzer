# alerts_hedge.py
from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from google.cloud import bigquery

from bq import get_bq_client
from routes.push import send_push

router = APIRouter(prefix="/alerts", tags=["alerts"])


# ======================================================
# Types
# ======================================================

class TrackedLeg(BaseModel):
    leg_id: str
    player_id: int
    player_name: str
    market: str  # "pts", "reb", "ast", "fg3m"
    side: str  # "over" | "under"
    line: float
    current: Optional[float] = None
    period: Optional[int] = None
    clock: Optional[str] = None
    game_status: Optional[str] = None  # "pregame" | "live" | "final"
    game_id: Optional[int] = None


class HedgeCheckRequest(BaseModel):
    parlay_id: str
    legs: List[TrackedLeg]
    expo_push_token: Optional[str] = None


class HedgeSuggestion(BaseModel):
    leg_id: str
    player_id: int
    player_name: str
    original_side: str
    original_line: float
    current_stat: Optional[float]
    risk_level: str  # "at_risk" | "danger"
    pace_ratio: float

    # Hedge details
    hedge_side: str
    hedge_line: float
    hedge_odds: int
    hedge_book: str


# ======================================================
# Pace Calculator (Python port)
# ======================================================

def calc_game_progress(period: Optional[int], clock: Optional[str]) -> float:
    """Calculate game progress as 0-1."""
    if not period:
        return 0.0

    clock_seconds = 0
    if clock:
        parts = clock.split(":")
        if len(parts) == 2:
            mins = int(parts[0]) if parts[0].isdigit() else 0
            secs = int(parts[1]) if parts[1].isdigit() else 0
            clock_seconds = mins * 60 + secs

    QUARTER_SEC = 12 * 60  # 720
    REGULATION_SEC = 4 * QUARTER_SEC  # 2880
    OT_SEC = 5 * 60  # 300

    if period <= 4:
        completed_quarters = period - 1
        time_into_quarter = QUARTER_SEC - clock_seconds
        elapsed_sec = completed_quarters * QUARTER_SEC + time_into_quarter
    else:
        ot_period = period - 4
        completed_ot = ot_period - 1
        time_into_ot = OT_SEC - clock_seconds
        elapsed_sec = REGULATION_SEC + completed_ot * OT_SEC + time_into_ot

    return min(1.0, elapsed_sec / REGULATION_SEC)


def calc_risk_level(leg: TrackedLeg) -> tuple[str, float]:
    """
    Calculate risk level and pace ratio for a leg.
    Returns: (risk_level, pace_ratio)
    """
    if leg.game_status == "final":
        stat = leg.current or 0
        hit = (stat > leg.line) if leg.side == "over" else (stat < leg.line)
        return ("hit" if hit else "lost", stat / max(leg.line, 0.1))

    if leg.game_status == "pregame" or leg.current is None:
        return ("on_track", 1.0)

    progress = calc_game_progress(leg.period, leg.clock)

    if progress < 0.05:
        return ("on_track", 1.0)

    expected_stat = leg.line * progress
    pace_ratio = leg.current / max(expected_stat, 0.1)

    if leg.side == "over":
        if leg.current > leg.line:
            return ("on_track", pace_ratio)
        elif pace_ratio >= 0.85:
            return ("on_track", pace_ratio)
        elif pace_ratio >= 0.6:
            return ("at_risk", pace_ratio)
        else:
            return ("danger", pace_ratio)
    else:
        if leg.current >= leg.line:
            return ("danger", pace_ratio)
        elif pace_ratio <= 1.15:
            return ("on_track", pace_ratio)
        elif pace_ratio <= 1.4:
            return ("at_risk", pace_ratio)
        else:
            return ("danger", pace_ratio)


# ======================================================
# Live Props Query
# ======================================================

HEDGE_PROPS_QUERY = """
SELECT
  player_id,
  market,
  line,
  book,
  over_odds,
  under_odds
FROM `graphite-flare-477419-h7.nba_live.v_live_player_prop_odds_latest`
WHERE player_id = @player_id
  AND market = @market
ORDER BY line
"""


def find_hedge_props(
    bq: bigquery.Client,
    player_id: int,
    market: str,
    original_side: str,
    current_stat: Optional[float],
) -> List[Dict[str, Any]]:
    """
    Find hedge prop options for a player/market.
    Returns props on the opposite side that could hedge the original bet.
    """
    rows = list(
        bq.query(
            HEDGE_PROPS_QUERY,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
                    bigquery.ScalarQueryParameter("market", "STRING", market),
                ]
            ),
        )
    )

    hedge_side = "under" if original_side == "over" else "over"
    hedges = []

    for r in rows:
        odds = r.under_odds if hedge_side == "under" else r.over_odds
        if odds is None:
            continue

        # For a hedge to make sense:
        # - If original is OVER 25 and struggling, hedge UNDER should have line >= current
        # - If original is UNDER 25 and in danger, hedge OVER should have line <= current
        line = float(r.line)

        if original_side == "over":
            # Hedging with UNDER - want line that's achievable
            if current_stat is not None and line < current_stat:
                continue  # Line already busted
        else:
            # Hedging with OVER - want line that's still reachable
            if current_stat is not None and line > (current_stat * 2):
                continue  # Line probably unreachable

        hedges.append({
            "line": line,
            "odds": odds,
            "book": r.book,
            "side": hedge_side,
        })

    # Sort by most favorable odds
    hedges.sort(key=lambda x: x["odds"], reverse=True)

    return hedges[:3]  # Return top 3 options


# ======================================================
# Endpoint
# ======================================================

@router.post("/hedge/check")
def check_hedge_alerts(req: HedgeCheckRequest) -> Dict[str, Any]:
    """
    Analyze tracked parlay legs for at-risk positions and find hedge options.

    Optionally sends a push notification if expo_push_token is provided.
    """
    bq = get_bq_client()

    suggestions: List[Dict[str, Any]] = []

    for leg in req.legs:
        risk_level, pace_ratio = calc_risk_level(leg)

        # Only suggest hedges for at_risk or danger
        if risk_level not in ("at_risk", "danger"):
            continue

        # Skip if game is final
        if leg.game_status == "final":
            continue

        # Find hedge options
        hedges = find_hedge_props(
            bq,
            leg.player_id,
            leg.market,
            leg.side,
            leg.current,
        )

        if not hedges:
            continue

        best_hedge = hedges[0]

        suggestions.append({
            "leg_id": leg.leg_id,
            "player_id": leg.player_id,
            "player_name": leg.player_name,
            "original_side": leg.side,
            "original_line": leg.line,
            "current_stat": leg.current,
            "risk_level": risk_level,
            "pace_ratio": round(pace_ratio, 2),
            "hedge_side": best_hedge["side"],
            "hedge_line": best_hedge["line"],
            "hedge_odds": best_hedge["odds"],
            "hedge_book": best_hedge["book"],
            "all_hedges": hedges,
        })

    # Send push notification if requested and suggestions exist
    push_sent = False
    if req.expo_push_token and suggestions:
        try:
            lines = [
                f"{s['player_name']} {s['original_side'].upper()} {s['original_line']} "
                f"→ Consider {s['hedge_side'].upper()} {s['hedge_line']} ({s['hedge_odds']:+})"
                for s in suggestions[:5]
            ]

            send_push(
                token=req.expo_push_token,
                title=f"🔄 {len(suggestions)} Hedge Option{'s' if len(suggestions) > 1 else ''} Available",
                body="\n".join(lines),
                data={
                    "type": "hedge_alert",
                    "parlay_id": req.parlay_id,
                    "count": len(suggestions),
                },
            )
            push_sent = True
        except Exception as e:
            print(f"❌ Hedge push failed: {e}")

    return {
        "ok": True,
        "parlay_id": req.parlay_id,
        "suggestions": suggestions,
        "push_sent": push_sent,
    }


@router.get("/hedge/test")
def test_hedge_endpoint():
    """Health check for hedge alerts."""
    return {"ok": True, "message": "Hedge alerts endpoint ready"}
