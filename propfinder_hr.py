import requests
from datetime import date

BASE = "https://api.propfinder.app"

def get_matchup(pitcher_id, opp_team_id):
    r = requests.get(f"{BASE}/mlb/pitcher-matchup", params={
        "pitcherId": pitcher_id,
        "opposingTeamId": opp_team_id
    })
    return r.json()

def pitcher_hr_vulnerability(pitch_log, batter_hand):
    """How vulnerable is this pitcher to HRs against this batter hand."""
    relevant = [p for p in pitch_log
                if p["type"] == batter_hand
                and p.get("percentage", 0) > 0.05]
    score = 0
    for p in relevant:
        usage = p.get("percentage", 0)
        iso = p.get("iso", 0) or 0
        woba = p.get("woba", 0) or 0
        hrs = p.get("homeRuns", 0) or 0
        score += usage * (iso * 3 + woba * 2 + hrs * 0.5)
    return round(score, 4)

def run(matchups):
    results = []

    for pitcher_id, opp_team_id, pitcher_name in matchups:
        data = get_matchup(pitcher_id, opp_team_id)
        pitch_log = data.get("pitchLog", [])

        # Name + hand lookup from hitData
        batter_info = {}
        for h in data.get("hitData", []):
            pid = h["playerId"]
            if pid not in batter_info:
                batter_info[pid] = {
                    "name": h["batterName"],
                    "hand": "RHB" if h["batSide"] == "R" else "LHB"
                }

        for b in data.get("batterVsPitcher", []):
            batter_id = b["batterId"]
            info = batter_info.get(batter_id)
            if not info:
                continue

            batter_hand = info["hand"]
            pitcher_vuln = pitcher_hr_vulnerability(pitch_log, batter_hand)
            
            # Individual batter quality: career wOBA + HRs vs this pitcher
            career_woba = b.get("woba", 0) or 0
            career_hr = b.get("homeRuns", 0) or 0
            career_slg = b.get("slg", 0) or 0

            # Combined score: pitcher vulnerability * batter quality
            combined = pitcher_vuln * (1 + career_woba + career_slg * 0.5 + career_hr * 0.1)

            results.append({
                "batter": info["name"],
                "hand": batter_hand,
                "pitcher": pitcher_name,
                "pitcher_vuln": pitcher_vuln,
                "career_hr": career_hr,
                "career_woba": round(career_woba, 3),
                "score": round(combined, 4),
            })

    results.sort(key=lambda x: x["score"], reverse=True)
    seen = set()
    print(f"\n{'='*60}")
    print(f"HR BEST BETS - {date.today()}")
    print(f"{'='*60}")
    rank = 1
    for r in results:
        if r["batter"] in seen:
            continue
        seen.add(r["batter"])
        print(f"{rank:2}. {r['batter']:<22} ({r['hand']}) vs {r['pitcher']:<16} "
              f"score={r['score']:.4f} HR={r['career_hr']} wOBA={r['career_woba']}")
        rank += 1
        if rank > 20:
            break

if __name__ == "__main__":
    run([
        (608331, 137, "Max Fried"),
        (608566, 147, "German Marquez"),
    ])
