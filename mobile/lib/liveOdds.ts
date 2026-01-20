// lib/liveOdds.ts
import { API_BASE } from "./config";

console.log("🧠 liveOdds.ts LOADED");

const API = API_BASE;

// ------------------------------
// Types
// ------------------------------

export type CanonicalMarket =
  | "pts"
  | "ast"
  | "reb"
  | "3pm";

export type LivePlayerProp = {
  player_id: number;
  market: CanonicalMarket;

  market_type: "over_under" | "milestone";
  line: number;
  book: "draftkings" | "fanduel";

  over: number | null;
  under: number | null;
  milestone: number | null;
};

export type LiveGameOdds = {
  book: "draftkings" | "fanduel";

  spread_home: number | null;
  spread_away: number | null;
  spread_home_odds: number | null;
  spread_away_odds: number | null;

  total: number | null;
  over: number | null;
  under: number | null;

  moneyline_home_odds?: number | null;
  moneyline_away_odds?: number | null;
};

// ------------------------------
// API calls
// ------------------------------

export async function fetchLivePlayerProps(gameId: number) {
  console.log("🔥 fetchLivePlayerProps CALLED", gameId);

  const url = `${API}/live/odds/player-props?game_id=${gameId}`;
  console.log("🚨 LIVE PROPS FETCH URL", url);

  const res = await fetch(url);
  const text = await res.text();

  if (!res.ok) {
    throw new Error(`Failed to fetch live player props: ${res.status}`);
  }

  let json: any;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error("Invalid JSON from live props endpoint");
  }

  const rawProps =
    json.props ??
    json.items ??
    json.data ??
    [];

  const props: LivePlayerProp[] = rawProps
    .map((p: any) => {
      // 🚨 We now REQUIRE canonical markets from backend
      if (!p.market) return null;

      return {
        player_id: p.player_id,
        market: p.market as CanonicalMarket,
        market_type: p.market_type,
        line: p.line,
        book: p.book,

        over: p.odds?.over ?? null,
        under: p.odds?.under ?? null,
        milestone: p.odds?.yes ?? null,
      };
    })
    .filter(Boolean);

  const payload = {
    game_id: json.game_id ?? json.gameId ?? gameId,
    updated_at: json.updated_at ?? json.updatedAt ?? null,
    props,
  };

  if (__DEV__) {
    console.log("✅ LIVE PLAYER PROPS (CANONICAL)", {
      gameId: payload.game_id,
      count: payload.props.length,
      sample: payload.props[0],
    });
  }

  return payload;
}

export async function fetchLiveGameOdds(gameId: number) {
  const res = await fetch(`${API}/live/odds/games?game_id=${gameId}`);
  const text = await res.text();

  let json: any;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error("Invalid JSON from game odds");
  }

  const odds =
    json.odds ??
    json.items ??
    json.data ??
    [];

  return {
    game_id: json.game_id ?? json.gameId ?? gameId,
    updated_at: json.updated_at ?? json.updatedAt ?? null,
    odds,
  };
}
