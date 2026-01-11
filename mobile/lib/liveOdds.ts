// lib/liveOdds.ts
import { API_BASE } from "./config";

console.log("🧠 liveOdds.ts LOADED");

const API = API_BASE;

// ------------------------------
// Types
// ------------------------------

export type LivePlayerProp = {
  player_id: number;
  market: "PTS" | "AST" | "REB" | "3PM";
  line: number;
  book: "draftkings" | "fanduel";
  over: number;
  under: number;
};

export type LiveGameOdds = {
  book: "draftkings" | "fanduel";
  spread: number | null;
  spread_odds: number | null;
  total: number | null;
  over: number | null;
  under: number | null;
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
  console.log("🧨 RAW LIVE PROPS RESPONSE", text);

  if (!res.ok) {
    throw new Error(`Failed to fetch live player props: ${res.status}`);
  }

  let json: any;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error("Invalid JSON from live props endpoint");
  }

  const props =
    json.props ??
    json.items ??
    json.data ??
    [];

  const payload = {
    game_id: json.game_id ?? json.gameId ?? gameId,
    updated_at: json.updated_at ?? json.updatedAt ?? null,
    props,
  };

  if (__DEV__) {
    console.log("✅ NORMALIZED LIVE PROPS", {
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
