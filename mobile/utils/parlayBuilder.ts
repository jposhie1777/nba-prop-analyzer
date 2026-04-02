import { Platform } from "react-native";

type BuildPlatform = "desktop" | "ios" | "android";

export type ParlayBatterInput = {
  batter_id?: number | null;
  batter_name?: string | null;
  score?: number | null;
  hr_odds_best_price?: number | null;
  dk_outcome_code?: string | null;
  dk_event_id?: string | null;
  fd_market_id?: string | null;
  fd_selection_id?: string | null;
};

type FanDuelLeg = { fd_market_id: string; fd_selection_id: string };
type DraftKingsLeg = { dk_outcome_code: string; dk_event_id: string };

export function getBuildPlatform(): BuildPlatform {
  if (Platform.OS === "ios") return "ios";
  if (Platform.OS === "android") return "android";
  return "desktop";
}

function normalizeCode(value?: string | null): string | null {
  const text = (value ?? "").trim();
  return text.length ? text : null;
}

function normalizedFdLegs(batters: ParlayBatterInput[]): FanDuelLeg[] | null {
  const out: FanDuelLeg[] = [];
  for (const batter of batters) {
    const market = normalizeCode(batter.fd_market_id);
    const selection = normalizeCode(batter.fd_selection_id);
    if (!market || !selection) {
      return null;
    }
    out.push({ fd_market_id: market, fd_selection_id: selection });
  }
  return out.length ? out : null;
}

function normalizedDkLegs(batters: ParlayBatterInput[]): DraftKingsLeg[] | null {
  const out: DraftKingsLeg[] = [];
  for (const batter of batters) {
    const outcome = normalizeCode(batter.dk_outcome_code);
    const eventId = normalizeCode(batter.dk_event_id);
    if (!outcome || !eventId) {
      return null;
    }
    out.push({ dk_outcome_code: outcome, dk_event_id: eventId });
  }
  return out.length ? out : null;
}

export function buildDraftKingsParlay(
  batters: Array<Pick<ParlayBatterInput, "dk_outcome_code" | "dk_event_id">>,
  platform: BuildPlatform = "desktop"
): string | null {
  const legs = normalizedDkLegs(batters as ParlayBatterInput[]);
  if (!legs?.length) return null;
  const outcomes = legs.map((leg) => leg.dk_outcome_code).join(",");
  if (platform === "ios" || platform === "android") {
    return `dksb://sb/addbet/${outcomes}`;
  }
  const eventId = legs[0].dk_event_id;
  return `https://sportsbook.draftkings.com/event/${eventId}?outcomes=${outcomes}`;
}

export function buildFanDuelParlay(
  batters: Array<Pick<ParlayBatterInput, "fd_market_id" | "fd_selection_id">>,
  platform: BuildPlatform = "desktop",
  fdState?: string
): string | null {
  const legs = normalizedFdLegs(batters as ParlayBatterInput[]);
  if (!legs?.length) return null;

  const st = (fdState ?? "").toLowerCase().trim();
  const statePrefix = st ? `${st}.` : "";
  const base =
    platform === "ios" || platform === "android"
      ? "fanduelsportsbook://account.sportsbook.fanduel.com/sportsbook/addToBetslip"
      : `https://${statePrefix}sportsbook.fanduel.com/addToBetslip`;

  // Use indexed array notation (marketId[0], selectionId[0]) which FanDuel requires
  const query = legs
    .map(
      (leg, i) =>
        `marketId[${i}]=${encodeURIComponent(leg.fd_market_id)}&selectionId[${i}]=${encodeURIComponent(leg.fd_selection_id)}`
    )
    .join("&");
  return `${base}?${query}`;
}

function americanToDecimal(odds: number): number {
  if (!Number.isFinite(odds) || odds === 0) return 1;
  if (odds > 0) return odds / 100 + 1;
  return 100 / Math.abs(odds) + 1;
}

function decimalToAmerican(decimal: number): number {
  if (!Number.isFinite(decimal) || decimal <= 1) return 0;
  if (decimal >= 2) return Math.round((decimal - 1) * 100);
  return Math.round(-100 / (decimal - 1));
}

export function calculateParlayOdds(americanOddsList: number[]): number | null {
  if (!americanOddsList.length) return null;
  const valid = americanOddsList.filter((value) => Number.isFinite(value) && value !== 0);
  if (!valid.length) return null;
  const decimal = valid.reduce((acc, odds) => acc * americanToDecimal(odds), 1);
  return decimalToAmerican(decimal);
}

export function buildParlayLinks(
  batters: ParlayBatterInput[],
  platform: BuildPlatform = "desktop"
): {
  draftkings: string | null;
  fanduel: string | null;
  combinedOdds: number | null;
} {
  const selected = batters.filter(Boolean);
  if (!selected.length) {
    return { draftkings: null, fanduel: null, combinedOdds: null };
  }
  const combinedOdds = calculateParlayOdds(
    selected
      .map((b) => b.hr_odds_best_price)
      .filter((value): value is number => Number.isFinite(value as number))
  );

  const draftkings = buildDraftKingsParlay(
    selected.map((b) => ({
      dk_outcome_code: b.dk_outcome_code ?? null,
      dk_event_id: b.dk_event_id ?? null,
    })),
    platform
  );

  const fanduel = buildFanDuelParlay(
    selected.map((b) => ({
      fd_market_id: b.fd_market_id ?? null,
      fd_selection_id: b.fd_selection_id ?? null,
    })),
    platform
  );

  return { draftkings, fanduel, combinedOdds };
}
