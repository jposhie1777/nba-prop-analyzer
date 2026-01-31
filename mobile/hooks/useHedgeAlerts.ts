// hooks/useHedgeAlerts.ts
import { useEffect, useRef, useState } from "react";
import { API_BASE } from "@/lib/config";
import { useParlayTracker, TrackedParlaySnapshot } from "@/store/useParlayTracker";
import { calcPace, isAtRisk, RiskLevel } from "@/utils/paceCalculator";
import AsyncStorage from "@react-native-async-storage/async-storage";

const PUSH_TOKEN_KEY = "expo_push_token_last";

export type HedgeSuggestion = {
  leg_id: string;
  player_id: number;
  player_name: string;
  original_side: string;
  original_line: number;
  current_stat: number | null;
  risk_level: RiskLevel;
  pace_ratio: number;
  hedge_side: string;
  hedge_line: number;
  hedge_odds: number;
  hedge_book: string;
  all_hedges?: Array<{
    line: number;
    odds: number;
    book: string;
    side: string;
  }>;
};

export type HedgeAlertResult = {
  parlay_id: string;
  suggestions: HedgeSuggestion[];
  push_sent: boolean;
};

type UseHedgeAlertsOptions = {
  /** Polling interval in ms (default: 30000 = 30s) */
  pollInterval?: number;
  /** Whether to send push notifications */
  sendPush?: boolean;
  /** Minimum risk level to trigger alerts */
  minRiskLevel?: "at_risk" | "danger";
};

export function useHedgeAlerts(options: UseHedgeAlertsOptions = {}) {
  const {
    pollInterval = 30_000,
    sendPush = true,
    minRiskLevel = "at_risk",
  } = options;

  const [suggestions, setSuggestions] = useState<
    Record<string, HedgeSuggestion[]>
  >({});
  const [loading, setLoading] = useState(false);
  const [lastChecked, setLastChecked] = useState<Date | null>(null);

  const tracked = useParlayTracker((s) => s.tracked);
  const lastPayloadRef = useRef<string | null>(null);

  useEffect(() => {
    let mounted = true;
    let intervalId: ReturnType<typeof setInterval> | null = null;

    async function checkHedges() {
      const parlays = Object.values(tracked);

      // Filter to parlays with at-risk legs
      const parlaysToCheck = parlays.filter((parlay) =>
        parlay.legs.some((leg) => {
          const pace = calcPace({
            current: leg.current,
            line: leg.line,
            side: leg.side,
            period: leg.period,
            clock: leg.clock,
            gameStatus: leg.game_status,
          });

          if (minRiskLevel === "danger") {
            return pace.riskLevel === "danger";
          }
          return isAtRisk(pace.riskLevel);
        })
      );

      if (parlaysToCheck.length === 0) {
        setSuggestions({});
        return;
      }

      // Get push token if we want to send pushes
      let pushToken: string | null = null;
      if (sendPush) {
        try {
          pushToken = await AsyncStorage.getItem(PUSH_TOKEN_KEY);
        } catch {
          // Ignore
        }
      }

      setLoading(true);

      const allSuggestions: Record<string, HedgeSuggestion[]> = {};

      for (const parlay of parlaysToCheck) {
        try {
          const result = await checkParlayHedges(parlay, pushToken);
          if (result.suggestions.length > 0) {
            allSuggestions[parlay.parlay_id] = result.suggestions;
          }
        } catch (err) {
          console.warn("[useHedgeAlerts] Error checking parlay:", err);
        }
      }

      if (mounted) {
        setSuggestions(allSuggestions);
        setLastChecked(new Date());
        setLoading(false);
      }
    }

    // Initial check
    checkHedges();

    // Poll
    intervalId = setInterval(checkHedges, pollInterval);

    return () => {
      mounted = false;
      if (intervalId) clearInterval(intervalId);
    };
  }, [tracked, pollInterval, sendPush, minRiskLevel]);

  return {
    suggestions,
    loading,
    lastChecked,
    /** Total count of suggestions across all parlays */
    totalCount: Object.values(suggestions).flat().length,
  };
}

/**
 * Check a single parlay for hedge opportunities.
 */
async function checkParlayHedges(
  parlay: TrackedParlaySnapshot,
  pushToken: string | null
): Promise<HedgeAlertResult> {
  const legs = parlay.legs.map((leg) => ({
    leg_id: leg.leg_id,
    player_id: leg.player_id,
    player_name: leg.player_name,
    market: leg.market,
    side: leg.side,
    line: leg.line,
    current: leg.current ?? null,
    period: leg.period ?? null,
    clock: leg.clock ?? null,
    game_status: leg.game_status ?? null,
  }));

  const res = await fetch(`${API_BASE}/alerts/hedge/check`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      parlay_id: parlay.parlay_id,
      legs,
      expo_push_token: pushToken,
    }),
  });

  if (!res.ok) {
    throw new Error(`Hedge check failed: ${res.status}`);
  }

  const json = await res.json();

  return {
    parlay_id: json.parlay_id,
    suggestions: json.suggestions ?? [],
    push_sent: json.push_sent ?? false,
  };
}

/**
 * Manually trigger a hedge check for a specific parlay.
 */
export async function manualHedgeCheck(
  parlay: TrackedParlaySnapshot
): Promise<HedgeAlertResult> {
  let pushToken: string | null = null;
  try {
    pushToken = await AsyncStorage.getItem(PUSH_TOKEN_KEY);
  } catch {
    // Ignore
  }

  return checkParlayHedges(parlay, pushToken);
}
