// /hooks/useLivePropsInfinite.ts
import { useInfiniteQuery } from "@tanstack/react-query";
import { fetchLiveProps } from "@/lib/apiLive";

/* ======================================================
   HELPERS (same as old hook)
====================================================== */

function periodNumToLabel(period?: number) {
  if (!period) return null;
  if (period <= 4) return `Q${period}`;
  return `OT${period - 4}`;
}

function secondsToClock(seconds?: number) {
  if (seconds == null) return null;
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${m}:${s.toString().padStart(2, "0")}`;
}

function resolveCurrentStat(item: any) {
  switch (item.market) {
    case "pts":
      return item.live_pts;
    case "reb":
      return item.live_reb;
    case "ast":
      return item.live_ast;
    case "3pm":
      return item.live_fg3m;
    case "pra":
      return item.live_pts + item.live_reb + item.live_ast;
    case "pr":
      return item.live_pts + item.live_reb;
    case "pa":
      return item.live_pts + item.live_ast;
    case "ra":
      return item.live_reb + item.live_ast;
    default:
      return null;
  }
}

function resolveDisplayOdds(item: any) {
  if (item.over_odds != null) {
    return { odds: item.over_odds, side: "OVER" };
  }
  if (item.under_odds != null) {
    return { odds: item.under_odds, side: "UNDER" };
  }
  if (item.milestone_odds != null) {
    return { odds: item.milestone_odds, side: "MILESTONE" };
  }
  return { odds: null, side: null };
}

/* ======================================================
   HOOK
====================================================== */

export function useLivePropsInfinite() {
  return useInfiniteQuery({
    queryKey: ["live-props"],
    queryFn: async ({ pageParam }) => {
      const res = await fetchLiveProps({
        limit: 100,
        cursor: pageParam,
      });

      // 🔥 NORMALIZE HERE
      const items = res.items.map((r: any) => {
        const current = resolveCurrentStat(r);
        const { odds, side } = resolveDisplayOdds(r);

        return {
          ...r,

          /* ----- Card fields ----- */
          game_period: periodNumToLabel(r.period_num),
          game_clock: secondsToClock(r.clock_seconds),

          current_stat: current,
          remaining_needed: r.remaining_to_line,

          display_odds: odds,
          display_odds_side: side,

          score_margin: Math.abs(
            (r.home_score ?? 0) - (r.away_score ?? 0)
          ),
        };
      });

      return {
        items,
        next_cursor: res.next_cursor,
      };
    },

    getNextPageParam: (lastPage) =>
      lastPage.next_cursor ?? undefined,

    refetchInterval: 60_000,
    staleTime: 5_000,
  });
}