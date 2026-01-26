import { useQuery } from "@tanstack/react-query";
import { fetchLiveProps } from "@/lib/apiLive";

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

export function useLivePropsDev(limit = 100) {
  return useQuery({
    queryKey: ["live-props", limit],
    queryFn: async () => {
      const rows = await fetchLiveProps(limit);

      return rows.map((r: any) => {
        const current = resolveCurrentStat(r);

        return {
          ...r,

          /* ---------- Card Friendly ---------- */
          game_period: periodNumToLabel(r.period_num),
          game_clock: secondsToClock(r.clock_seconds),

          current_stat: current,
          remaining_needed: r.remaining_to_line,

          score_margin: Math.abs(
            (r.home_score ?? 0) - (r.away_score ?? 0)
          ),
        };
      });
    },
    refetchInterval: 15_000, // live but not insane
    staleTime: 5_000,
  });
}