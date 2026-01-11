// utils/resolveSparkline.ts
import { HistoricalPlayerTrend } from "@/hooks/useHistoricalPlayerTrends";

export function resolveSparklineByMarket(
  market: string,
  trend?: HistoricalPlayerTrend
) {
  if (!trend) return {};

  const m = market.toLowerCase();

  // PRA
  if (m.includes("pra")) {
    return {
      sparkline_l5: trend.pra_last5_list,
      sparkline_l10: trend.pra_last10_list,
      sparkline_l20: trend.pra_last20_list,
    };
  }

  // PR (points + rebounds)
  if (m === "pr" || m.includes("points + rebounds")) {
    return {
      sparkline_l5: trend.pr_last5_list,
      sparkline_l10: trend.pr_last10_list,
      sparkline_l20: trend.pr_last20_list,
    };
  }

  // PA (points + assists)
  if (m === "pa" || m.includes("points + assists")) {
    return {
      sparkline_l5: trend.pa_last5_list,
      sparkline_l10: trend.pa_last10_list,
      sparkline_l20: trend.pa_last20_list,
    };
  }

  // RA (rebounds + assists)
  if (m === "ra" || m.includes("rebounds + assists")) {
    return {
      sparkline_l5: trend.ra_last5_list,
      sparkline_l10: trend.ra_last10_list,
      sparkline_l20: trend.ra_last20_list,
    };
  }

  // Assists
  if (m.includes("ast")) {
    return {
      sparkline_l5: trend.ast_last5_list,
      sparkline_l10: trend.ast_last10_list,
      sparkline_l20: trend.ast_last20_list,
    };
  }

  // Rebounds
  if (m.includes("reb")) {
    return {
      sparkline_l5: trend.reb_last5_list,
      sparkline_l10: trend.reb_last10_list,
      sparkline_l20: trend.reb_last20_list,
    };
  }

  // Points
  if (m.includes("pts")) {
    return {
      sparkline_l5: trend.pts_last5_list,
      sparkline_l10: trend.pts_last10_list,
      sparkline_l20: trend.pts_last20_list,
    };
  }

  console.warn("Unhandled sparkline market:", market);
  return {};
}