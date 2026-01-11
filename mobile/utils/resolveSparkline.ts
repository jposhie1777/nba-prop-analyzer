// utils/resolveSparkline.ts
import { HistoricalPlayerTrend } from "@/hooks/useHistoricalPlayerTrends";

export function resolveSparklineByMarket(
  market: string,
  trend?: HistoricalPlayerTrend
) {
  if (!trend || !market) return {};

  const m = market.toLowerCase();

  switch (m) {
    // ------------------
    // COMBO MARKETS
    // ------------------
    case "points_rebounds_assists":
      return {
        sparkline_l5: trend.pra_last5_list,
        sparkline_l10: trend.pra_last10_list,
        sparkline_l20: trend.pra_last20_list,
      };

    case "points_rebounds":
      return {
        sparkline_l5: trend.pr_last5_list,
        sparkline_l10: trend.pr_last10_list,
        sparkline_l20: trend.pr_last20_list,
      };

    case "points_assists":
      return {
        sparkline_l5: trend.pa_last5_list,
        sparkline_l10: trend.pa_last10_list,
        sparkline_l20: trend.pa_last20_list,
      };

    case "rebounds_assists":
      return {
        sparkline_l5: trend.ra_last5_list,
        sparkline_l10: trend.ra_last10_list,
        sparkline_l20: trend.ra_last20_list,
      };

    // ------------------
    // CORE STATS
    // ------------------
    case "points":
      return {
        sparkline_l5: trend.pts_last5_list,
        sparkline_l10: trend.pts_last10_list,
        sparkline_l20: trend.pts_last20_list,
      };

    case "rebounds":
      return {
        sparkline_l5: trend.reb_last5_list,
        sparkline_l10: trend.reb_last10_list,
        sparkline_l20: trend.reb_last20_list,
      };

    case "assists":
      return {
        sparkline_l5: trend.ast_last5_list,
        sparkline_l10: trend.ast_last10_list,
        sparkline_l20: trend.ast_last20_list,
      };

    case "blocks":
      return {
        sparkline_l5: trend.blk_last5_list,
        sparkline_l10: trend.blk_last10_list,
        sparkline_l20: trend.blk_last20_list,
      };

    case "steals":
      return {
        sparkline_l5: trend.stl_last5_list,
        sparkline_l10: trend.stl_last10_list,
        sparkline_l20: trend.stl_last20_list,
      };

    case "threes":
      return {
        sparkline_l5: trend.fg3m_last5_list,
        sparkline_l10: trend.fg3m_last10_list,
        sparkline_l20: trend.fg3m_last20_list,
      };

    // ------------------
    // MILESTONES
    // ------------------
    case "double_double":
      return {
        sparkline_l5: trend.dd_last5_list,
        sparkline_l10: trend.dd_last10_list,
        sparkline_l20: trend.dd_last20_list,
      };

    case "triple_double":
      return {
        sparkline_l5: trend.td_last5_list,
        sparkline_l10: trend.td_last10_list,
        sparkline_l20: trend.td_last20_list,
      };

    default:
      console.warn("Unhandled sparkline market:", market);
      return {};
  }
}