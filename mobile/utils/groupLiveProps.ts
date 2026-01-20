// utils/groupliveprops
import { LivePlayerProp } from "@/lib/liveOdds";

/* ============================
   Types
============================ */

export type BookOdds = {
  over: number | null;
  under: number | null;
  milestone?: number | null;
};

export type LineEntry = {
  line: number;
  line_type: "over_under" | "milestone";
  books: Record<string, BookOdds>;
};

export type MarketEntry = {
  lines: LineEntry[];
};

export type GroupedLiveProps = Record<
  number,
  {
    player_id: number;
    markets: Record<string, MarketEntry>;
  }
>;

/* ============================
   Grouping Logic
============================ */

export function groupLiveProps(
  props: LivePlayerProp[]
): GroupedLiveProps {
  const grouped: GroupedLiveProps = {};

  for (const p of props) {
    const {
      player_id,
      market,
      market_type,
      line,
      book,
      over,
      under,
      milestone,
    } = p;
  
    // 🔧 NORMALIZE MARKET TYPE
    // Treat 3PM as over/under, not milestone
    const normalizedMarketType =
      market === "3pm" ? "over_under" : market_type;

    // ---------------------------
    // Init player
    // ---------------------------
    if (!grouped[player_id]) {
      grouped[player_id] = {
        player_id,
        markets: {},
      };
    }

    const player = grouped[player_id];

    // ---------------------------
    // Init market
    // ---------------------------
    if (!player.markets[market]) {
      player.markets[market] = {
        lines: [],
      };
    }

    const marketEntry = player.markets[market];

    // ---------------------------
    // Init or find line
    // ---------------------------
    let lineEntry = marketEntry.lines.find(
      (l) =>
        l.line === line &&
        l.line_type === normalizedMarketType
    );

    if (!lineEntry) {
      lineEntry = {
        line,
        line_type: normalizedMarketType,
        books: {},
      };
      marketEntry.lines.push(lineEntry);
    }

    // ---------------------------
    // Assign book odds
    // ---------------------------
    if (!lineEntry.books[book]) {
      lineEntry.books[book] = {
        over: null,
        under: null,
        milestone: null,
      };
    }
    
    if (normalizedMarketType === "over_under") {
      lineEntry.books[book].over = over ?? null;
      lineEntry.books[book].under = under ?? null;
    }
    
    if (normalizedMarketType === "milestone") {
      lineEntry.books[book].milestone = milestone ?? null;
    }
  }

  // ---------------------------
  // Sort lines ascending (UX)
  // ---------------------------
  for (const player of Object.values(grouped)) {
    for (const market of Object.values(player.markets)) {
      market.lines.sort((a, b) => {
        // Main line first
        if (a.line_type !== b.line_type) {
          return a.line_type === "over_under" ? -1 : 1;
        }
        // Then by line value
        return a.line - b.line;
      });
    }
  }

  return grouped;
}