import { LivePlayerProp } from "@/lib/liveOdds";

/* ============================
   Types
============================ */

export type BookOdds = {
  over: number | null;
  under: number | null;
};

export type LineEntry = {
  line: number;
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
      line,
      book,
      over,
      under,
    } = p;

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
      (l) => l.line === line
    );

    if (!lineEntry) {
      lineEntry = {
        line,
        books: {},
      };
      marketEntry.lines.push(lineEntry);
    }

    // ---------------------------
    // Assign book odds
    // ---------------------------
    lineEntry.books[book] = {
      over: over ?? null,
      under: under ?? null,
    };
  }

  // ---------------------------
  // Sort lines ascending (UX)
  // ---------------------------
  for (const player of Object.values(grouped)) {
    for (const market of Object.values(player.markets)) {
      market.lines.sort((a, b) => a.line - b.line);
    }
  }

  return grouped;
}