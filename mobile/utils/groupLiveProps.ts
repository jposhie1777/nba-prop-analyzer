import { LivePlayerProp } from "@/lib/liveOdds";

export type GroupedLiveProps = Record<
  number,
  {
    player_id: number;
    markets: {
      [market: string]: {
        line: number;
        books: {
          draftkings?: { over: number; under: number };
          fanduel?: { over: number; under: number };
        };
      };
    };
  }
>;

export function groupLiveProps(
  props: LivePlayerProp[]
): GroupedLiveProps {
  const grouped: GroupedLiveProps = {};

  for (const p of props) {
    if (!grouped[p.player_id]) {
      grouped[p.player_id] = {
        player_id: p.player_id,
        markets: {},
      };
    }

    if (!grouped[p.player_id].markets[p.market]) {
      grouped[p.player_id].markets[p.market] = {
        line: p.line,
        books: {},
      };
    }

    grouped[p.player_id].markets[p.market].books[p.book] = {
      over: p.over,
      under: p.under,
    };
  }

  return grouped;
}