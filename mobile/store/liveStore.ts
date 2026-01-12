//.  /store/liveStore.ts
import { create } from "zustand";
import {
  LiveGame,
  GameOdds,
  LivePlayer,
  PlayerPropMarket,
  GameId,
  PlayerId,
  MarketKey,
} from "@/types/betting";

type LiveStore = {
  // ---------------------------
  // Games
  // ---------------------------
  gamesById: Record<GameId, LiveGame>;
  gameIds: GameId[];

  // ---------------------------
  // Odds
  // ---------------------------
  oddsByGameId: Record<GameId, GameOdds>;

  // ---------------------------
  // Players
  // ---------------------------
  playersByKey: Record<string, LivePlayer>;
  playerIdsByGame: Record<GameId, PlayerId[]>;

  // ---------------------------
  // Player Prop Markets
  // ---------------------------
  propMarketsByKey: Record<string, PlayerPropMarket>;
  propMarketKeysByPlayer: Record<string, MarketKey[]>;

  // ---------------------------
  // Upserts
  // ---------------------------
  upsertGames: (games: LiveGame[]) => void;
  upsertOdds: (odds: GameOdds[]) => void;
  upsertPlayers: (players: LivePlayer[]) => void;
  upsertPropMarkets: (
    gameId: GameId,
    playerId: PlayerId,
    markets: PlayerPropMarket[]
  ) => void;
};

export const useLiveStore = create<LiveStore>((set, get) => ({
  gamesById: {},
  gameIds: [],

  oddsByGameId: {},

  playersByKey: {},
  playerIdsByGame: {},

  propMarketsByKey: {},
  propMarketKeysByPlayer: {},

  // ===========================
  // UPSERTS
  // ===========================

  upsertGames(games) {
    set((state) => {
      const next = { ...state.gamesById };
      const ids = new Set(state.gameIds);

      for (const g of games) {
        next[g.gameId] = g;
        ids.add(g.gameId);
      }

      return {
        gamesById: next,
        gameIds: Array.from(ids),
      };
    });
  },

  upsertOdds(odds) {
    set((state) => {
      const next = { ...state.oddsByGameId };
      for (const o of odds) {
        next[o.gameId] = o;
      }
      return { oddsByGameId: next };
    });
  },

  upsertPlayers(players) {
    set((state) => {
      const nextPlayers = { ...state.playersByKey };
      const nextByGame = { ...state.playerIdsByGame };

      for (const p of players) {
        const key = `${p.gameId}:${p.playerId}`;
        nextPlayers[key] = p;

        if (!nextByGame[p.gameId]) {
          nextByGame[p.gameId] = [];
        }

        if (!nextByGame[p.gameId].includes(p.playerId)) {
          nextByGame[p.gameId].push(p.playerId);
        }
      }

      return {
        playersByKey: nextPlayers,
        playerIdsByGame: nextByGame,
      };
    });
  },

  upsertPropMarkets(gameId, playerId, markets) {
    set((state) => {
      const marketKeys: MarketKey[] = [];
      const nextMarkets = { ...state.propMarketsByKey };

      for (const m of markets) {
        const key = `${gameId}:${playerId}:${m.marketKey}`;
        nextMarkets[key] = m;
        marketKeys.push(m.marketKey);
      }

      return {
        propMarketsByKey: nextMarkets,
        propMarketKeysByPlayer: {
          ...state.propMarketKeysByPlayer,
          [`${gameId}:${playerId}`]: marketKeys,
        },
      };
    });
  },
}));