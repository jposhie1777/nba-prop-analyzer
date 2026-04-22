// components/nba/NbaResearchScreen.tsx
import {
  ActivityIndicator,
  Image,
  Linking,
  Modal,
  Platform,
  Pressable,
  ScrollView,
  StyleSheet,
  Switch,
  Text,
  TextInput,
  View,
} from "react-native";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Ionicons } from "@expo/vector-icons";
import * as Haptics from "expo-haptics";

import { useTheme } from "@/store/useTheme";
import { useBetslipDrawer } from "@/store/useBetslipDrawer";
import { usePropBetslip, PropSlipItem } from "@/store/usePropBetslip";
import { PropBetslipDrawer } from "@/components/prop/PropBetslipDrawer";
import { LastUpdatedBadge } from "@/components/mlb/LastUpdatedBadge";
import { TEAM_LOGOS } from "@/utils/teamLogos";
import {
  useNbaResearch,
  type ResearchProp,
  type ResearchGame,
  type ResearchCategory,
} from "@/hooks/useNbaResearch";

/* ======================================================
   CONSTANTS / LABELS
====================================================== */
const SORT_CHIPS: { key: SortKey; label: string }[] = [
  { key: "pfRating", label: "PF Rating" },
  { key: "streak", label: "Streak" },
  { key: "oppDef", label: "Opp Def" },
  { key: "season", label: "'25-'26" },
  { key: "h2h", label: "H2H" },
  { key: "l5", label: "L5" },
  { key: "l10", label: "L10" },
  { key: "l20", label: "L20" },
  { key: "line", label: "Line" },
  { key: "odds", label: "Odds" },
];

type SortKey =
  | "pfRating"
  | "streak"
  | "oppDef"
  | "season"
  | "h2h"
  | "l5"
  | "l10"
  | "l20"
  | "line"
  | "odds";

type SortDir = "asc" | "desc";
type OverUnder = "over" | "under";

const CATEGORY_MARKET_MAP: Record<string, string> = {
  points: "pts",
  rebounds: "reb",
  assists: "ast",
  threePointsMade: "3pm",
  steals: "stl",
  blocks: "blk",
  stealsBlocks: "stl+blk",
  pointsReboundsAssists: "pra",
  pointsRebounds: "pr",
  pointsAssists: "pa",
  reboundAssists: "ra",
  turnovers: "to",
  freeThrowsMade: "ftm",
  pointsInPaint: "pip",
  fantasyPoints: "fp",
};

const CATEGORY_SHORT_LABEL: Record<string, string> = {
  points: "Points",
  rebounds: "Rebounds",
  assists: "Assists",
  threePointsMade: "Three Pointers",
  steals: "Steals",
  blocks: "Blocks",
  stealsBlocks: "Steals + Blocks",
  pointsRebounds: "Pts + Reb",
  pointsAssists: "Pts + Ast",
  reboundAssists: "Reb + Ast",
  pointsReboundsAssists: "Pts + Reb + Ast",
  turnovers: "Turnovers",
  freeThrowsMade: "Free Throws",
  pointsInPaint: "Pts in Paint",
  fantasyPoints: "Fantasy Pts",
  doubleDouble: "Double Double",
  tripleDouble: "Triple Double",
  q1Points: "Q1 Points",
  q1Rebounds: "Q1 Rebounds",
  q1Assists: "Q1 Assists",
  q1ThreePointsMade: "Q1 3PM",
  q1PointsRebounds: "Q1 Pts + Reb",
  q1PointsAssists: "Q1 Pts + Ast",
  q1ReboundAssists: "Q1 Reb + Ast",
  q1PointsReboundsAssists: "Q1 PRA",
};

/* ======================================================
   HELPERS
====================================================== */
function fmtOdds(v: number | null | undefined): string {
  if (v == null) return "—";
  return v > 0 ? `+${v}` : String(v);
}

function fmtLine(v: number | null | undefined): string {
  if (v == null) return "—";
  return v.toString();
}

function fmtAvg(v: number | null | undefined, d = 1): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return v.toFixed(d);
}

function fmtHitFrac(raw: string | null, pct: number | null): string {
  if (raw && raw.includes("/")) return raw;
  if (pct == null) return "—";
  return `${Math.round(pct * 100)}%`;
}

function fmtHitPct(pct: number | null): string {
  if (pct == null) return "—";
  return `${Math.round(pct * 100)}%`;
}

function hitRateColor(pct: number | null): { bg: string; border: string } {
  if (pct == null) return { bg: "rgba(45,55,72,0.4)", border: "rgba(45,55,72,0.8)" };
  if (pct >= 0.75) return { bg: "rgba(22,163,74,0.35)", border: "#16A34A" };
  if (pct >= 0.55) return { bg: "rgba(34,197,94,0.25)", border: "#22C55E" };
  if (pct >= 0.45) return { bg: "rgba(234,179,8,0.25)", border: "#CA8A04" };
  if (pct >= 0.25) return { bg: "rgba(239,68,68,0.25)", border: "#EF4444" };
  return { bg: "rgba(220,38,38,0.35)", border: "#DC2626" };
}

function opponentRankColor(rank: number | null): string {
  if (rank == null) return "#9CA3AF";
  if (rank <= 10) return "#22C55E";
  if (rank <= 20) return "#F59E0B";
  return "#EF4444";
}

function pfRatingColor(rating: number | null): { bg: string; border: string; text: string } {
  if (rating == null) return { bg: "#2D3748", border: "#4A5568", text: "#9CA3AF" };
  if (rating >= 70) return { bg: "#3B0764", border: "#9333EA", text: "#E9D5FF" };
  if (rating >= 60) return { bg: "#4C1D95", border: "#7C3AED", text: "#DDD6FE" };
  if (rating >= 50) return { bg: "#1E3A8A", border: "#3B82F6", text: "#BFDBFE" };
  return { bg: "#1F2937", border: "#374151", text: "#9CA3AF" };
}

const POSITIONS_FIXED = ["PG", "SG", "SF", "PF", "C", "G", "F"];

/* ======================================================
   SORTED PROP COMPUTATION
====================================================== */
function sortValue(prop: ResearchProp, key: SortKey): number | null {
  switch (key) {
    case "pfRating":
      return prop.pf_rating;
    case "streak":
      return prop.streak;
    case "oppDef":
      // Lower rank = tougher defense; show easiest first (DESC) → higher rank better
      return prop.matchup_rank;
    case "season":
      return prop.hit_rate_season;
    case "h2h":
      return prop.hit_rate_vs_team;
    case "l5":
      return prop.hit_rate_l5;
    case "l10":
      return prop.hit_rate_l10;
    case "l20":
      return prop.hit_rate_l20;
    case "line":
      return prop.line;
    case "odds": {
      const o = prop.best_price;
      if (o == null) return null;
      // Sort odds by implied probability desc = odds ascending (negative -200 > +150)
      // Represent as American → implied percentage
      const imp = o > 0 ? 100 / (o + 100) : -o / (-o + 100);
      return imp;
    }
    default:
      return null;
  }
}

function applySort(
  props: ResearchProp[],
  sortKey: SortKey,
  dir: SortDir,
): ResearchProp[] {
  const sorted = [...props].sort((a, b) => {
    const av = sortValue(a, sortKey);
    const bv = sortValue(b, sortKey);
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    return dir === "desc" ? bv - av : av - bv;
  });
  return sorted;
}

/* ======================================================
   MODALS — Categories / Games / Teams / Positions
====================================================== */
function MultiSelectModal({
  visible,
  title,
  options,
  selected,
  onToggle,
  onToggleAll,
  onClose,
  colors,
  customRender,
}: {
  visible: boolean;
  title: string;
  options: { label: string; value: string; extra?: React.ReactNode }[];
  selected: Set<string>;
  onToggle: (v: string) => void;
  onToggleAll: () => void;
  onClose: () => void;
  colors: any;
  customRender?: (o: { label: string; value: string; extra?: React.ReactNode }) => React.ReactNode;
}) {
  const allSelected = selected.size === options.length;
  return (
    <Modal visible={visible} transparent animationType="fade" onRequestClose={onClose}>
      <Pressable style={ms.overlay} onPress={onClose}>
        <Pressable style={[ms.modal, { backgroundColor: colors.surface.elevated }]}>
          <View style={[ms.header, { borderBottomColor: colors.border.subtle }]}>
            <Text style={[ms.title, { color: colors.text.primary }]}>{title}</Text>
            <Pressable onPress={onClose}>
              <Ionicons name="close" size={22} color={colors.text.muted} />
            </Pressable>
          </View>
          <ScrollView style={ms.list}>
            <Pressable style={ms.item} onPress={onToggleAll}>
              <Ionicons
                name={allSelected ? "checkbox" : "square-outline"}
                size={20}
                color={allSelected ? "#A855F7" : colors.text.muted}
              />
              <Text style={[ms.itemText, { color: colors.text.primary, fontWeight: "800" }]}>
                Select All
              </Text>
            </Pressable>
            {options.map((o) => {
              const isSel = selected.has(o.value);
              return (
                <Pressable
                  key={o.value}
                  style={[ms.item, isSel && { backgroundColor: "rgba(168,85,247,0.12)" }]}
                  onPress={() => onToggle(o.value)}
                >
                  <Ionicons
                    name={isSel ? "checkbox" : "square-outline"}
                    size={20}
                    color={isSel ? "#A855F7" : colors.text.muted}
                  />
                  {customRender ? (
                    customRender(o)
                  ) : (
                    <Text style={[ms.itemText, { color: colors.text.primary }]}>{o.label}</Text>
                  )}
                </Pressable>
              );
            })}
          </ScrollView>
        </Pressable>
      </Pressable>
    </Modal>
  );
}

function SingleSelectModal({
  visible,
  title,
  options,
  selected,
  onSelect,
  onClose,
  colors,
}: {
  visible: boolean;
  title: string;
  options: { label: string; value: string }[];
  selected: string;
  onSelect: (v: string) => void;
  onClose: () => void;
  colors: any;
}) {
  return (
    <Modal visible={visible} transparent animationType="fade" onRequestClose={onClose}>
      <Pressable style={ms.overlay} onPress={onClose}>
        <Pressable style={[ms.modal, { backgroundColor: colors.surface.elevated }]}>
          <View style={[ms.header, { borderBottomColor: colors.border.subtle }]}>
            <Text style={[ms.title, { color: colors.text.primary }]}>{title}</Text>
            <Pressable onPress={onClose}>
              <Ionicons name="close" size={22} color={colors.text.muted} />
            </Pressable>
          </View>
          <ScrollView style={ms.list}>
            {options.map((o) => {
              const isSel = selected === o.value;
              return (
                <Pressable
                  key={o.value}
                  style={[ms.item, isSel && { backgroundColor: "rgba(168,85,247,0.12)" }]}
                  onPress={() => {
                    onSelect(o.value);
                    onClose();
                  }}
                >
                  <Ionicons
                    name={isSel ? "radio-button-on" : "radio-button-off"}
                    size={20}
                    color={isSel ? "#A855F7" : colors.text.muted}
                  />
                  <Text style={[ms.itemText, { color: colors.text.primary }]}>{o.label}</Text>
                </Pressable>
              );
            })}
          </ScrollView>
        </Pressable>
      </Pressable>
    </Modal>
  );
}

/* ======================================================
   FILTER BUTTON
====================================================== */
function DropdownButton({
  label,
  value,
  onPress,
  colors,
}: {
  label: string;
  value: string;
  onPress: () => void;
  colors: any;
}) {
  return (
    <View style={fb.wrap}>
      <Text style={[fb.label, { color: colors.text.muted }]}>{label}</Text>
      <Pressable
        onPress={onPress}
        style={[fb.btn, { backgroundColor: colors.surface.elevated, borderColor: colors.border.subtle }]}
      >
        <Text style={[fb.btnText, { color: colors.text.primary }]} numberOfLines={1}>
          {value}
        </Text>
        <Ionicons name="chevron-down" size={14} color={colors.text.muted} />
      </Pressable>
    </View>
  );
}

/* ======================================================
   FILTERS MODAL (IMG_0461)
====================================================== */
type FilterState = {
  overUnder: OverUnder;
  sortDir: SortDir;
  categories: Set<string>;  // category values
  games: Set<string>;       // game_ids
  teams: Set<string>;
  positions: Set<string>;
  minOdds: number;
  maxOdds: number;
  minOppRank: number;
  maxOppRank: number;
  minPfRating: number;
  maxPfRating: number;
  showAltLines: boolean;
};

function FiltersModal({
  visible,
  onClose,
  state,
  setState,
  allCategories,
  allGames,
  allTeams,
  allPositions,
  totalProps,
  matchingProps,
  onApply,
  onReset,
  colors,
}: {
  visible: boolean;
  onClose: () => void;
  state: FilterState;
  setState: (s: FilterState) => void;
  allCategories: ResearchCategory[];
  allGames: ResearchGame[];
  allTeams: string[];
  allPositions: string[];
  totalProps: number;
  matchingProps: number;
  onApply: () => void;
  onReset: () => void;
  colors: any;
}) {
  const [catModal, setCatModal] = useState(false);
  const [gameModal, setGameModal] = useState(false);
  const [teamModal, setTeamModal] = useState(false);
  const [posModal, setPosModal] = useState(false);

  const toggleSet = (set: Set<string>, v: string): Set<string> => {
    const next = new Set(set);
    if (next.has(v)) next.delete(v);
    else next.add(v);
    return next;
  };

  const catOptions = allCategories.map((c) => ({ label: c.label, value: c.value }));
  const teamOptions = allTeams.map((t) => ({ label: t, value: t }));
  const posOptions = allPositions.map((p) => ({ label: p, value: p }));

  const gameOptions = allGames
    .slice()
    .sort((a, b) => (a.sort_key || "").localeCompare(b.sort_key || ""))
    .map((g) => ({
      label: `${g.date_label} - ${g.time_label} ${g.favorite_label || `${g.away_team_code} @ ${g.home_team_code}`}`,
      value: g.game_id,
    }));

  const gameLookup = useMemo(() => {
    const m: Record<string, ResearchGame> = {};
    for (const g of allGames) m[g.game_id] = g;
    return m;
  }, [allGames]);

  const catLabel =
    state.categories.size === allCategories.length
      ? "All categories"
      : `${state.categories.size} selected`;
  const gameLabel =
    state.games.size === allGames.length
      ? "All games"
      : `${state.games.size} selected`;
  const teamLabel =
    state.teams.size === allTeams.length ? "All Teams" : `${state.teams.size} teams`;
  const posLabel =
    state.positions.size === allPositions.length ? "All Positions" : `${state.positions.size} pos`;

  return (
    <Modal visible={visible} transparent animationType="slide" onRequestClose={onClose}>
      <View style={fm.overlay}>
        <View style={[fm.sheet, { backgroundColor: colors.surface.screen }]}>
          <View style={[fm.header, { borderBottomColor: colors.border.subtle }]}>
            <Pressable onPress={onClose}>
              <Ionicons name="close" size={24} color={colors.text.muted} />
            </Pressable>
            <Text style={[fm.title, { color: colors.text.primary }]}>Filters</Text>
            <View style={{ width: 24 }} />
          </View>

          <ScrollView style={{ flex: 1 }} contentContainerStyle={{ padding: 16 }}>
            {/* Over / Under */}
            <View style={fm.pillRow}>
              <Pressable
                style={[fm.pill, state.overUnder === "over" && fm.pillActive]}
                onPress={() => setState({ ...state, overUnder: "over" })}
              >
                <Text style={[fm.pillText, state.overUnder === "over" && fm.pillTextActive]}>Over</Text>
              </Pressable>
              <Pressable
                style={[fm.pill, state.overUnder === "under" && fm.pillActive]}
                onPress={() => setState({ ...state, overUnder: "under" })}
              >
                <Text style={[fm.pillText, state.overUnder === "under" && fm.pillTextActive]}>Under</Text>
              </Pressable>
            </View>

            {/* ASC / DESC */}
            <View style={fm.pillRow}>
              <Pressable
                style={[fm.pill, state.sortDir === "asc" && fm.pillActiveAlt]}
                onPress={() => setState({ ...state, sortDir: "asc" })}
              >
                <Text style={[fm.pillText, state.sortDir === "asc" && fm.pillTextActive]}>ASC</Text>
              </Pressable>
              <Pressable
                style={[fm.pill, state.sortDir === "desc" && fm.pillActiveAlt]}
                onPress={() => setState({ ...state, sortDir: "desc" })}
              >
                <Text style={[fm.pillText, state.sortDir === "desc" && fm.pillTextActive]}>DESC</Text>
              </Pressable>
            </View>

            <View style={fm.grid2}>
              <DropdownButton label="Categories" value={catLabel} onPress={() => setCatModal(true)} colors={colors} />
              <DropdownButton label="Games" value={gameLabel} onPress={() => setGameModal(true)} colors={colors} />
            </View>

            <View style={fm.grid2}>
              <DropdownButton label="Team" value={teamLabel} onPress={() => setTeamModal(true)} colors={colors} />
              <DropdownButton label="Position" value={posLabel} onPress={() => setPosModal(true)} colors={colors} />
            </View>

            {/* Min/Max Odds */}
            <View style={fm.grid2}>
              <NumberField
                label="Min Odds"
                value={state.minOdds}
                onChange={(v) => setState({ ...state, minOdds: v })}
                colors={colors}
              />
              <NumberField
                label="Max Odds"
                value={state.maxOdds}
                onChange={(v) => setState({ ...state, maxOdds: v })}
                colors={colors}
              />
            </View>

            <View style={fm.grid2}>
              <NumberField
                label="Min Opposing Rank"
                value={state.minOppRank}
                onChange={(v) => setState({ ...state, minOppRank: v })}
                colors={colors}
              />
              <NumberField
                label="Max Opposing Rank"
                value={state.maxOppRank}
                onChange={(v) => setState({ ...state, maxOppRank: v })}
                colors={colors}
              />
            </View>

            <View style={fm.grid2}>
              <NumberField
                label="Min PF Rating"
                value={state.minPfRating}
                onChange={(v) => setState({ ...state, minPfRating: v })}
                colors={colors}
              />
              <NumberField
                label="Max PF Rating"
                value={state.maxPfRating}
                onChange={(v) => setState({ ...state, maxPfRating: v })}
                colors={colors}
              />
            </View>

            {/* Show Alt Lines */}
            <View style={[fm.altRow, { borderColor: colors.border.subtle }]}>
              <Text style={[fm.altLabel, { color: colors.text.primary }]}>Show Alt Lines</Text>
              <Switch
                value={state.showAltLines}
                onValueChange={(v) => setState({ ...state, showAltLines: v })}
                trackColor={{ false: "#3F3F46", true: "#A855F7" }}
                thumbColor="#fff"
              />
              <Text style={[fm.altCount, { color: "#A855F7" }]}>
                {matchingProps}/{totalProps} Props
              </Text>
            </View>

            {/* Buttons */}
            <View style={fm.btnRow}>
              <Pressable style={[fm.btnReset, { borderColor: "#A855F7" }]} onPress={onReset}>
                <Text style={[fm.btnResetText, { color: "#A855F7" }]}>Reset</Text>
              </Pressable>
              <Pressable style={fm.btnApply} onPress={onApply}>
                <Text style={fm.btnApplyText}>See Results</Text>
              </Pressable>
            </View>
          </ScrollView>
        </View>
      </View>

      {/* Nested modals */}
      <MultiSelectModal
        visible={catModal}
        title="Categories"
        options={catOptions}
        selected={state.categories}
        onToggle={(v) => setState({ ...state, categories: toggleSet(state.categories, v) })}
        onToggleAll={() =>
          setState({
            ...state,
            categories:
              state.categories.size === catOptions.length
                ? new Set()
                : new Set(catOptions.map((o) => o.value)),
          })
        }
        onClose={() => setCatModal(false)}
        colors={colors}
      />

      <MultiSelectModal
        visible={gameModal}
        title="Games"
        options={gameOptions}
        selected={state.games}
        onToggle={(v) => setState({ ...state, games: toggleSet(state.games, v) })}
        onToggleAll={() =>
          setState({
            ...state,
            games:
              state.games.size === gameOptions.length
                ? new Set()
                : new Set(gameOptions.map((o) => o.value)),
          })
        }
        onClose={() => setGameModal(false)}
        colors={colors}
        customRender={(o) => {
          const g = gameLookup[o.value];
          if (!g) {
            return <Text style={[ms.itemText, { color: colors.text.primary }]}>{o.label}</Text>;
          }
          return (
            <View style={ms.gameRow}>
              <Text style={[ms.gameDate, { color: colors.text.primary }]}>{g.date_label}</Text>
              <Text style={[ms.gameTime, { color: colors.text.muted }]}>{g.time_label}</Text>
              <View style={ms.gameTeams}>
                {TEAM_LOGOS[g.away_team_code] && (
                  <Image source={{ uri: TEAM_LOGOS[g.away_team_code] }} style={ms.logo} />
                )}
                <Text style={[ms.gameFav, { color: colors.text.primary }]}>
                  {g.favorite_label || `${g.away_team_code} @ ${g.home_team_code}`}
                </Text>
                {TEAM_LOGOS[g.home_team_code] && (
                  <Image source={{ uri: TEAM_LOGOS[g.home_team_code] }} style={ms.logo} />
                )}
              </View>
            </View>
          );
        }}
      />

      <MultiSelectModal
        visible={teamModal}
        title="Teams"
        options={teamOptions}
        selected={state.teams}
        onToggle={(v) => setState({ ...state, teams: toggleSet(state.teams, v) })}
        onToggleAll={() =>
          setState({
            ...state,
            teams:
              state.teams.size === teamOptions.length
                ? new Set()
                : new Set(teamOptions.map((o) => o.value)),
          })
        }
        onClose={() => setTeamModal(false)}
        colors={colors}
      />

      <MultiSelectModal
        visible={posModal}
        title="Positions"
        options={posOptions}
        selected={state.positions}
        onToggle={(v) => setState({ ...state, positions: toggleSet(state.positions, v) })}
        onToggleAll={() =>
          setState({
            ...state,
            positions:
              state.positions.size === posOptions.length
                ? new Set()
                : new Set(posOptions.map((o) => o.value)),
          })
        }
        onClose={() => setPosModal(false)}
        colors={colors}
      />
    </Modal>
  );
}

function NumberField({
  label,
  value,
  onChange,
  colors,
}: {
  label: string;
  value: number;
  onChange: (v: number) => void;
  colors: any;
}) {
  return (
    <View style={fb.wrap}>
      <Text style={[fb.label, { color: colors.text.muted }]}>{label}</Text>
      <TextInput
        value={String(value)}
        onChangeText={(t) => {
          const n = Number(t.replace(/[^\-0-9.]/g, ""));
          onChange(Number.isFinite(n) ? n : 0);
        }}
        keyboardType="numbers-and-punctuation"
        style={[fb.input, { color: colors.text.primary, backgroundColor: colors.surface.elevated, borderColor: colors.border.subtle }]}
      />
    </View>
  );
}

/* ======================================================
   PLAYER ROW (IMG_0460)
====================================================== */
function PropRow({
  prop,
  expanded,
  onToggle,
  onAddToBetslip,
  colors,
}: {
  prop: ResearchProp;
  expanded: boolean;
  onToggle: () => void;
  onAddToBetslip: (p: ResearchProp, book: "DraftKings" | "FanDuel") => void;
  colors: any;
}) {
  const posLabel = prop.position || "";
  const catLabel = CATEGORY_SHORT_LABEL[prop.category] ?? prop.category;
  const overSide = (prop.over_under || "over").toLowerCase();
  const sideLetter = overSide === "under" ? "u" : "o";
  const lineLabel = `${sideLetter}${fmtLine(prop.line)} ${catLabel}`;
  const pfColors = pfRatingColor(prop.pf_rating);
  const oppColor = opponentRankColor(prop.matchup_rank);
  const oppLabel = prop.opp_team_code || "OPP";

  const seasonColors = hitRateColor(prop.hit_rate_season);
  const h2hColors = hitRateColor(prop.hit_rate_vs_team);
  const l5Colors = hitRateColor(prop.hit_rate_l5);
  const l10Colors = hitRateColor(prop.hit_rate_l10);
  const l20Colors = hitRateColor(prop.hit_rate_l20);

  const l10AvgLabel = fmtAvg(prop.avg_l10);
  const venueLabel = prop.is_home === false ? "AWAY" : "HOME";

  // Best odds shown in badge (either DK or FD → pick best)
  const showDk = prop.dk_price != null;
  const showFd = prop.fd_price != null;

  return (
    <Pressable onPress={onToggle} style={[row.wrapper, { borderColor: colors.border.subtle }]}>
      {/* Top row: avatar | name/meta | PFR | odds */}
      <View style={row.top}>
        <View style={row.avatar}>
          <Ionicons name="person-circle" size={34} color={colors.text.muted} />
        </View>
        <View style={row.nameCol}>
          <View style={row.nameRow}>
            <Text style={[row.name, { color: colors.text.primary }]} numberOfLines={1}>
              {prop.player_name}
            </Text>
            {posLabel ? (
              <Text style={[row.pos, { color: colors.text.muted }]}>{posLabel}</Text>
            ) : null}
          </View>
          <Text style={[row.line, { color: colors.text.muted }]} numberOfLines={1}>
            {lineLabel}
          </Text>
        </View>

        {/* Odds + PFR (stacked) */}
        <View style={row.rightCol}>
          <View style={row.oddsCluster}>
            {showDk && (
              <Pressable
                onPress={(e) => {
                  e.stopPropagation?.();
                  if (prop.dk_deep_link) Linking.openURL(prop.dk_deep_link);
                }}
                style={[row.oddsBtn, { backgroundColor: "rgba(59,125,60,0.25)", borderColor: "#3B7D3C" }]}
              >
                <Text style={row.bookTag}>DK</Text>
                <Text style={row.odds}>{fmtOdds(prop.dk_price)}</Text>
              </Pressable>
            )}
            {showFd && (
              <Pressable
                onPress={(e) => {
                  e.stopPropagation?.();
                  if (prop.fd_deep_link) Linking.openURL(prop.fd_deep_link);
                }}
                style={[row.oddsBtn, { backgroundColor: "rgba(26,82,118,0.3)", borderColor: "#1A5276" }]}
              >
                <Text style={row.bookTag}>FD</Text>
                <Text style={row.odds}>{fmtOdds(prop.fd_price)}</Text>
              </Pressable>
            )}
          </View>
          <View style={[row.pfrBadge, { backgroundColor: pfColors.bg, borderColor: pfColors.border }]}>
            <Text style={[row.pfrText, { color: pfColors.text }]}>
              PFR {prop.pf_rating != null ? prop.pf_rating.toFixed(1) : "—"}
            </Text>
          </View>
        </View>
      </View>

      {/* Stats grid (scrollable horizontally) */}
      <ScrollView horizontal showsHorizontalScrollIndicator={false} style={row.gridScroll}>
        <View style={row.grid}>
          {/* SZN OPP DEF */}
          <View style={[row.cell, { borderColor: colors.border.subtle }]}>
            <Text style={[row.cellHead, { color: colors.text.muted }]}>
              SZN {oppLabel} DEF
            </Text>
            {prop.matchup_rank != null ? (
              <Text style={[row.cellVal, { color: oppColor, fontWeight: "800" }]}>
                Rank {prop.matchup_rank}
              </Text>
            ) : (
              <Text style={[row.cellVal, { color: colors.text.muted }]}>—</Text>
            )}
          </View>

          {/* L10 */}
          <HitCell
            head="L10"
            fraction={fmtHitFrac(prop.hit_rate_l10_raw, prop.hit_rate_l10)}
            pct={fmtHitPct(prop.hit_rate_l10)}
            colors={l10Colors}
            textColors={colors}
          />
          {/* Season */}
          <HitCell
            head="'25-'26"
            fraction={fmtHitFrac(prop.hit_rate_season_raw, prop.hit_rate_season)}
            pct={fmtHitPct(prop.hit_rate_season)}
            colors={seasonColors}
            textColors={colors}
          />
          {/* H2H */}
          <HitCell
            head="H2H"
            fraction={fmtHitFrac(prop.hit_rate_vs_team_raw, prop.hit_rate_vs_team)}
            pct={fmtHitPct(prop.hit_rate_vs_team)}
            colors={h2hColors}
            textColors={colors}
          />
          {/* L5 */}
          <HitCell
            head="L5"
            fraction={fmtHitFrac(prop.hit_rate_l5_raw, prop.hit_rate_l5)}
            pct={fmtHitPct(prop.hit_rate_l5)}
            colors={l5Colors}
            textColors={colors}
          />
          {/* L20 */}
          <HitCell
            head="L20"
            fraction={fmtHitFrac(prop.hit_rate_l20_raw, prop.hit_rate_l20)}
            pct={fmtHitPct(prop.hit_rate_l20)}
            colors={l20Colors}
            textColors={colors}
          />
        </View>
      </ScrollView>

      {/* Average footer (Streak + L10 avg + venue avg + vs opp avg) */}
      <View style={row.footer}>
        {prop.streak != null && prop.streak > 0 ? (
          <View style={row.streakPill}>
            <Text style={row.streakText}>Streak: {prop.streak}</Text>
          </View>
        ) : null}
        {prop.avg_l10 != null && (
          <Text style={[row.avgLabel, { color: colors.text.muted }]}>
            <Text style={[row.avgValue, { color: colors.text.primary }]}>{l10AvgLabel}</Text> L10
          </Text>
        )}
        {prop.avg_home_away != null && (
          <Text style={[row.avgLabel, { color: colors.text.muted }]}>
            <Text style={[row.avgValue, { color: colors.text.primary }]}>{fmtAvg(prop.avg_home_away)}</Text> {venueLabel}
          </Text>
        )}
        {prop.avg_vs_opponent != null && (
          <Text style={[row.avgLabel, { color: colors.text.muted }]}>
            <Text style={[row.avgValue, { color: colors.text.primary }]}>{fmtAvg(prop.avg_vs_opponent)}</Text> vs {oppLabel}
          </Text>
        )}
      </View>

      {expanded ? (
        <View style={[row.expanded, { borderTopColor: colors.border.subtle }]}>
          {showDk ? (
            <Pressable
              style={[row.addBtn, { backgroundColor: "#3B7D3C" }]}
              onPress={(e) => {
                e.stopPropagation?.();
                onAddToBetslip(prop, "DraftKings");
                if (Platform.OS !== "web") Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light);
              }}
            >
              <Ionicons name="add-circle-outline" size={14} color="#fff" />
              <Text style={row.addBtnText}>Add DK</Text>
            </Pressable>
          ) : null}
          {showFd ? (
            <Pressable
              style={[row.addBtn, { backgroundColor: "#1A5276" }]}
              onPress={(e) => {
                e.stopPropagation?.();
                onAddToBetslip(prop, "FanDuel");
                if (Platform.OS !== "web") Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light);
              }}
            >
              <Ionicons name="add-circle-outline" size={14} color="#fff" />
              <Text style={row.addBtnText}>Add FD</Text>
            </Pressable>
          ) : null}
        </View>
      ) : null}
    </Pressable>
  );
}

function HitCell({
  head,
  fraction,
  pct,
  colors,
  textColors,
}: {
  head: string;
  fraction: string;
  pct: string;
  colors: { bg: string; border: string };
  textColors: any;
}) {
  return (
    <View style={[row.cell, { backgroundColor: colors.bg, borderColor: colors.border }]}>
      <Text style={[row.cellHead, { color: textColors.text.muted }]}>{head}</Text>
      <Text style={[row.cellVal, { color: "#fff", fontWeight: "800" }]}>{fraction}</Text>
      <Text style={[row.cellSub, { color: "#fff" }]}>{pct}</Text>
    </View>
  );
}

/* ======================================================
   MAIN SCREEN
====================================================== */
export function NbaResearchScreen() {
  const { colors } = useTheme();
  const { data, loading, error, refreshedAt, cacheSource, refetch } = useNbaResearch();
  const { add: addToBetslip } = usePropBetslip();
  const { open: openBetslip } = useBetslipDrawer();

  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("pfRating");
  const [expandedPropId, setExpandedPropId] = useState<string | null>(null);
  const [filtersOpen, setFiltersOpen] = useState(false);

  const allCategories = data?.categories ?? [];
  const allTeams = data?.teams ?? [];
  const allGames = data?.games ?? [];
  const allPositions = useMemo(() => {
    const avail = new Set(data?.positions ?? []);
    // Maintain canonical order, only include positions actually present
    return POSITIONS_FIXED.filter((p) => avail.has(p)).concat(
      (data?.positions ?? []).filter((p) => !POSITIONS_FIXED.includes(p)).sort(),
    );
  }, [data?.positions]);

  const [filters, setFilters] = useState<FilterState>({
    overUnder: "over",
    sortDir: "desc",
    categories: new Set(),
    games: new Set(),
    teams: new Set(),
    positions: new Set(),
    minOdds: -200000,
    maxOdds: 200000,
    minOppRank: 0,
    maxOppRank: 265,
    minPfRating: 0,
    maxPfRating: 100,
    showAltLines: false,
  });

  // Populate selections once data lands
  useEffect(() => {
    if (!data) return;
    setFilters((f) => ({
      ...f,
      categories: f.categories.size === 0 ? new Set(data.categories.map((c) => c.value)) : f.categories,
      games: f.games.size === 0 ? new Set(data.games.map((g) => g.game_id)) : f.games,
      teams: f.teams.size === 0 ? new Set(data.teams) : f.teams,
      positions: f.positions.size === 0 ? new Set(allPositions) : f.positions,
    }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data?.refreshed_at]);

  const resetFilters = () => {
    if (!data) return;
    setFilters({
      overUnder: "over",
      sortDir: "desc",
      categories: new Set(data.categories.map((c) => c.value)),
      games: new Set(data.games.map((g) => g.game_id)),
      teams: new Set(data.teams),
      positions: new Set(allPositions),
      minOdds: -200000,
      maxOdds: 200000,
      minOppRank: 0,
      maxOppRank: 265,
      minPfRating: 0,
      maxPfRating: 100,
      showAltLines: false,
    });
  };

  const filteredProps = useMemo(() => {
    if (!data) return [];
    const allProps = data.props;
    const q = search.trim().toLowerCase();
    const filtered = allProps.filter((p) => {
      if (p.over_under !== filters.overUnder) return false;
      if (!filters.showAltLines && p.is_alternate) return false;
      if (filters.categories.size > 0 && !filters.categories.has(p.category)) return false;
      if (filters.games.size > 0 && p.game_id && !filters.games.has(p.game_id)) return false;
      if (filters.teams.size > 0 && p.team_code && !filters.teams.has(p.team_code)) return false;
      if (filters.positions.size > 0 && p.position && !filters.positions.has(p.position)) return false;
      const price = p.best_price;
      if (price != null) {
        if (price < filters.minOdds || price > filters.maxOdds) return false;
      }
      if (p.matchup_rank != null) {
        if (p.matchup_rank < filters.minOppRank || p.matchup_rank > filters.maxOppRank) return false;
      }
      if (p.pf_rating != null) {
        if (p.pf_rating < filters.minPfRating || p.pf_rating > filters.maxPfRating) return false;
      }
      if (q && !(p.player_name || "").toLowerCase().includes(q)) return false;
      return true;
    });
    return applySort(filtered, sortKey, filters.sortDir);
  }, [data, filters, sortKey, search]);

  const matchingCount = useMemo(() => {
    if (!data) return 0;
    return data.props.filter(
      (p) =>
        p.over_under === filters.overUnder &&
        (filters.showAltLines || !p.is_alternate),
    ).length;
  }, [data, filters.overUnder, filters.showAltLines]);

  const totalCount = data?.props.length ?? 0;

  const handleSortChipPress = useCallback((key: SortKey) => {
    setSortKey((prev) => {
      if (prev === key) {
        setFilters((f) => ({ ...f, sortDir: f.sortDir === "desc" ? "asc" : "desc" }));
        return prev;
      }
      setFilters((f) => ({ ...f, sortDir: "desc" }));
      return key;
    });
  }, []);

  const handleAddToBetslip = useCallback(
    (prop: ResearchProp, book: "DraftKings" | "FanDuel") => {
      const market = CATEGORY_MARKET_MAP[prop.category] ?? prop.category;
      const isDK = book === "DraftKings";
      const side = (prop.over_under || "over").toLowerCase() as "over" | "under";
      const item: PropSlipItem = {
        id: `nba-research-${prop.prop_id}-${book}`,
        player_id: Number(prop.player_id) || 0,
        player: prop.player_name,
        market,
        side,
        line: prop.line ?? 0,
        odds: (isDK ? prop.dk_price : prop.fd_price) ?? 0,
        matchup: `${prop.team_code ?? ""} vs ${prop.opp_team_code ?? ""}`,
        bookmaker: book,
        dk_event_id: prop.dk_event_id ?? undefined,
        dk_outcome_code: prop.dk_outcome_code ?? undefined,
        fd_market_id: prop.fd_market_id ?? undefined,
        fd_selection_id: prop.fd_selection_id ?? undefined,
      };
      addToBetslip(item);
      openBetslip();
    },
    [addToBetslip, openBetslip],
  );

  return (
    <View style={[styles.screen, { backgroundColor: colors.surface.screen }]}>
      {/* Search + filter */}
      <View style={[styles.searchRow, { borderBottomColor: colors.border.subtle }]}>
        <View style={[styles.searchBox, { backgroundColor: colors.surface.elevated, borderColor: colors.border.subtle }]}>
          <Ionicons name="search" size={16} color={colors.text.muted} />
          <TextInput
            placeholder="Search Player"
            placeholderTextColor={colors.text.muted}
            value={search}
            onChangeText={setSearch}
            style={[styles.searchInput, { color: colors.text.primary }]}
          />
        </View>
        <Pressable
          onPress={() => setFiltersOpen(true)}
          style={[styles.filterBtn, { backgroundColor: "#A855F7" }]}
        >
          <Ionicons name="options-outline" size={14} color="#fff" />
          <Text style={styles.filterBtnText}>Filters</Text>
        </Pressable>
      </View>

      {/* Updated at */}
      <View style={{ paddingHorizontal: 16, paddingTop: 4 }}>
        <LastUpdatedBadge refreshedAt={refreshedAt} cacheSource={cacheSource} />
      </View>

      {/* Sort chips */}
      <ScrollView
        horizontal
        showsHorizontalScrollIndicator={false}
        contentContainerStyle={styles.chipsRow}
      >
        {SORT_CHIPS.map((chip) => {
          const isActive = sortKey === chip.key;
          return (
            <Pressable
              key={chip.key}
              onPress={() => handleSortChipPress(chip.key)}
              style={[
                styles.chip,
                isActive && { backgroundColor: "#A855F7", borderColor: "#A855F7" },
                !isActive && { borderColor: colors.border.subtle, backgroundColor: colors.surface.elevated },
              ]}
            >
              <Text
                style={[
                  styles.chipText,
                  { color: isActive ? "#fff" : colors.text.primary },
                ]}
              >
                {chip.label}
              </Text>
              {isActive && (
                <Ionicons
                  name={filters.sortDir === "desc" ? "arrow-down" : "arrow-up"}
                  size={12}
                  color="#fff"
                />
              )}
            </Pressable>
          );
        })}
      </ScrollView>

      {loading ? (
        <View style={styles.center}>
          <ActivityIndicator size="large" color="#A855F7" />
          <Text style={[styles.dim, { color: colors.text.muted, marginTop: 8 }]}>Loading research…</Text>
        </View>
      ) : error ? (
        <View style={styles.center}>
          <Ionicons name="alert-circle" size={32} color="#EF4444" />
          <Text style={[styles.dim, { color: "#EF4444", marginTop: 8 }]}>{error}</Text>
          <Pressable style={styles.retryBtn} onPress={refetch}>
            <Text style={styles.retryText}>Retry</Text>
          </Pressable>
        </View>
      ) : filteredProps.length === 0 ? (
        <View style={styles.center}>
          <Text style={[styles.dim, { color: colors.text.muted }]}>No props match the current filters.</Text>
        </View>
      ) : (
        <ScrollView style={{ flex: 1 }} contentContainerStyle={{ paddingBottom: 40 }}>
          {filteredProps.map((p) => (
            <PropRow
              key={p.prop_id}
              prop={p}
              expanded={expandedPropId === p.prop_id}
              onToggle={() => setExpandedPropId((cur) => (cur === p.prop_id ? null : p.prop_id))}
              onAddToBetslip={handleAddToBetslip}
              colors={colors}
            />
          ))}
        </ScrollView>
      )}

      <FiltersModal
        visible={filtersOpen}
        onClose={() => setFiltersOpen(false)}
        state={filters}
        setState={setFilters}
        allCategories={allCategories}
        allGames={allGames}
        allTeams={allTeams}
        allPositions={allPositions}
        totalProps={totalCount}
        matchingProps={matchingCount}
        onApply={() => setFiltersOpen(false)}
        onReset={resetFilters}
        colors={colors}
      />

      <PropBetslipDrawer />
    </View>
  );
}

/* ======================================================
   STYLES
====================================================== */
const styles = StyleSheet.create({
  screen: { flex: 1 },
  searchRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: 12,
    paddingVertical: 10,
    gap: 8,
    borderBottomWidth: StyleSheet.hairlineWidth,
  },
  searchBox: {
    flex: 1,
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: 10,
    borderRadius: 8,
    borderWidth: StyleSheet.hairlineWidth,
    gap: 6,
  },
  searchInput: {
    flex: 1,
    fontSize: 13,
    paddingVertical: 8,
  },
  filterBtn: {
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: 12,
    paddingVertical: 8,
    borderRadius: 8,
    gap: 6,
  },
  filterBtnText: { color: "#fff", fontSize: 12, fontWeight: "700" },
  chipsRow: {
    paddingHorizontal: 12,
    paddingVertical: 10,
    gap: 8,
  },
  chip: {
    flexDirection: "row",
    alignItems: "center",
    gap: 4,
    paddingHorizontal: 12,
    paddingVertical: 6,
    borderRadius: 999,
    borderWidth: StyleSheet.hairlineWidth,
  },
  chipText: { fontSize: 12, fontWeight: "700" },
  center: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    padding: 32,
  },
  dim: { fontSize: 13, textAlign: "center" },
  retryBtn: {
    marginTop: 12,
    backgroundColor: "#A855F7",
    paddingHorizontal: 16,
    paddingVertical: 8,
    borderRadius: 8,
  },
  retryText: { color: "#fff", fontWeight: "700" },
});

const row = StyleSheet.create({
  wrapper: {
    borderBottomWidth: StyleSheet.hairlineWidth,
    paddingVertical: 10,
    paddingHorizontal: 12,
  },
  top: { flexDirection: "row", alignItems: "center", gap: 10 },
  avatar: { width: 36, alignItems: "center" },
  nameCol: { flex: 1 },
  nameRow: { flexDirection: "row", alignItems: "center", gap: 6 },
  name: { fontSize: 14, fontWeight: "800" },
  pos: { fontSize: 11, fontWeight: "700" },
  line: { fontSize: 11, marginTop: 1 },
  rightCol: { alignItems: "flex-end", gap: 4 },
  oddsCluster: { flexDirection: "row", gap: 4 },
  oddsBtn: {
    flexDirection: "row",
    alignItems: "center",
    gap: 4,
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 6,
    borderWidth: 1,
  },
  bookTag: { color: "#fff", fontSize: 9, fontWeight: "800" },
  odds: { color: "#fff", fontSize: 12, fontWeight: "800" },
  pfrBadge: {
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderRadius: 6,
    borderWidth: 1,
  },
  pfrText: { fontSize: 11, fontWeight: "800" },
  gridScroll: { marginTop: 10 },
  grid: { flexDirection: "row", gap: 6 },
  cell: {
    minWidth: 72,
    paddingHorizontal: 8,
    paddingVertical: 6,
    borderRadius: 6,
    borderWidth: StyleSheet.hairlineWidth,
    alignItems: "center",
  },
  cellHead: { fontSize: 9, fontWeight: "700", textTransform: "uppercase", letterSpacing: 0.5 },
  cellVal: { fontSize: 12, marginTop: 2 },
  cellSub: { fontSize: 10, marginTop: 1, fontWeight: "700" },
  footer: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
    marginTop: 8,
    flexWrap: "wrap",
  },
  streakPill: {
    backgroundColor: "rgba(234,179,8,0.25)",
    borderWidth: 1,
    borderColor: "#EAB308",
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderRadius: 6,
  },
  streakText: { color: "#FDE68A", fontSize: 11, fontWeight: "800" },
  avgLabel: { fontSize: 11, fontWeight: "600" },
  avgValue: { fontWeight: "800" },
  expanded: {
    flexDirection: "row",
    gap: 8,
    marginTop: 10,
    paddingTop: 10,
    borderTopWidth: StyleSheet.hairlineWidth,
  },
  addBtn: {
    flexDirection: "row",
    alignItems: "center",
    gap: 4,
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 6,
  },
  addBtnText: { color: "#fff", fontSize: 11, fontWeight: "700" },
});

const fb = StyleSheet.create({
  wrap: { flex: 1, marginVertical: 6 },
  label: {
    fontSize: 10,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.5,
    marginBottom: 4,
  },
  btn: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    borderRadius: 8,
    borderWidth: StyleSheet.hairlineWidth,
    paddingHorizontal: 10,
    paddingVertical: 10,
  },
  btnText: { fontSize: 13, fontWeight: "600", flex: 1, marginRight: 6 },
  input: {
    borderRadius: 8,
    borderWidth: StyleSheet.hairlineWidth,
    paddingHorizontal: 10,
    paddingVertical: 10,
    fontSize: 13,
    fontWeight: "700",
  },
});

const fm = StyleSheet.create({
  overlay: { flex: 1, backgroundColor: "rgba(0,0,0,0.5)", justifyContent: "flex-end" },
  sheet: { flex: 1, marginTop: 40, borderTopLeftRadius: 16, borderTopRightRadius: 16 },
  header: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingHorizontal: 16,
    paddingVertical: 14,
    borderBottomWidth: StyleSheet.hairlineWidth,
  },
  title: { fontSize: 16, fontWeight: "800" },
  pillRow: { flexDirection: "row", gap: 8, marginBottom: 12 },
  pill: {
    flex: 1,
    paddingVertical: 10,
    borderRadius: 8,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#3F3F46",
    alignItems: "center",
  },
  pillActive: { backgroundColor: "#A855F7", borderColor: "#A855F7" },
  pillActiveAlt: { backgroundColor: "#A855F7", borderColor: "#A855F7" },
  pillText: { color: "#A1A1AA", fontSize: 13, fontWeight: "800" },
  pillTextActive: { color: "#fff" },
  grid2: { flexDirection: "row", gap: 10 },
  altRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
    marginTop: 14,
    paddingVertical: 12,
    paddingHorizontal: 10,
    borderRadius: 8,
    borderWidth: StyleSheet.hairlineWidth,
  },
  altLabel: { fontSize: 13, fontWeight: "700", flex: 1 },
  altCount: { fontSize: 12, fontWeight: "800" },
  btnRow: { flexDirection: "row", gap: 10, marginTop: 16 },
  btnReset: {
    flex: 1,
    borderWidth: 1,
    borderRadius: 8,
    paddingVertical: 12,
    alignItems: "center",
  },
  btnResetText: { fontSize: 14, fontWeight: "800" },
  btnApply: {
    flex: 2,
    backgroundColor: "#A855F7",
    borderRadius: 8,
    paddingVertical: 12,
    alignItems: "center",
  },
  btnApplyText: { color: "#fff", fontSize: 14, fontWeight: "800" },
});

const ms = StyleSheet.create({
  overlay: { flex: 1, backgroundColor: "rgba(0,0,0,0.55)", justifyContent: "center", alignItems: "center", padding: 20 },
  modal: { width: "100%", maxWidth: 420, maxHeight: "80%", borderRadius: 14, overflow: "hidden" },
  header: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingHorizontal: 16,
    paddingVertical: 12,
    borderBottomWidth: StyleSheet.hairlineWidth,
  },
  title: { fontSize: 15, fontWeight: "800" },
  list: { maxHeight: 480 },
  item: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
    paddingHorizontal: 16,
    paddingVertical: 12,
  },
  itemText: { fontSize: 13, fontWeight: "600" },
  gameRow: { flex: 1, flexDirection: "row", alignItems: "center", gap: 8 },
  gameDate: { fontSize: 12, fontWeight: "800", minWidth: 40 },
  gameTime: { fontSize: 11, fontWeight: "600", minWidth: 58 },
  gameTeams: { flex: 1, flexDirection: "row", alignItems: "center", gap: 6 },
  gameFav: { fontSize: 12, fontWeight: "700" },
  logo: { width: 18, height: 18 },
});
