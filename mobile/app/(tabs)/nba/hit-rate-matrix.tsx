// app/(tabs)/nba/hit-rate-matrix.tsx
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  Pressable,
  ActivityIndicator,
  Platform,
  Linking,
  Modal,
} from "react-native";
import { useState, useMemo, useCallback, useEffect } from "react";
import { Stack } from "expo-router";
import { Ionicons } from "@expo/vector-icons";
import * as Haptics from "expo-haptics";

import { useTheme } from "@/store/useTheme";
import { useBetslipDrawer } from "@/store/useBetslipDrawer";
import { usePropBetslip, PropSlipItem } from "@/store/usePropBetslip";
import { PropBetslipDrawer } from "@/components/prop/PropBetslipDrawer";
import {
  useHitRateMatrix,
  HitRatePlayer,
} from "@/hooks/useHitRateMatrix";

/* ======================================================
   CONSTANTS
====================================================== */
const CATEGORIES = [
  { label: "Points", value: "points" },
  { label: "Rebounds", value: "rebounds" },
  { label: "Assists", value: "assists" },
  { label: "3-Pointers", value: "threePointsMade" },
  { label: "Steals", value: "steals" },
  { label: "Blocks", value: "blocks" },
  { label: "Pts+Reb+Ast", value: "pointsReboundsAssists" },
  { label: "Pts+Reb", value: "pointsRebounds" },
  { label: "Pts+Ast", value: "pointsAssists" },
  { label: "Reb+Ast", value: "reboundAssists" },
];

const POSITIONS = [
  { label: "All Positions", value: "all" },
  { label: "PG", value: "PG" },
  { label: "SG", value: "SG" },
  { label: "SF", value: "SF" },
  { label: "PF", value: "PF" },
  { label: "C", value: "C" },
];

const GAME_COUNTS = [
  { label: "Last 5 Games", value: "L5" },
  { label: "Last 10 Games", value: "L10" },
  { label: "Last 15 Games", value: "L15" },
];

const CATEGORY_MARKET_MAP: Record<string, string> = {
  points: "pts",
  rebounds: "reb",
  assists: "ast",
  threePointsMade: "3pm",
  steals: "stl",
  blocks: "blk",
  pointsReboundsAssists: "pra",
  pointsRebounds: "pr",
  pointsAssists: "pa",
  reboundAssists: "ra",
};

/* ======================================================
   HELPERS
====================================================== */
function getHitRateColor(hit: number, total: number): string {
  if (total === 0) return "rgba(255,255,255,0.05)";
  const pct = hit / total;
  if (pct >= 0.8) return "rgba(22,163,74,0.75)";   // deep green
  if (pct >= 0.6) return "rgba(34,197,94,0.55)";   // green
  if (pct >= 0.4) return "rgba(234,179,8,0.35)";   // yellow-ish
  if (pct >= 0.2) return "rgba(239,68,68,0.45)";   // red
  return "rgba(220,38,38,0.65)";                     // deep red
}

function getHitRateBg(hit: number, total: number): string {
  if (total === 0) return "transparent";
  const pct = hit / total;
  if (pct >= 0.8) return "#166534";
  if (pct >= 0.6) return "#15803d";
  if (pct >= 0.4) return "#854d0e";
  if (pct >= 0.2) return "#991b1b";
  return "#7f1d1d";
}

function fmtOdds(v: number | null | undefined): string {
  if (v == null) return "\u2014";
  return v > 0 ? `+${v}` : String(v);
}

function calcEV(hit: number, total: number, odds: number | null): number | null {
  if (total === 0 || odds == null) return null;
  const pct = hit / total;
  const decimal = odds > 0 ? 1 + odds / 100 : 1 + 100 / Math.abs(odds);
  return Math.round((pct * (decimal - 1) - (1 - pct)) * 100);
}

/* ======================================================
   DROPDOWN MODAL (renders options as a bottom-sheet overlay)
====================================================== */
function DropdownModal({
  visible,
  title,
  options,
  value,
  onSelect,
  onClose,
  colors,
}: {
  visible: boolean;
  title: string;
  options: { label: string; value: string; selected?: boolean }[];
  value?: string;
  onSelect: (v: string) => void;
  onClose: () => void;
  colors: any;
}) {
  return (
    <Modal
      visible={visible}
      transparent
      animationType="fade"
      onRequestClose={onClose}
    >
      <Pressable style={dd.overlay} onPress={onClose}>
        <View
          style={[
            dd.modal,
            { backgroundColor: colors.surface.elevated },
          ]}
        >
          <Text style={[dd.modalTitle, { color: colors.text.primary }]}>
            {title}
          </Text>
          <ScrollView style={dd.modalScroll} bounces={false}>
            {options.map((o) => {
              const isActive = o.selected !== undefined ? o.selected : o.value === value;
              return (
                <Pressable
                  key={o.value}
                  onPress={() => {
                    onSelect(o.value);
                    if (Platform.OS !== "web") Haptics.selectionAsync();
                  }}
                  style={[
                    dd.modalItem,
                    isActive && { backgroundColor: colors.accent.primary + "22" },
                    { borderBottomColor: colors.border.subtle },
                  ]}
                >
                  <Text
                    style={[
                      dd.modalItemText,
                      { color: isActive ? colors.accent.primary : colors.text.primary },
                    ]}
                  >
                    {o.label}
                  </Text>
                  {isActive && (
                    <Ionicons name="checkmark" size={18} color={colors.accent.primary} />
                  )}
                </Pressable>
              );
            })}
          </ScrollView>
        </View>
      </Pressable>
    </Modal>
  );
}

/* ======================================================
   DROPDOWN BUTTON (opens modal on tap)
====================================================== */
function Dropdown({
  label,
  options,
  value,
  onChange,
  colors,
}: {
  label: string;
  options: { label: string; value: string }[];
  value: string;
  onChange: (v: string) => void;
  colors: any;
}) {
  const [open, setOpen] = useState(false);
  const selected = options.find((o) => o.value === value);

  return (
    <View style={dd.container}>
      <Text style={[dd.label, { color: colors.text.muted }]}>{label}</Text>
      <Pressable
        onPress={() => setOpen(true)}
        style={[
          dd.button,
          {
            backgroundColor: colors.surface.elevated,
            borderColor: colors.border.subtle,
          },
        ]}
      >
        <Text style={[dd.buttonText, { color: colors.text.primary }]}>
          {selected?.label ?? value}
        </Text>
        <Ionicons name="chevron-down" size={14} color={colors.text.muted} />
      </Pressable>

      <DropdownModal
        visible={open}
        title={label}
        options={options}
        value={value}
        onSelect={(v) => {
          onChange(v);
          setOpen(false);
        }}
        onClose={() => setOpen(false)}
        colors={colors}
      />
    </View>
  );
}

/* ======================================================
   GAMES MULTI-SELECT DROPDOWN
====================================================== */
function GamesDropdown({
  games,
  selectedIds,
  onChange,
  colors,
}: {
  games: { game_id: string; label: string }[];
  selectedIds: string[];
  onChange: (ids: string[]) => void;
  colors: any;
}) {
  const [open, setOpen] = useState(false);
  const allSelected = selectedIds.length === 0 || selectedIds.length === games.length;
  const displayText = allSelected
    ? "All selected"
    : `${selectedIds.length} game${selectedIds.length !== 1 ? "s" : ""}`;

  const toggleGame = (id: string) => {
    if (id === "__all__") {
      onChange([]);
    } else if (selectedIds.includes(id)) {
      onChange(selectedIds.filter((g) => g !== id));
    } else {
      onChange([...selectedIds, id]);
    }
  };

  const modalOptions = [
    { label: "All Games", value: "__all__", selected: allSelected },
    ...games.map((g) => ({
      label: g.label,
      value: g.game_id,
      selected: selectedIds.includes(g.game_id),
    })),
  ];

  return (
    <View style={dd.container}>
      <Text style={[dd.label, { color: colors.text.muted }]}>Select Games</Text>
      <Pressable
        onPress={() => setOpen(true)}
        style={[
          dd.button,
          {
            backgroundColor: colors.surface.elevated,
            borderColor: colors.border.subtle,
          },
        ]}
      >
        <Text style={[dd.buttonText, { color: colors.text.primary }]}>
          {displayText}
        </Text>
        <Ionicons name="chevron-down" size={14} color={colors.text.muted} />
      </Pressable>

      <DropdownModal
        visible={open}
        title="Select Games"
        options={modalOptions}
        onSelect={toggleGame}
        onClose={() => setOpen(false)}
        colors={colors}
      />
    </View>
  );
}

/* ======================================================
   MATRIX CELL
====================================================== */
function MatrixCell({
  hit,
  total,
  odds,
  colors,
}: {
  hit: number;
  total: number;
  odds: number | null;
  colors: any;
}) {
  const ev = calcEV(hit, total, odds);
  const bg = getHitRateBg(hit, total);

  return (
    <View style={[tbl.cell, { backgroundColor: bg }]}>
      <Text style={[tbl.cellFraction, { color: "#fff" }]}>
        {total === 0 ? "0/0" : `${hit}/${total}`}
      </Text>
      {ev !== null && (
        <View style={tbl.evRow}>
          <Text
            style={[
              tbl.cellEv,
              { color: ev >= 0 ? "#4ade80" : "#f87171" },
            ]}
          >
            {ev >= 0 ? `+${ev}` : ev}
          </Text>
          <Text style={{ fontSize: 8, marginLeft: 2 }}>
            {ev >= 0 ? "\u{1F7E2}" : "\u{1F534}"}
          </Text>
        </View>
      )}
    </View>
  );
}

/* ======================================================
   PLAYER ROW
====================================================== */
function PlayerRow({
  player,
  thresholds,
  category,
  colors,
  onSave,
}: {
  player: HitRatePlayer;
  thresholds: number[];
  category: string;
  colors: any;
  onSave: (player: HitRatePlayer) => void;
}) {
  const matchupColor =
    player.matchup_rank != null && player.matchup_rank <= 10
      ? colors.accent.success
      : player.matchup_rank != null && player.matchup_rank <= 20
        ? colors.accent.warning
        : colors.accent.danger;

  return (
    <View style={[tbl.row, { borderBottomColor: colors.border.subtle }]}>
      {/* PLAYER */}
      <Pressable
        style={[tbl.playerCell, { backgroundColor: colors.surface.card }]}
        onPress={() => onSave(player)}
      >
        <Text style={[tbl.playerName, { color: colors.text.primary }]} numberOfLines={1}>
          {player.player_name}
        </Text>
      </Pressable>

      {/* LINE */}
      <View style={[tbl.lineCell, { backgroundColor: colors.surface.card }]}>
        <Text style={[tbl.lineText, { color: colors.text.primary }]}>
          {player.line}
        </Text>
      </View>

      {/* SZN MATCHUP */}
      <View style={[tbl.sznCell, { backgroundColor: colors.surface.card }]}>
        {player.matchup_rank != null ? (
          <>
            <Text style={[tbl.sznRank, { color: matchupColor }]}>
              Rank {player.matchup_rank}
            </Text>
            <Text style={[tbl.sznPos, { color: colors.text.muted }]}>
              vs {player.position}
            </Text>
          </>
        ) : (
          <Text style={[tbl.sznPos, { color: colors.text.muted }]}>—</Text>
        )}
      </View>

      {/* THRESHOLD CELLS */}
      {thresholds.map((t) => {
        const cell = player.cells[String(t)] ?? { hit: 0, total: 0 };
        return (
          <MatrixCell
            key={t}
            hit={cell.hit}
            total={cell.total}
            odds={player.best_price}
            colors={colors}
          />
        );
      })}
    </View>
  );
}

/* ======================================================
   ODDS BADGE (DK / FD)
====================================================== */
function OddsBadge({
  label,
  price,
  onPress,
  colors,
}: {
  label: string;
  price: number | null;
  onPress: () => void;
  colors: any;
}) {
  if (price == null) return null;
  return (
    <Pressable
      onPress={onPress}
      style={[
        styles.oddsBadge,
        {
          backgroundColor:
            label === "DK" ? "#3B7D3C" : "#1A5276",
          borderColor: label === "DK" ? "#4CAF50" : "#2196F3",
        },
      ]}
    >
      <Text style={styles.oddsBadgeLabel}>{label}</Text>
      <Text style={styles.oddsBadgePrice}>{fmtOdds(price)}</Text>
    </Pressable>
  );
}

/* ======================================================
   EXPANDED ROW (ODDS + DEEP LINKS)
====================================================== */
function ExpandedRow({
  player,
  category,
  colors,
  onAddToBetslip,
}: {
  player: HitRatePlayer;
  category: string;
  colors: any;
  onAddToBetslip: (player: HitRatePlayer, book: string) => void;
}) {
  const openDeepLink = (url: string | null) => {
    if (url) Linking.openURL(url);
  };

  return (
    <View
      style={[
        styles.expandedRow,
        {
          backgroundColor: colors.surface.cardSoft,
          borderBottomColor: colors.border.subtle,
        },
      ]}
    >
      <View style={styles.expandedContent}>
        <View style={styles.oddsSection}>
          <Text style={[styles.oddsTitle, { color: colors.text.secondary }]}>
            {player.player_name} — Over {player.line}
          </Text>
          <View style={styles.oddsRow}>
            <OddsBadge
              label="DK"
              price={player.dk_price}
              onPress={() => openDeepLink(player.dk_deep_link)}
              colors={colors}
            />
            <OddsBadge
              label="FD"
              price={player.fd_price}
              onPress={() => openDeepLink(player.fd_deep_link)}
              colors={colors}
            />
          </View>
        </View>

        <View style={styles.actionsSection}>
          {player.dk_price != null && (
            <Pressable
              style={[styles.addBtn, { backgroundColor: "#3B7D3C" }]}
              onPress={() => {
                onAddToBetslip(player, "DraftKings");
                if (Platform.OS !== "web") Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light);
              }}
            >
              <Ionicons name="add-circle-outline" size={14} color="#fff" />
              <Text style={styles.addBtnText}>DK Slip</Text>
            </Pressable>
          )}
          {player.fd_price != null && (
            <Pressable
              style={[styles.addBtn, { backgroundColor: "#1A5276" }]}
              onPress={() => {
                onAddToBetslip(player, "FanDuel");
                if (Platform.OS !== "web") Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light);
              }}
            >
              <Ionicons name="add-circle-outline" size={14} color="#fff" />
              <Text style={styles.addBtnText}>FD Slip</Text>
            </Pressable>
          )}
        </View>

        {/* Game values sparkline */}
        {player.game_values.length > 0 && (
          <View style={styles.gameValuesRow}>
            <Text style={[styles.gameValuesLabel, { color: colors.text.muted }]}>
              Recent:
            </Text>
            {player.game_values.map((v, i) => (
              <Text
                key={i}
                style={[
                  styles.gameValue,
                  {
                    color: v >= player.line ? "#4ade80" : "#f87171",
                    backgroundColor:
                      v >= player.line
                        ? "rgba(34,197,94,0.15)"
                        : "rgba(239,68,68,0.15)",
                  },
                ]}
              >
                {v}
              </Text>
            ))}
          </View>
        )}
      </View>
    </View>
  );
}

/* ======================================================
   MAIN SCREEN
====================================================== */
export default function HitRateMatrixScreen() {
  const { colors } = useTheme();
  const { add: addToBetslip } = usePropBetslip();
  const { open: openBetslip } = useBetslipDrawer();

  // Filter state
  const [category, setCategory] = useState("points");
  const [position, setPosition] = useState("all");
  const [gameCount, setGameCount] = useState("L5");
  const [selectedGameIds, setSelectedGameIds] = useState<string[]>([]);
  const [expandedPlayerId, setExpandedPlayerId] = useState<string | null>(null);
  const [sortColumn, setSortColumn] = useState<string | null>(null); // "player", "line", or a threshold like "10"
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  const { data, loading, error } = useHitRateMatrix({
    category,
    position,
    gameCount,
    gameIds: selectedGameIds.length > 0 ? selectedGameIds : undefined,
  });

  // Reset expanded when data changes
  useEffect(() => {
    setExpandedPlayerId(null);
  }, [category, position, gameCount, selectedGameIds]);

  const handleSort = useCallback((col: string) => {
    if (sortColumn === col) {
      setSortDir((d) => (d === "desc" ? "asc" : "desc"));
    } else {
      setSortColumn(col);
      setSortDir("desc");
    }
  }, [sortColumn]);

  // Filter + sort players
  const filteredPlayers = useMemo(() => {
    let list = data?.players ?? [];
    if (selectedGameIds.length > 0) {
      list = list.filter((p) => selectedGameIds.includes(p.game_id));
    }
    if (!sortColumn) return list;

    const sorted = [...list].sort((a, b) => {
      let aVal: number;
      let bVal: number;

      if (sortColumn === "player") {
        return sortDir === "asc"
          ? a.player_name.localeCompare(b.player_name)
          : b.player_name.localeCompare(a.player_name);
      } else if (sortColumn === "line") {
        aVal = a.line ?? 0;
        bVal = b.line ?? 0;
      } else {
        // Threshold column — sort by hit rate (hit/total)
        const aCel = a.cells[sortColumn] ?? { hit: 0, total: 0 };
        const bCel = b.cells[sortColumn] ?? { hit: 0, total: 0 };
        aVal = aCel.total > 0 ? aCel.hit / aCel.total : -1;
        bVal = bCel.total > 0 ? bCel.hit / bCel.total : -1;
      }

      return sortDir === "desc" ? bVal - aVal : aVal - bVal;
    });

    return sorted;
  }, [data?.players, selectedGameIds, sortColumn, sortDir]);

  const handleSave = useCallback(
    (player: HitRatePlayer) => {
      setExpandedPlayerId((prev) =>
        prev === player.player_id ? null : player.player_id
      );
      if (Platform.OS !== "web") Haptics.selectionAsync();
    },
    []
  );

  const handleAddToBetslip = useCallback(
    (player: HitRatePlayer, book: string) => {
      const market = CATEGORY_MARKET_MAP[category] ?? category;
      const isDK = book === "DraftKings";
      const item: PropSlipItem = {
        id: `hrm-${player.player_id}-${market}-${book}`,
        player_id: Number(player.player_id) || 0,
        player: player.player_name,
        market,
        side: "over",
        line: player.line,
        odds: (isDK ? player.dk_price : player.fd_price) ?? 0,
        matchup: `${player.team_code} vs ${player.opp_team_code}`,
        bookmaker: book,
        dk_event_id: player.dk_event_id,
        dk_outcome_code: player.dk_outcome_code,
        fd_market_id: player.fd_market_id,
        fd_selection_id: player.fd_selection_id,
      };
      addToBetslip(item);
      openBetslip();
    },
    [category, addToBetslip, openBetslip]
  );

  const thresholds = data?.thresholds ?? [];
  const games = data?.games ?? [];

  return (
    <View style={[styles.screen, { backgroundColor: colors.surface.screen }]}>
      <Stack.Screen options={{ headerShown: false }} />

      {/* ── HEADER ──────────────────────────────────── */}
      <View
        style={[
          styles.header,
          {
            backgroundColor: colors.surface.card,
            borderBottomColor: colors.border.subtle,
          },
        ]}
      >
        <Text style={[styles.title, { color: colors.text.primary }]}>
          NBA Hit Rate Matrix
        </Text>
        <Text style={[styles.subtitle, { color: colors.text.muted }]}>
          Track player performance against key statistical milestones and find
          betting opportunities
        </Text>
      </View>

      {/* ── FILTERS ─────────────────────────────────── */}
      <View style={styles.filtersWrapper}>
        <ScrollView
          horizontal
          showsHorizontalScrollIndicator={false}
          contentContainerStyle={styles.filtersRow}
        >
          <Dropdown
            label="Category"
            options={CATEGORIES}
            value={category}
            onChange={setCategory}
            colors={colors}
          />
          <Dropdown
            label="Position"
            options={POSITIONS}
            value={position}
            onChange={setPosition}
            colors={colors}
          />
          <GamesDropdown
            games={games}
            selectedIds={selectedGameIds}
            onChange={setSelectedGameIds}
            colors={colors}
          />
          <Dropdown
            label="Game Count"
            options={GAME_COUNTS}
            value={gameCount}
            onChange={setGameCount}
            colors={colors}
          />
        </ScrollView>
      </View>

      {/* ── MATRIX TABLE ────────────────────────────── */}
      {loading ? (
        <View style={styles.center}>
          <ActivityIndicator size="large" color={colors.accent.primary} />
          <Text style={[styles.loadingText, { color: colors.text.muted }]}>
            Loading hit rate data...
          </Text>
        </View>
      ) : error ? (
        <View style={styles.center}>
          <Ionicons name="alert-circle" size={32} color={colors.accent.danger} />
          <Text style={[styles.errorText, { color: colors.accent.danger }]}>
            {error}
          </Text>
        </View>
      ) : filteredPlayers.length === 0 ? (
        <View style={styles.center}>
          <Text style={[styles.emptyText, { color: colors.text.muted }]}>
            No players found for this selection
          </Text>
        </View>
      ) : (
        <ScrollView style={styles.tableWrapper}>
          <ScrollView horizontal showsHorizontalScrollIndicator={true}>
            <View>
              {/* TABLE HEADER */}
              <View
                style={[
                  tbl.headerRow,
                  {
                    backgroundColor: colors.surface.elevated,
                    borderBottomColor: colors.border.strong,
                  },
                ]}
              >
                <Pressable style={tbl.playerCell} onPress={() => handleSort("player")}>
                  <View style={tbl.headerInner}>
                    <Text style={[tbl.headerText, { color: sortColumn === "player" ? colors.accent.primary : colors.text.secondary }]}>
                      PLAYER
                    </Text>
                    {sortColumn === "player" && (
                      <Ionicons name={sortDir === "desc" ? "arrow-down" : "arrow-up"} size={10} color={colors.accent.primary} />
                    )}
                  </View>
                </Pressable>
                <Pressable style={tbl.lineCell} onPress={() => handleSort("line")}>
                  <View style={tbl.headerInner}>
                    <Text style={[tbl.headerText, { color: sortColumn === "line" ? colors.accent.primary : colors.text.secondary }]}>
                      LINE
                    </Text>
                    {sortColumn === "line" && (
                      <Ionicons name={sortDir === "desc" ? "arrow-down" : "arrow-up"} size={10} color={colors.accent.primary} />
                    )}
                  </View>
                </Pressable>
                <View style={tbl.sznCell}>
                  <Text style={[tbl.headerText, { color: colors.text.secondary }]}>
                    SZN MATCHUP
                  </Text>
                </View>
                {thresholds.map((t) => (
                  <Pressable key={t} style={tbl.cell} onPress={() => handleSort(String(t))}>
                    <View style={tbl.headerInner}>
                      <Text
                        style={[tbl.headerText, { color: sortColumn === String(t) ? colors.accent.primary : colors.text.secondary }]}
                      >
                        {t}+
                      </Text>
                      {sortColumn === String(t) && (
                        <Ionicons name={sortDir === "desc" ? "arrow-down" : "arrow-up"} size={10} color={colors.accent.primary} />
                      )}
                    </View>
                  </Pressable>
                ))}
              </View>

              {/* TABLE BODY */}
              {filteredPlayers.map((player) => (
                <View key={`${player.player_id}-${player.line}`}>
                  <PlayerRow
                    player={player}
                    thresholds={thresholds}
                    category={category}
                    colors={colors}
                    onSave={handleSave}
                  />
                  {expandedPlayerId === player.player_id && (
                    <ExpandedRow
                      player={player}
                      category={category}
                      colors={colors}
                      onAddToBetslip={handleAddToBetslip}
                    />
                  )}
                </View>
              ))}
            </View>
          </ScrollView>
        </ScrollView>
      )}

      {/* ── BETSLIP DRAWER ──────────────────────────── */}
      <PropBetslipDrawer />
    </View>
  );
}

/* ======================================================
   DROPDOWN STYLES
====================================================== */
const dd = StyleSheet.create({
  container: {
    marginRight: 12,
  },
  label: {
    fontSize: 10,
    fontWeight: "600",
    textTransform: "uppercase",
    letterSpacing: 0.5,
    marginBottom: 4,
  },
  button: {
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: 12,
    paddingVertical: 8,
    borderRadius: 8,
    borderWidth: StyleSheet.hairlineWidth,
    minWidth: 130,
    justifyContent: "space-between",
  },
  buttonText: {
    fontSize: 13,
    fontWeight: "600",
    marginRight: 8,
  },
  overlay: {
    flex: 1,
    backgroundColor: "rgba(0,0,0,0.6)",
    justifyContent: "center",
    alignItems: "center",
    padding: 32,
  },
  modal: {
    width: "100%",
    maxWidth: 340,
    borderRadius: 14,
    paddingTop: 16,
    paddingBottom: 8,
    maxHeight: "70%",
  },
  modalTitle: {
    fontSize: 16,
    fontWeight: "800",
    textAlign: "center",
    marginBottom: 12,
    paddingHorizontal: 16,
  },
  modalScroll: {
    maxHeight: 400,
  },
  modalItem: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingHorizontal: 20,
    paddingVertical: 14,
    borderBottomWidth: StyleSheet.hairlineWidth,
  },
  modalItemText: {
    fontSize: 15,
    fontWeight: "600",
  },
});

/* ======================================================
   TABLE STYLES
====================================================== */
const PLAYER_W = 140;
const LINE_W = 60;
const SZN_W = 90;
const CELL_W = 72;

const tbl = StyleSheet.create({
  headerRow: {
    flexDirection: "row",
    borderBottomWidth: 1,
    height: 38,
    alignItems: "center",
  },
  row: {
    flexDirection: "row",
    borderBottomWidth: StyleSheet.hairlineWidth,
    minHeight: 52,
    alignItems: "center",
  },
  headerText: {
    fontSize: 11,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.5,
    textAlign: "center",
  },
  headerInner: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "center",
    gap: 3,
  },
  playerCell: {
    width: PLAYER_W,
    paddingHorizontal: 10,
    paddingVertical: 8,
    justifyContent: "center",
  },
  playerName: {
    fontSize: 12,
    fontWeight: "700",
  },
  lineCell: {
    width: LINE_W,
    alignItems: "center",
    justifyContent: "center",
    paddingVertical: 8,
  },
  lineText: {
    fontSize: 13,
    fontWeight: "700",
  },
  sznCell: {
    width: SZN_W,
    alignItems: "center",
    justifyContent: "center",
    paddingVertical: 4,
  },
  sznRank: {
    fontSize: 11,
    fontWeight: "700",
  },
  sznPos: {
    fontSize: 9,
    marginTop: 1,
  },
  cell: {
    width: CELL_W,
    alignItems: "center",
    justifyContent: "center",
    paddingVertical: 6,
    paddingHorizontal: 4,
  },
  cellFraction: {
    fontSize: 13,
    fontWeight: "800",
  },
  evRow: {
    flexDirection: "row",
    alignItems: "center",
    marginTop: 2,
  },
  cellEv: {
    fontSize: 9,
    fontWeight: "600",
  },
});

/* ======================================================
   MAIN STYLES
====================================================== */
const styles = StyleSheet.create({
  screen: {
    flex: 1,
  },
  header: {
    paddingHorizontal: 16,
    paddingTop: 12,
    paddingBottom: 14,
    borderBottomWidth: StyleSheet.hairlineWidth,
  },
  title: {
    fontSize: 20,
    fontWeight: "800",
    textAlign: "center",
  },
  subtitle: {
    fontSize: 12,
    textAlign: "center",
    marginTop: 4,
  },
  filtersWrapper: {
    zIndex: 50,
    paddingVertical: 10,
    paddingHorizontal: 8,
  },
  filtersRow: {
    flexDirection: "row",
    alignItems: "flex-start",
    paddingHorizontal: 4,
  },
  tableWrapper: {
    flex: 1,
  },
  center: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    padding: 40,
  },
  loadingText: {
    marginTop: 12,
    fontSize: 14,
  },
  errorText: {
    marginTop: 8,
    fontSize: 14,
    textAlign: "center",
  },
  emptyText: {
    fontSize: 14,
    textAlign: "center",
  },

  /* Expanded row */
  expandedRow: {
    borderBottomWidth: StyleSheet.hairlineWidth,
    paddingHorizontal: 14,
    paddingVertical: 10,
  },
  expandedContent: {},
  oddsSection: {
    marginBottom: 8,
  },
  oddsTitle: {
    fontSize: 12,
    fontWeight: "600",
    marginBottom: 6,
  },
  oddsRow: {
    flexDirection: "row",
    gap: 8,
  },
  oddsBadge: {
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: 10,
    paddingVertical: 5,
    borderRadius: 6,
    borderWidth: 1,
    gap: 6,
  },
  oddsBadgeLabel: {
    fontSize: 10,
    fontWeight: "800",
    color: "#fff",
  },
  oddsBadgePrice: {
    fontSize: 13,
    fontWeight: "700",
    color: "#fff",
  },
  actionsSection: {
    flexDirection: "row",
    gap: 8,
    marginBottom: 8,
  },
  addBtn: {
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 6,
    gap: 4,
  },
  addBtnText: {
    fontSize: 11,
    fontWeight: "700",
    color: "#fff",
  },
  gameValuesRow: {
    flexDirection: "row",
    alignItems: "center",
    flexWrap: "wrap",
    gap: 4,
  },
  gameValuesLabel: {
    fontSize: 10,
    fontWeight: "600",
    marginRight: 4,
  },
  gameValue: {
    fontSize: 11,
    fontWeight: "700",
    paddingHorizontal: 6,
    paddingVertical: 2,
    borderRadius: 4,
    overflow: "hidden",
  },
});
