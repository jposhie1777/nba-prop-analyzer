"""Shared configuration for NBA pipeline."""

import os

# ── GCP / BigQuery ────────────────────────────────────────────────────────────
PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "graphite-flare-477419-h7")
DATASET = "nba"

# ── PropFinder ────────────────────────────────────────────────────────────────
PF_BASE = "https://api.propfinder.app"
# Read PropFinder creds from env (set via GitHub Actions secrets / Cloud Run env).
PF_EMAIL = os.getenv("PROPFINDER_EMAIL", "")
PF_PASSWORD = os.getenv("PROPFINDER_PASSWORD", "")

# ── Discord (read from env only — DO NOT commit tokens to the repo) ──────────
DISCORD_BOT_TOKEN = os.getenv("DISCORD_NBA_BOT_TOKEN") or os.getenv("DISCORD_BOT_TOKEN") or ""
DISCORD_API = "https://discord.com/api/v10"

# Webhook (fallback) — set DISCORD_NBA_WEBHOOK via env
DISCORD_NBA_WEBHOOK = os.getenv("DISCORD_NBA_WEBHOOK", "")

# NBA channel IDs (hardcoded — env var overrides if set)
CHANNELS = {
    "moneyline":       os.getenv("DISCORD_NBA_ML_CHANNEL_ID",    "1494157159427215452"),
    "spread":          os.getenv("DISCORD_NBA_SPREAD_CHANNEL_ID", "1494157200778858586"),
    "total":           os.getenv("DISCORD_NBA_TOTAL_CHANNEL_ID",  "1494157237864763442"),
    "player-points":   os.getenv("DISCORD_NBA_PTS_CHANNEL_ID",    "1494157283159179325"),
    "player-rebounds": os.getenv("DISCORD_NBA_REB_CHANNEL_ID",    "1494157332836384988"),
    "player-assists":  os.getenv("DISCORD_NBA_AST_CHANNEL_ID",    "1494157379309142146"),
    "player-combos":   os.getenv("DISCORD_NBA_COMBO_CHANNEL_ID",  "1494157436674904096"),
    "player-threes":   os.getenv("DISCORD_NBA_3PM_CHANNEL_ID",    "1494157539674165268"),
}

# Prop category -> discord channel mapping
PROP_CHANNEL_MAP = {
    "points":                   "player-points",
    "rebounds":                 "player-rebounds",
    "assists":                  "player-assists",
    "threePointsMade":          "player-threes",
    "pointsRebounds":          "player-combos",
    "pointsAssists":           "player-combos",
    "reboundAssists":          "player-combos",
    "pointsReboundsAssists":   "player-combos",
}

# Only DK and FD
BOOKS_FILTER = {"DraftKings", "FanDuel"}

# ── Grading ───────────────────────────────────────────────────────────────────
PULSE_FIRE = 80
PULSE_STRONG = 65
PULSE_LEAN = 50

GRADE_LABELS = {
    "FIRE":   (80, 100),
    "STRONG": (65, 79),
    "LEAN":   (50, 64),
    "SKIP":   (0, 49),
}

GRADE_COLORS = {
    "FIRE":   0xEF4444,   # red
    "STRONG": 0x22C55E,   # green
    "LEAN":   0xF59E0B,   # amber
}

GRADE_EMOJI = {
    "FIRE":   "\U0001f525",   # fire
    "STRONG": "\U0001f7e2",   # green circle
    "LEAN":   "\U0001f7e1",   # yellow circle
}

# ── Learning ──────────────────────────────────────────────────────────────────
LEARNING_RATE = 0.15
MIN_SAMPLE_SIZE = 20
WEIGHT_FLOOR = 1.0
WEIGHT_CEILING = 25.0
