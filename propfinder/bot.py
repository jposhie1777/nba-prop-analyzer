"""
bot.py — Discord bot for HR pick alerts with interactive parlay building.

Users tap DK/FD buttons on individual picks to add them to a personal
parlay cart, then use /parlay to generate a combined deeplink.
"""

import asyncio
import logging
import os
from aiohttp import web
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import quote

import discord
from discord import app_commands
from google.cloud import bigquery

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ── Config ──────────────────────────────────────────────────────────────────
TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
CHANNEL_ID = int(os.getenv("DISCORD_HR_CHANNEL_ID", "1493058403990507622"))
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "1460293607847231633"))

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "graphite-flare-477419-h7")
DATASET = "propfinder"
HR_TABLE = f"{PROJECT}.{DATASET}.hr_picks_daily"

ET = ZoneInfo("America/New_York")

# Custom emoji IDs
DK_EMOJI = discord.PartialEmoji(name="dk", id=1493069919766708295)
FD_EMOJI = discord.PartialEmoji(name="fd", id=1493070566809403593)

GRADE_COLORS = {"IDEAL": 0x22C55E, "FAVORABLE": 0xF59E0B}
GRADE_EMOJI = {"IDEAL": "\U0001f7e2", "FAVORABLE": "\U0001f7e1"}
WEAK_SPOT_COLOR = 0xEF4444

COMPASS = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
           "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]


def _wind_str(speed, direction):
    if not speed or int(speed) == 0:
        return ""
    spd = int(speed)
    if direction is not None:
        idx = round(int(direction) / 22.5) % 16
        return f"{spd} mph {COMPASS[idx]}"
    return f"{spd} mph"


def _weather_circle(indicator, precip_prob):
    ind = (indicator or "").lower()
    if ind == "red" or (precip_prob and precip_prob >= 50):
        return "\U0001f534"
    if ind == "yellow" or (precip_prob and precip_prob >= 25):
        return "\U0001f7e1"
    return "\U0001f7e2"


def _conditions_label(conditions, precip_prob):
    c = (conditions or "").strip()
    if c:
        return c
    if precip_prob and precip_prob >= 50:
        return "Rain likely"
    if precip_prob and precip_prob >= 25:
        return "Chance of rain"
    return "Clear"


def _venue_weather_short(p):
    parts = []
    park = p.get("ballpark_name") or ""
    roof = (p.get("roof_type") or "").lower()
    if park:
        parts.append(park)
    wind = _wind_str(p.get("wind_speed"), p.get("wind_dir"))
    if wind and "retractable" not in roof and "dome" not in roof:
        parts.append(wind)
    elif "dome" in roof or "retractable" in roof:
        parts.append("Dome")
    forecast = _conditions_label(p.get("conditions"), p.get("precip_prob"))
    circle = _weather_circle(p.get("weather_indicator"), p.get("precip_prob"))
    parts.append(f"{circle} {forecast}")
    return " | ".join(parts)


def _stats_line(p):
    parts = []
    iso = p.get("iso")
    if iso is not None:
        parts.append(f"ISO {iso:.3f}")
    barrel = p.get("l15_barrel_pct")
    if barrel is not None:
        parts.append(f"Barrel {barrel:.0f}%")
    ev = p.get("l15_ev")
    if ev is not None:
        parts.append(f"EV {ev:.1f}")
    hh = p.get("l15_hard_hit_pct")
    if hh is not None:
        parts.append(f"HH {hh:.0f}%")
    hrfb = p.get("hr_fb_pct")
    if hrfb is not None:
        parts.append(f"HR/FB {hrfb:.0f}%")
    bvp_ab = p.get("bvp_ab")
    bvp_hits = p.get("bvp_hits")
    bvp_hr = p.get("bvp_hr")
    if bvp_ab and bvp_ab > 0:
        bvp_str = f"BvP {bvp_hits}-{bvp_ab}"
        if bvp_hr and bvp_hr > 0:
            bvp_str += f", {bvp_hr} HR"
        parts.append(bvp_str)
    phr9 = p.get("p_hr9_vs_hand")
    if phr9 is not None:
        parts.append(f"P-HR/9 {phr9:.2f}")
    return " | ".join(parts)

# ── State ───────────────────────────────────────────────────────────────────
# pick_id -> pick data (populated when alerts are sent)
pick_store: dict[str, dict] = {}

# user_id -> {"dk": {pick_id: pick_data}, "fd": {pick_id: pick_data}}
user_carts: dict[int, dict[str, dict[str, dict]]] = {}


# ── Helpers ─────────────────────────────────────────────────────────────────

def _hand_label(side):
    s = (side or "R").upper()
    return "LHB" if s == "L" else "SHB" if s == "S" else "RHB"

def _pitcher_hand(side):
    s = (side or "R").upper()
    return "LHP" if s.startswith("L") else "RHP"

def _fmt_odds(val):
    if val is None:
        return "\u2014"
    v = int(val)
    return f"+{v}" if v > 0 else str(v)

def _build_dk_parlay(picks):
    """Build a DraftKings parlay deeplink from multiple picks."""
    codes = []
    event_id = None
    for p in picks:
        code = p.get("dk_outcome_code")
        eid = p.get("dk_event_id")
        if not code or not eid:
            return None
        codes.append(code)
        if event_id is None:
            event_id = eid
    if not codes:
        return None
    return f"https://sportsbook.draftkings.com/event/{event_id}?outcomes={','.join(codes)}"

def _build_fd_parlay(picks):
    """Build a FanDuel parlay deeplink from multiple picks."""
    parts = []
    for i, p in enumerate(picks):
        mid = p.get("fd_market_id")
        sid = p.get("fd_selection_id")
        if not mid or not sid:
            return None
        parts.append(f"marketId[{i}]={quote(str(mid))}&selectionId[{i}]={quote(str(sid))}")
    if not parts:
        return None
    return f"https://sportsbook.fanduel.com/addToBetslip?{'&'.join(parts)}"

def _parlay_odds(picks):
    """Calculate combined parlay odds (American format)."""
    decimals = []
    for p in picks:
        odds = p.get("hr_odds_best_price")
        if not odds or not isinstance(odds, (int, float)):
            continue
        if odds > 0:
            decimals.append(odds / 100 + 1)
        else:
            decimals.append(100 / abs(odds) + 1)
    if not decimals:
        return None
    combined = 1
    for d in decimals:
        combined *= d
    if combined >= 2:
        return f"+{int(round((combined - 1) * 100))}"
    else:
        return str(int(round(-100 / (combined - 1))))


# ── Pick Button View ───────────────────────────────────────────────────────

class PickView(discord.ui.View):
    def __init__(self, pick_id: str):
        super().__init__(timeout=None)
        self.pick_id = pick_id

        dk_btn = discord.ui.Button(
            emoji=DK_EMOJI,
            label="DK",
            style=discord.ButtonStyle.secondary,
            custom_id=f"dk:{pick_id}",
        )
        dk_btn.callback = self.dk_callback
        self.add_item(dk_btn)

        fd_btn = discord.ui.Button(
            emoji=FD_EMOJI,
            label="FD",
            style=discord.ButtonStyle.secondary,
            custom_id=f"fd:{pick_id}",
        )
        fd_btn.callback = self.fd_callback
        self.add_item(fd_btn)

    async def dk_callback(self, interaction: discord.Interaction):
        await self._toggle(interaction, "dk")

    async def fd_callback(self, interaction: discord.Interaction):
        await self._toggle(interaction, "fd")

    async def _toggle(self, interaction: discord.Interaction, book: str):
        uid = interaction.user.id
        pick = pick_store.get(self.pick_id)
        if not pick:
            await interaction.response.send_message("Pick no longer available.", ephemeral=True)
            return

        cart = user_carts.setdefault(uid, {"dk": {}, "fd": {}})
        book_cart = cart[book]
        batter = pick.get("batter_name", "?")
        book_label = "DraftKings" if book == "dk" else "FanDuel"

        if self.pick_id in book_cart:
            del book_cart[self.pick_id]
            count = len(book_cart)
            await interaction.response.send_message(
                f"\u274c Removed **{batter}** from {book_label} parlay ({count} leg{'s' if count != 1 else ''})",
                ephemeral=True,
            )
        else:
            book_cart[self.pick_id] = pick
            count = len(book_cart)
            await interaction.response.send_message(
                f"\u2705 Added **{batter}** to {book_label} parlay ({count} leg{'s' if count != 1 else ''})",
                ephemeral=True,
            )


# ── Bot Setup ──────────────────────────────────────────────────────────────

intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)


@bot.event
async def on_ready():
    log.info(f"Bot ready: {bot.user}")
    try:
        guild = discord.Object(id=GUILD_ID)
        tree.copy_global_to(guild=guild)
        await tree.sync(guild=guild)
        log.info("Slash commands synced")
    except Exception as e:
        log.error(f"Failed to sync commands: {e}")


@bot.event
async def on_interaction(interaction: discord.Interaction):
    """Handle DK/FD button clicks from ANY message (bot-sent or discord_alerts.py-sent)."""
    if interaction.type != discord.InteractionType.component:
        return
    custom_id = interaction.data.get("custom_id", "")
    if not (custom_id.startswith("dk:") or custom_id.startswith("fd:")):
        return

    book, pick_id = custom_id.split(":", 1)
    uid = interaction.user.id
    book_label = "DraftKings" if book == "dk" else "FanDuel"

    # Try pick_store first; if not there, build minimal pick from the embed
    pick = pick_store.get(pick_id)
    if not pick:
        # Extract batter name from pick_id (format: "Name_gamepk")
        batter_name = pick_id.rsplit("_", 1)[0] if "_" in pick_id else pick_id
        pick = {"batter_name": batter_name, "game_pk": pick_id.rsplit("_", 1)[-1] if "_" in pick_id else ""}
        pick_store[pick_id] = pick

    cart = user_carts.setdefault(uid, {"dk": {}, "fd": {}})
    book_cart = cart[book]
    batter = pick.get("batter_name", "?")

    if pick_id in book_cart:
        del book_cart[pick_id]
        count = len(book_cart)
        await interaction.response.send_message(
            f"\u274c Removed **{batter}** from {book_label} parlay ({count} leg{'s' if count != 1 else ''})",
            ephemeral=True,
        )
    else:
        book_cart[pick_id] = pick
        count = len(book_cart)
        await interaction.response.send_message(
            f"\u2705 Added **{batter}** to {book_label} parlay ({count} leg{'s' if count != 1 else ''})",
            ephemeral=True,
        )


@tree.command(name="parlay", description="Generate your parlay deeplink from selected picks")
async def parlay_cmd(interaction: discord.Interaction):
    uid = interaction.user.id
    cart = user_carts.get(uid)
    if not cart or (not cart.get("dk") and not cart.get("fd")):
        await interaction.response.send_message(
            "Your parlay cart is empty. Tap the <:dk:1493069919766708295> or <:fd:1493070566809403593> buttons on picks to add them.",
            ephemeral=True,
        )
        return

    embeds = []

    for book, label, builder, emoji_ref in [
        ("dk", "DraftKings", _build_dk_parlay, "<:dk:1493069919766708295>"),
        ("fd", "FanDuel", _build_fd_parlay, "<:fd:1493070566809403593>"),
    ]:
        picks = list(cart.get(book, {}).values())
        if not picks:
            continue

        legs = []
        for p in picks:
            odds = _fmt_odds(p.get("hr_odds_best_price"))
            legs.append(f"\u2022 **{p.get('batter_name', '?')}** 1+ HR ({odds})")

        combined = _parlay_odds(picks)
        link = builder(picks)
        link_text = f"\n\n[**Open {label} Betslip \u2192**]({link})" if link else "\n\n\u26a0\ufe0f Missing deeplink data"

        embeds.append(discord.Embed(
            title=f"{emoji_ref} {label} Parlay \u2014 {len(picks)} legs",
            description="\n".join(legs) + f"\n\n**Combined Odds: {combined or '?'}**" + link_text,
            color=0x22C55E if book == "dk" else 0x1A6CFF,
        ))

    await interaction.response.send_message(embeds=embeds, ephemeral=True)


@tree.command(name="clear", description="Clear your parlay cart")
async def clear_cmd(interaction: discord.Interaction):
    uid = interaction.user.id
    user_carts.pop(uid, None)
    await interaction.response.send_message("\U0001f5d1\ufe0f Parlay cart cleared.", ephemeral=True)


# ── Send Alerts ────────────────────────────────────────────────────────────

async def send_alerts():
    """Fetch today's picks from BigQuery and send to #hr-bets with buttons."""
    await bot.wait_until_ready()
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        log.error(f"Channel {CHANNEL_ID} not found")
        return

    today = datetime.now(ET).date()
    client = bigquery.Client(project=PROJECT)
    query = f"""
    SELECT
      batter_name, bat_side, pitcher_name, pitcher_hand,
      home_team, away_team, batter_team, pitcher_team,
      score, grade, why, flags,
      iso, slg, l15_ev, l15_barrel_pct, l15_hard_hit_pct, hr_fb_pct,
      p_hr9_vs_hand, p_hr_fb_pct, p_barrel_pct,
      hr_odds_best_price, hr_odds_best_book,
      dk_outcome_code, dk_event_id, fd_market_id, fd_selection_id,
      weather_indicator, game_temp, wind_speed, wind_dir, precip_prob,
      conditions, ballpark_name, roof_type,
      bvp_ab, bvp_hits, bvp_hr,
      batting_order_pos, ws_batting_order, ws_at_bats, ws_hits, ws_home_runs, ws_slg,
      game_pk
    FROM `{HR_TABLE}`
    WHERE run_date = @run_date
      AND grade IN ('IDEAL', 'FAVORABLE')
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY batter_name, pitcher_name
      ORDER BY run_timestamp DESC
    ) = 1
    ORDER BY score DESC
    """
    params = [bigquery.ScalarQueryParameter("run_date", "DATE", today.isoformat())]
    job = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
    picks = list(job.result())

    if not picks:
        log.info("No IDEAL/FAVORABLE picks today")
        return

    # Clear old pick store and carts
    pick_store.clear()
    user_carts.clear()

    # Send header
    ideal_count = sum(1 for p in picks if p.get("grade") == "IDEAL")
    fav_count = sum(1 for p in picks if p.get("grade") == "FAVORABLE")
    await channel.send(
        f"# \u26be HR Picks \u2014 {today.strftime('%b %d, %Y')}\n"
        f"**{ideal_count}** IDEAL + **{fav_count}** FAVORABLE matchups\n"
        f"Tap <:dk:1493069919766708295> or <:fd:1493070566809403593> to build your parlay, then `/parlay` to get your deeplink."
    )

    def _is_ws(p):
        return p.get("ws_batting_order") is not None

    def _bot_embed(p, color):
        batter = p.get("batter_name", "?")
        bat = _hand_label(p.get("bat_side"))
        pitcher = p.get("pitcher_name", "?")
        phand = _pitcher_hand(p.get("pitcher_hand"))
        bt = p.get("batter_team") or ""
        pt = p.get("pitcher_team") or ""
        score = int(p.get("score") or 0)
        odds = _fmt_odds(p.get("hr_odds_best_price"))
        ws = "\U0001f3af " if _is_ws(p) else ""
        return discord.Embed(
            description=(
                f"{ws}**{batter}** ({bat}) {bt} vs {pitcher} ({phand}) {pt}\n"
                f"Pulse **{score}** \u2022 {odds} | {_venue_weather_short(p)}\n"
                f"{_stats_line(p)}"
            ),
            color=color,
        )

    async def _send_pick(p, color):
        pick_id = f"{p.get('batter_name','?')}_{p.get('game_pk','')}"
        pick_store[pick_id] = dict(p)
        await channel.send(embed=_bot_embed(p, color), view=PickView(pick_id))

    # ALL IDEAL picks
    ideal_picks = [p for p in picks if p.get("grade") == "IDEAL"]
    if ideal_picks:
        await channel.send(f"## \U0001f7e2 IDEAL Matchups ({len(ideal_picks)})")
        for p in ideal_picks:
            await _send_pick(p, GRADE_COLORS["IDEAL"])

    # Top 10 FAVORABLE — weak spots sorted first, then by score
    fav_picks = [p for p in picks if p.get("grade") == "FAVORABLE"]
    fav_picks.sort(key=lambda p: (_is_ws(p), p.get("score") or 0), reverse=True)
    fav_top = fav_picks[:10]

    if fav_top:
        await channel.send(f"## \U0001f7e1 FAVORABLE Matchups (top {len(fav_top)})")
        for p in fav_top:
            await _send_pick(p, GRADE_COLORS["FAVORABLE"])

    log.info(f"Sent {len(pick_store)} pick alerts to #{channel.name}")


# ── Auto-check for new data ────────────────────────────────────────────────

_last_sent_date = None

async def _alert_loop():
    """Check for new picks every 30 min. Send alerts when new data appears."""
    global _last_sent_date
    await bot.wait_until_ready()
    await asyncio.sleep(5)

    while not bot.is_closed():
        try:
            today = datetime.now(ET).date()
            if _last_sent_date != today:
                # Check if there are picks for today
                client = bigquery.Client(project=PROJECT)
                result = list(client.query(
                    f"SELECT COUNT(*) as cnt FROM `{HR_TABLE}` WHERE run_date = '{today.isoformat()}' AND grade IN ('IDEAL','FAVORABLE')"
                ).result())
                count = result[0].cnt if result else 0
                if count > 0:
                    log.info(f"New picks found for {today} ({count} picks). Sending alerts...")
                    await send_alerts()
                    _last_sent_date = today
                else:
                    log.info(f"No picks yet for {today}. Checking again in 30 min.")
        except Exception as e:
            log.error(f"Alert loop error: {e}")

        await asyncio.sleep(1800)  # 30 minutes


# ── Entrypoint ─────────────────────────────────────────────────────────────

async def _health_server():
    """Simple HTTP server for Cloud Run health checks."""
    async def health(request):
        return web.Response(text="OK")
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "8080"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"Health server listening on port {port}")


async def main():
    async with bot:
        await _health_server()
        bot.loop.create_task(_alert_loop())
        await bot.start(TOKEN)


if __name__ == "__main__":
    if not TOKEN:
        log.error("DISCORD_BOT_TOKEN not set")
        raise SystemExit(1)
    asyncio.run(main())
