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
    # Re-register persistent views for old messages
    bot.add_view(PickView("__persistent__"))
    try:
        guild = discord.Object(id=GUILD_ID)
        tree.copy_global_to(guild=guild)
        await tree.sync(guild=guild)
        log.info("Slash commands synced")
    except Exception as e:
        log.error(f"Failed to sync commands: {e}")


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
      score, grade, why, flags,
      iso, slg, l15_ev, l15_barrel_pct,
      p_hr9_vs_hand, p_hr_fb_pct, p_barrel_pct,
      hr_odds_best_price, hr_odds_best_book,
      dk_outcome_code, dk_event_id, fd_market_id, fd_selection_id,
      weather_indicator, game_temp, ballpark_name,
      game_pk
    FROM `{HR_TABLE}`
    WHERE run_date = @run_date
      AND grade IN ('IDEAL', 'FAVORABLE')
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY batter_name, pitcher_name
      ORDER BY score DESC
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

    # Send weak spots first
    weak_spots = [
        p for p in picks
        if (p.get("p_hr9_vs_hand") or 0) >= 1.8
        and (p.get("p_hr_fb_pct") or 0) >= 15
    ][:5]

    if weak_spots:
        await channel.send("## \U0001f3af Pitcher Weak Spots")

    for p in weak_spots:
        pick_id = f"{p.get('batter_name','?')}_{p.get('game_pk','')}"
        pick_data = dict(p)
        pick_store[pick_id] = pick_data

        batter = p.get("batter_name", "?")
        bat = _hand_label(p.get("bat_side"))
        pitcher = p.get("pitcher_name", "?")
        phand = _pitcher_hand(p.get("pitcher_hand"))
        score = int(p.get("score") or 0)
        odds = _fmt_odds(p.get("hr_odds_best_price"))
        hr9 = p.get("p_hr9_vs_hand") or 0
        hrfb = p.get("p_hr_fb_pct") or 0
        grade_em = GRADE_EMOJI.get(p.get("grade"), "")

        embed = discord.Embed(
            description=(
                f"{grade_em} **{batter}** ({bat}) vs {pitcher} ({phand})\n"
                f"Pulse **{score}** \u2022 {odds}\n"
                f"P-HR/9 **{hr9:.2f}** \u2022 HR/FB **{hrfb:.1f}%**"
            ),
            color=WEAK_SPOT_COLOR,
        )
        await channel.send(embed=embed, view=PickView(pick_id))

    # Send IDEAL and FAVORABLE picks
    for grade in ("IDEAL", "FAVORABLE"):
        group = [p for p in picks if p.get("grade") == grade][:8]
        if not group:
            continue

        emoji = GRADE_EMOJI.get(grade, "")
        await channel.send(f"## {emoji} {grade} Matchups")

        for p in group:
            pick_id = f"{p.get('batter_name','?')}_{p.get('game_pk','')}"
            pick_data = dict(p)
            pick_store[pick_id] = pick_data

            batter = p.get("batter_name", "?")
            bat = _hand_label(p.get("bat_side"))
            pitcher = p.get("pitcher_name", "?")
            phand = _pitcher_hand(p.get("pitcher_hand"))
            score = int(p.get("score") or 0)
            odds = _fmt_odds(p.get("hr_odds_best_price"))

            iso = p.get("iso")
            l15_ev = p.get("l15_ev")
            l15_bar = p.get("l15_barrel_pct")

            stat_parts = []
            if iso is not None:
                stat_parts.append(f"ISO {iso:.3f}")
            if l15_ev is not None:
                stat_parts.append(f"EV {l15_ev:.1f}")
            if l15_bar is not None:
                stat_parts.append(f"Barrel {l15_bar:.0f}%")

            weather = p.get("weather_indicator") or ""
            temp = p.get("game_temp")
            park = p.get("ballpark_name") or ""
            w_parts = []
            if weather:
                w_em = "\U0001f7e2" if weather == "Green" else "\U0001f7e1" if weather == "Yellow" else "\U0001f534"
                w_parts.append(w_em)
            if temp:
                w_parts.append(f"{int(temp)}\u00b0")
            if park:
                w_parts.append(park)
            weather_str = f" ({' '.join(w_parts)})" if w_parts else ""

            embed = discord.Embed(
                description=(
                    f"**{batter}** ({bat}) vs {pitcher} ({phand})\n"
                    f"Pulse **{score}** \u2022 {odds}{weather_str}\n"
                    f"{' | '.join(stat_parts)}"
                ),
                color=GRADE_COLORS.get(grade, 0x6366F1),
            )
            await channel.send(embed=embed, view=PickView(pick_id))

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
