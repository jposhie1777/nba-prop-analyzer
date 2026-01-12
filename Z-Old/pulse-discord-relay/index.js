import 'dotenv/config';
import { Client, GatewayIntentBits, ChannelType } from "discord.js";

const DISCORD_TOKEN = process.env.DISCORD_TOKEN;

// 🔧 CONFIG
const SOURCE_CHANNEL_NAME = "pulse-bets";
const GAMBLY_BOT_ID = "1338973806383071392";

if (!DISCORD_TOKEN) {
  throw new Error("❌ Missing DISCORD_TOKEN");
}

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
});

client.once("ready", () => {
  console.log(`✅ Pulse Relay logged in as ${client.user?.tag}`);
});

client.on("messageCreate", async (message) => {
  try {
    // -------------------------
    // HARD GUARDS
    // -------------------------
    if (message.author.bot) return;
    if (!message.guild) return;

    if (
      message.channel.type !== ChannelType.GuildText ||
      message.channel.name !== SOURCE_CHANNEL_NAME
    ) {
      return;
    }

    const content = message.content?.trim();
    if (!content) return;

    // -------------------------
    // RELAY TO GAMBLY
    // -------------------------
    const relay = `<@${GAMBLY_BOT_ID}>\n${content}`;

    await message.channel.send({ content: relay });

    console.log("🔁 Relayed Pulse bets → Gambly");
  } catch (err) {
    console.error("❌ Relay failed:", err);
  }
});

client.login(DISCORD_TOKEN);