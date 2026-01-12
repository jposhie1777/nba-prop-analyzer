import { Client, GatewayIntentBits } from "discord.js";

const DISCORD_TOKEN = process.env.DISCORD_TOKEN;
const SOURCE_CHANNEL = "pulse-bets"; // channel name
const GAMBLY_BOT_ID = "1338973806383071392"; // GamblyBot ID

if (!DISCORD_TOKEN) {
  throw new Error("Missing DISCORD_TOKEN");
}

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
});

client.once("ready", () => {
  console.log(`✅ Pulse Relay logged in as ${client.user.tag}`);
});

client.on("messageCreate", async (message) => {
  // Ignore bots (including ourselves)
  if (message.author.bot) return;

  // Ignore other channels
  if (message.channel.name !== SOURCE_CHANNEL) return;

  // Ignore empty messages
  if (!message.content?.trim()) return;

  try {
    const relayMessage = `<@${GAMBLY_BOT_ID}>\n${message.content}`;

    await message.channel.send(relayMessage);

    console.log("🔁 Relayed message to Gambly");
  } catch (err) {
    console.error("❌ Relay failed:", err);
  }
});

client.login(DISCORD_TOKEN);
