const express = require("express");
const cors = require("cors");
const fs = require("fs/promises");
const path = require("path");
const amqp = require("amqplib");

require("dotenv").config();

const app = express();

app.use(cors());
app.use(express.json());
app.use(express.static("public"));

const PORT = process.env.MODERATE_SERVICE_PORT || 3100;

const TYPES_CACHE_FILE = process.env.TYPES_CACHE_FILE || path.join(__dirname, "cache", "types-cache.json");

const INITIAL_TYPES_FILE = process.env.INITIAL_TYPES_FILE || path.join(__dirname, "defaults", "types-cache.json");

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://rabbitmq:5672";
const SUBMIT_QUEUE = process.env.SUBMIT_QUEUE || "submit";
const MODERATED_QUEUE = process.env.MODERATED_QUEUE || "moderated";

const TYPE_UPDATE_EXCHANGE = process.env.TYPE_UPDATE_EXCHANGE || "type_update";
const MOD_TYPE_UPDATE_QUEUE = process.env.MOD_TYPE_UPDATE_QUEUE || "mod_type_update";

let rabbitConnection = null;
let rabbitChannel = null;
let typeUpdateConsumerStarted = false;
let reconnectTimer = null;

async function ensureCacheDirectoryExists() {
  const cacheDir = path.dirname(TYPES_CACHE_FILE);
  await fs.mkdir(cacheDir, { recursive: true });
}

async function writeTypesCache(types) {
  await ensureCacheDirectoryExists();
  await fs.writeFile(TYPES_CACHE_FILE, JSON.stringify(types, null, 2), "utf8");
}

async function readTypesCache() {
  const fileContent = await fs.readFile(TYPES_CACHE_FILE, "utf8");
  return JSON.parse(fileContent);
}

function normalizeTypes(types) {
  if (!Array.isArray(types)) {
    return [];
  }

  const seen = new Set();
  const normalized = [];

  for (const item of types) {
    const name = typeof item === "string" ? item.trim() : String(item?.name || "").trim();

    if (!name) {
      continue;
    }

    const key = name.toLowerCase();

    if (!seen.has(key)) {
      seen.add(key);
      normalized.push({ name });
    }
  }

  normalized.sort((a, b) => a.name.localeCompare(b.name));
  return normalized;
}

async function ensureTypesCacheExists() {
  await ensureCacheDirectoryExists();

  try {
    await fs.access(TYPES_CACHE_FILE);
    return;
  } catch {
    // cache file does not exist yet
  }

  try {
    const initialContent = await fs.readFile(INITIAL_TYPES_FILE, "utf8");
    const initialTypes = normalizeTypes(JSON.parse(initialContent));
    await writeTypesCache(initialTypes);
    console.log(`[MODERATE] Created initial cache from ${INITIAL_TYPES_FILE}`);
  } catch (err) {
    console.warn("[MODERATE] Could not load initial cache file:", err.message);
    await writeTypesCache([]);
    console.log("[MODERATE] Created empty cache file");
  }
}

async function connectRabbitMQ() {
  if (rabbitConnection && rabbitChannel) {
    return rabbitChannel;
  }

  rabbitConnection = await amqp.connect(RABBITMQ_URL);
  rabbitChannel = await rabbitConnection.createChannel();

  await rabbitChannel.assertQueue(SUBMIT_QUEUE, { durable: true });
  await rabbitChannel.assertQueue(MODERATED_QUEUE, { durable: true });

  await rabbitChannel.assertExchange(TYPE_UPDATE_EXCHANGE, "fanout", {
    durable: true,
  });

  await rabbitChannel.assertQueue(MOD_TYPE_UPDATE_QUEUE, { durable: true });
  await rabbitChannel.bindQueue(MOD_TYPE_UPDATE_QUEUE, TYPE_UPDATE_EXCHANGE, "");

  console.log(`[MODERATE] Connected to RabbitMQ: ${RABBITMQ_URL}`);
  console.log(`[MODERATE] Submit queue: ${SUBMIT_QUEUE}`);
  console.log(`[MODERATE] Moderated queue: ${MODERATED_QUEUE}`);
  console.log(`[MODERATE] Type update exchange: ${TYPE_UPDATE_EXCHANGE}`);
  console.log(`[MODERATE] Moderate type update queue: ${MOD_TYPE_UPDATE_QUEUE}`);

  rabbitConnection.on("error", (err) => {
    console.error("[MODERATE] RabbitMQ connection error:", err.message);
  });

  rabbitConnection.on("close", () => {
    console.warn("[MODERATE] RabbitMQ connection closed");
    rabbitConnection = null;
    rabbitChannel = null;
    typeUpdateConsumerStarted = false;
    scheduleReconnect();
  });

  return rabbitChannel;
}

function scheduleReconnect() {
  if (reconnectTimer) {
    return;
  }

  reconnectTimer = setTimeout(async () => {
    reconnectTimer = null;
    await reconnectTypeUpdateConsumer();
  }, 5000);
}

async function mergeTypeIntoCache(typeName) {
  const cleanType = String(typeName || "").trim();

  if (!cleanType) {
    return false;
  }

  let existingTypes = [];

  try {
    existingTypes = normalizeTypes(await readTypesCache());
  } catch {
    existingTypes = [];
  }

  const alreadyExists = existingTypes.some((item) => item.name.toLowerCase() === cleanType.toLowerCase());

  if (alreadyExists) {
    return false;
  }

  const updatedTypes = normalizeTypes([...existingTypes, { name: cleanType }]);
  await writeTypesCache(updatedTypes);

  return true;
}

async function startTypeUpdateConsumer() {
  if (typeUpdateConsumerStarted) {
    return;
  }

  const channel = await connectRabbitMQ();

  await channel.prefetch(1);

  await channel.consume(MOD_TYPE_UPDATE_QUEUE, async (msg) => {
    if (!msg) {
      return;
    }

    const rawMessage = msg.content.toString();

    try {
      const payload = JSON.parse(rawMessage);
      const updated = await mergeTypeIntoCache(payload.type);

      if (updated) {
        console.log(`[MODERATE] Type cache updated from event: "${payload.type}"`);
      } else {
        console.log(`[MODERATE] Type already present or invalid, skipped: "${payload.type || ""}"`);
      }

      channel.ack(msg);
    } catch (err) {
      console.error("[MODERATE] Failed to process type_update event:", err.message);
      channel.nack(msg, false, true);
    }
  });

  typeUpdateConsumerStarted = true;
  console.log("[MODERATE] Listening for type_update events");
}

async function reconnectTypeUpdateConsumer() {
  if (typeUpdateConsumerStarted) {
    return;
  }

  try {
    console.log("[MODERATE] Attempting RabbitMQ reconnect...");
    await startTypeUpdateConsumer();
  } catch (err) {
    console.error("[MODERATE] Reconnect failed:", err.message);
    scheduleReconnect();
  }
}

app.get("/types", async (req, res) => {
  try {
    const cachedTypes = normalizeTypes(await readTypesCache());

    return res.json({
      source: "cache",
      data: cachedTypes,
    });
  } catch (err) {
    return res.status(503).json({
      error: "Unable to read local types cache",
      details: err.message,
    });
  }
});

app.get("/moderate", async (req, res) => {
  try {
    const channel = await connectRabbitMQ();
    const msg = await channel.get(SUBMIT_QUEUE, { noAck: false });

    if (!msg) {
      return res.json({
        available: false,
        message: "No jokes available for moderation right now.",
      });
    }

    const rawMessage = msg.content.toString();

    let payload;
    try {
      payload = JSON.parse(rawMessage);
    } catch (err) {
      channel.nack(msg, false, false);

      return res.status(500).json({
        available: false,
        error: "Failed to parse joke payload from queue",
        details: err.message,
      });
    }

    channel.ack(msg);

    return res.json({
      available: true,
      data: {
        setup: String(payload.setup || ""),
        punchline: String(payload.punchline || ""),
        type: String(payload.type || ""),
        submittedAt: payload.submittedAt || null,
        status: payload.status || "pending_moderation",
      },
    });
  } catch (err) {
    return res.status(500).json({
      available: false,
      error: "Failed to fetch joke from submit queue",
      details: err.message,
    });
  }
});

app.post("/moderated", async (req, res) => {
  try {
    const { setup, punchline, type } = req.body;

    if (!setup || !punchline || !type) {
      return res.status(400).json({
        error: "Setup, punchline, and type are required",
      });
    }

    const payload = {
      setup: String(setup).trim(),
      punchline: String(punchline).trim(),
      type: String(type).trim(),
      moderatedAt: new Date().toISOString(),
      status: "approved",
    };

    if (!payload.setup || !payload.punchline || !payload.type) {
      return res.status(400).json({
        error: "Setup, punchline, and type are required",
      });
    }

    const channel = await connectRabbitMQ();

    const published = channel.sendToQueue(MODERATED_QUEUE, Buffer.from(JSON.stringify(payload)), { persistent: true });

    if (!published) {
      return res.status(503).json({
        error: "Moderated queue is temporarily unavailable. Please try again.",
      });
    }

    return res.status(202).json({
      message: "Joke approved and sent to moderated queue",
      queue: MODERATED_QUEUE,
      data: payload,
    });
  } catch (err) {
    return res.status(500).json({
      error: "Failed to publish moderated joke",
      details: err.message,
    });
  }
});

app.listen(PORT, async () => {
  console.log(`[MODERATE] Service running on port ${PORT}`);
  console.log(`[MODERATE] Types cache file: ${TYPES_CACHE_FILE}`);

  try {
    await ensureTypesCacheExists();
    await startTypeUpdateConsumer();
  } catch (err) {
    console.error("[MODERATE] Startup warning:", err.message);
    scheduleReconnect();
  }
});

async function shutdown() {
  console.log("[MODERATE] Shutting down...");

  try {
    if (rabbitChannel) {
      await rabbitChannel.close();
    }
  } catch (err) {
    console.error("[MODERATE] Error closing channel:", err.message);
  }

  try {
    if (rabbitConnection) {
      await rabbitConnection.close();
    }
  } catch (err) {
    console.error("[MODERATE] Error closing connection:", err.message);
  }

  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
