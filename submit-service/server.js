const express = require("express");
const cors = require("cors");
const fs = require("fs/promises");
const path = require("path");
const amqp = require("amqplib");
const swaggerUi = require("swagger-ui-express");
const swaggerSpec = require("./swagger");

require("dotenv").config();

const app = express();

app.use(cors());
app.use(express.json());
app.use(express.static("public"));

const PORT = process.env.SUBMIT_SERVICE_PORT || 3200;

const TYPES_CACHE_FILE = process.env.TYPES_CACHE_FILE || path.join(__dirname, "cache", "types-cache.json");

const INITIAL_TYPES_FILE = process.env.INITIAL_TYPES_FILE || path.join(__dirname, "defaults", "types-cache.json");

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://rabbitmq:5672";

const SUBMIT_QUEUE = process.env.SUBMIT_QUEUE || "submit";
const MODERATED_QUEUE = process.env.MODERATED_QUEUE || "moderated";

const TYPE_UPDATE_EXCHANGE = process.env.TYPE_UPDATE_EXCHANGE || "type_update";
const SUB_TYPE_UPDATE_QUEUE = process.env.SUB_TYPE_UPDATE_QUEUE || "sub_type_update";
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
  const result = [];

  for (const item of types) {
    const name = typeof item === "string" ? item.trim() : String(item?.name || "").trim();

    if (!name) {
      continue;
    }

    const key = name.toLowerCase();

    if (!seen.has(key)) {
      seen.add(key);
      result.push({ name });
    }
  }

  result.sort((a, b) => a.name.localeCompare(b.name));

  return result;
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
    console.log(`[SUBMIT] Created initial cache from ${INITIAL_TYPES_FILE}`);
  } catch (err) {
    console.warn("[SUBMIT] Could not load initial cache file:", err.message);
    await writeTypesCache([]);
    console.log("[SUBMIT] Created empty cache file");
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

  await rabbitChannel.assertQueue(SUB_TYPE_UPDATE_QUEUE, { durable: true });
  await rabbitChannel.bindQueue(SUB_TYPE_UPDATE_QUEUE, TYPE_UPDATE_EXCHANGE, "");

  await rabbitChannel.assertQueue(MOD_TYPE_UPDATE_QUEUE, { durable: true });
  await rabbitChannel.bindQueue(MOD_TYPE_UPDATE_QUEUE, TYPE_UPDATE_EXCHANGE, "");

  console.log(`[SUBMIT] Connected to RabbitMQ: ${RABBITMQ_URL}`);
  console.log(`[SUBMIT] Submit queue: ${SUBMIT_QUEUE}`);
  console.log(`[SUBMIT] Moderated queue: ${MODERATED_QUEUE}`);
  console.log(`[SUBMIT] Type update exchange: ${TYPE_UPDATE_EXCHANGE}`);
  console.log(`[SUBMIT] Submit type update queue: ${SUB_TYPE_UPDATE_QUEUE}`);
  console.log(`[SUBMIT] Moderate type update queue: ${MOD_TYPE_UPDATE_QUEUE}`);

  rabbitConnection.on("error", (err) => {
    console.error("[SUBMIT] RabbitMQ connection error:", err.message);
  });

  rabbitConnection.on("close", () => {
    console.warn("[SUBMIT] RabbitMQ connection closed");
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

  await channel.consume(SUB_TYPE_UPDATE_QUEUE, async (msg) => {
    if (!msg) {
      return;
    }

    const rawMessage = msg.content.toString();

    try {
      const payload = JSON.parse(rawMessage);
      const updated = await mergeTypeIntoCache(payload.type);

      if (updated) {
        console.log(`[SUBMIT] Type cache updated from event: "${payload.type}"`);
      } else {
        console.log(`[SUBMIT] Type already present or invalid, skipped: "${payload.type || ""}"`);
      }

      channel.ack(msg);
    } catch (err) {
      console.error("[SUBMIT] Failed to process type_update event:", err.message);
      channel.nack(msg, false, true);
    }
  });

  typeUpdateConsumerStarted = true;
  console.log("[SUBMIT] Listening for type_update events");
}

async function reconnectTypeUpdateConsumer() {
  if (typeUpdateConsumerStarted) {
    return;
  }

  try {
    console.log("[SUBMIT] Attempting RabbitMQ reconnect...");
    await startTypeUpdateConsumer();
  } catch (err) {
    console.error("[SUBMIT] Reconnect failed:", err.message);
    scheduleReconnect();
  }
}

/**
 * @swagger
 * /submit/types:
 *   get:
 *     summary: Get all joke types from local cache
 *     responses:
 *       200:
 *         description: List of cached types
 */
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

/**
 * @swagger
 * /submit:
 *   post:
 *     summary: Submit a new joke for moderation
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               setup:
 *                 type: string
 *               punchline:
 *                 type: string
 *               type:
 *                 type: string
 *     responses:
 *       202:
 *         description: Joke accepted and queued for moderation
 */
app.post("/submit", async (req, res) => {
  try {
    const { setup, punchline, type } = req.body;

    if (!setup || !punchline || !type) {
      return res.status(400).json({ error: "All fields required" });
    }

    const payload = {
      setup: String(setup).trim(),
      punchline: String(punchline).trim(),
      type: String(type).trim(),
      submittedAt: new Date().toISOString(),
      status: "pending_moderation",
    };

    if (!payload.setup || !payload.punchline || !payload.type) {
      return res.status(400).json({ error: "All fields required" });
    }

    const channel = await connectRabbitMQ();

    const published = channel.sendToQueue(SUBMIT_QUEUE, Buffer.from(JSON.stringify(payload)), { persistent: true });

    if (!published) {
      return res.status(503).json({
        error: "Queue is temporarily unavailable. Please try again.",
      });
    }

    return res.status(202).json({
      message: "Joke accepted and added to moderation queue",
      queue: SUBMIT_QUEUE,
      data: payload,
    });
  } catch (err) {
    return res.status(500).json({
      error: "Failed to publish joke to RabbitMQ",
      details: err.message,
    });
  }
});

app.use("/docs", swaggerUi.serve, swaggerUi.setup(swaggerSpec));

app.listen(PORT, async () => {
  console.log(`[SUBMIT] Service running on port ${PORT}`);
  console.log(`[SUBMIT] Types cache file: ${TYPES_CACHE_FILE}`);

  try {
    await ensureTypesCacheExists();
    await startTypeUpdateConsumer();
  } catch (err) {
    console.error("[SUBMIT] Startup warning:", err.message);
    scheduleReconnect();
  }
});

process.on("SIGINT", async () => {
  try {
    if (rabbitChannel) {
      await rabbitChannel.close();
    }
    if (rabbitConnection) {
      await rabbitConnection.close();
    }
  } catch (err) {
    console.error("[SUBMIT] Error closing RabbitMQ connection:", err.message);
  } finally {
    process.exit(0);
  }
});

process.on("SIGTERM", async () => {
  try {
    if (rabbitChannel) {
      await rabbitChannel.close();
    }
    if (rabbitConnection) {
      await rabbitConnection.close();
    }
  } catch (err) {
    console.error("[SUBMIT] Error closing RabbitMQ connection:", err.message);
  } finally {
    process.exit(0);
  }
});
