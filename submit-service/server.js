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
const JOKE_SERVICE_URL = process.env.JOKE_SERVICE_URL || "http://joke-service:3000";
const TYPES_CACHE_FILE = process.env.TYPES_CACHE_FILE || path.join(__dirname, "cache", "types-cache.json");

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://rabbitmq:5672";
const RABBITMQ_QUEUE = process.env.RABBITMQ_QUEUE || "submitted_jokes";

let rabbitConnection = null;
let rabbitChannel = null;

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

async function connectRabbitMQ() {
  if (rabbitConnection && rabbitChannel) {
    return rabbitChannel;
  }

  rabbitConnection = await amqp.connect(RABBITMQ_URL);
  rabbitChannel = await rabbitConnection.createChannel();

  await rabbitChannel.assertQueue(RABBITMQ_QUEUE, {
    durable: true,
  });

  console.log(`Connected to RabbitMQ at ${RABBITMQ_URL}`);
  console.log(`Using queue: ${RABBITMQ_QUEUE}`);

  rabbitConnection.on("error", (err) => {
    console.error("RabbitMQ connection error:", err.message);
    rabbitConnection = null;
    rabbitChannel = null;
  });

  rabbitConnection.on("close", () => {
    console.warn("RabbitMQ connection closed");
    rabbitConnection = null;
    rabbitChannel = null;
  });

  return rabbitChannel;
}

/**
 * @swagger
 * /types:
 *   get:
 *     summary: Get all joke types
 *     responses:
 *       200:
 *         description: List of types
 */
app.get("/types", async (req, res) => {
  try {
    const response = await fetch(`${JOKE_SERVICE_URL}/types`);

    if (!response.ok) {
      throw new Error(`Joke service responded with status ${response.status}`);
    }

    const freshTypes = await response.json();

    await writeTypesCache(freshTypes);

    return res.json({
      source: "joke-service",
      data: freshTypes,
    });
  } catch (httpError) {
    try {
      const cachedTypes = await readTypesCache();

      return res.json({
        source: "cache",
        data: cachedTypes,
      });
    } catch (cacheError) {
      return res.status(503).json({
        error: "Unable to fetch joke types from joke-service or cache",
        details: {
          jokeServiceError: httpError.message,
          cacheError: cacheError.message,
        },
      });
    }
  }
});

/**
 * @swagger
 * /submit:
 *   post:
 *     summary: Submit a new joke
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
 *         description: Joke accepted and queued
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
    };

    if (!payload.setup || !payload.punchline || !payload.type) {
      return res.status(400).json({ error: "All fields required" });
    }

    const channel = await connectRabbitMQ();

    const published = channel.sendToQueue(RABBITMQ_QUEUE, Buffer.from(JSON.stringify(payload)), { persistent: true });

    if (!published) {
      return res.status(503).json({
        error: "Queue is temporarily unavailable. Please try again.",
      });
    }

    return res.status(202).json({
      message: "Joke accepted and added to queue",
      queue: RABBITMQ_QUEUE,
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
  try {
    await ensureCacheDirectoryExists();
    await connectRabbitMQ();
    console.log(`Submit service running on port ${PORT}`);
    console.log(`Types cache file: ${TYPES_CACHE_FILE}`);
  } catch (err) {
    console.error("Startup warning:", err.message);
    console.log(`Submit service running on port ${PORT}`);
    console.log(`Types cache file: ${TYPES_CACHE_FILE}`);
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
    console.error("Error closing RabbitMQ connection:", err.message);
  } finally {
    process.exit(0);
  }
});
