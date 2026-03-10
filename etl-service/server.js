const express = require("express");
const amqp = require("amqplib");
const pool = require("./db");

require("dotenv").config();

const app = express();

const PORT = process.env.ETL_SERVICE_PORT || 3001;
const RABBITMQ_URL = process.env.ETL_SERVICE_RABBITMQ_URL || "amqp://rabbitmq:5672";
const RABBITMQ_QUEUE = process.env.ETL_SERVICE_RABBITMQ_QUEUE || "submitted_jokes";

let rabbitConnection = null;
let rabbitChannel = null;
let consumerStarted = false;

app.get("/alive", (req, res) => {
  res.json({
    service: "etl-service",
    status: "alive",
    queue: RABBITMQ_QUEUE,
  });
});

async function ensureTypeExists(connection, typeName) {
  const cleanType = String(typeName || "").trim();

  if (!cleanType) {
    throw new Error("Type is required");
  }

  const [existingRows] = await connection.query("SELECT id FROM types WHERE name = ? LIMIT 1", [cleanType]);

  if (existingRows.length > 0) {
    return existingRows[0].id;
  }

  try {
    const [insertResult] = await connection.query("INSERT INTO types (name) VALUES (?)", [cleanType]);
    return insertResult.insertId;
  } catch (err) {
    if (err.code === "ER_DUP_ENTRY") {
      const [rowsAfterDuplicate] = await connection.query("SELECT id FROM types WHERE name = ? LIMIT 1", [cleanType]);

      if (rowsAfterDuplicate.length > 0) {
        return rowsAfterDuplicate[0].id;
      }
    }

    throw err;
  }
}

async function insertJokeMessage(payload) {
  const setup = String(payload.setup || "").trim();
  const punchline = String(payload.punchline || "").trim();
  const type = String(payload.type || "").trim();

  if (!setup || !punchline || !type) {
    throw new Error("Invalid payload: setup, punchline, and type are required");
  }

  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const typeId = await ensureTypeExists(connection, type);

    await connection.query("INSERT INTO jokes (setup, punchline, type_id) VALUES (?, ?, ?)", [
      setup,
      punchline,
      typeId,
    ]);

    await connection.commit();

    return {
      setup,
      punchline,
      type,
      typeId,
    };
  } catch (err) {
    await connection.rollback();
    throw err;
  } finally {
    connection.release();
  }
}

async function startConsumer() {
  rabbitConnection = await amqp.connect(RABBITMQ_URL);
  rabbitChannel = await rabbitConnection.createChannel();

  await rabbitChannel.assertQueue(RABBITMQ_QUEUE, { durable: true });
  await rabbitChannel.prefetch(1);

  console.log(`[ETL] Connected to RabbitMQ: ${RABBITMQ_URL}`);
  console.log(`[ETL] Listening to queue: ${RABBITMQ_QUEUE}`);

  rabbitConnection.on("error", (err) => {
    console.error("[ETL] RabbitMQ connection error:", err.message);
  });

  rabbitConnection.on("close", () => {
    console.warn("[ETL] RabbitMQ connection closed");
    rabbitConnection = null;
    rabbitChannel = null;
    consumerStarted = false;

    setTimeout(() => {
      reconnectConsumer();
    }, 5000);
  });

  await rabbitChannel.consume(RABBITMQ_QUEUE, async (msg) => {
    if (!msg) {
      return;
    }

    const rawMessage = msg.content.toString();

    try {
      console.log("[ETL] Message received:", rawMessage);

      const payload = JSON.parse(rawMessage);

      const result = await insertJokeMessage(payload);

      rabbitChannel.ack(msg);

      console.log(`[ETL] Message processed successfully | type="${result.type}" | typeId=${result.typeId}`);
    } catch (err) {
      console.error("[ETL] Failed to process message:", err.message);

      rabbitChannel.nack(msg, false, true);
    }
  });

  consumerStarted = true;
}

async function reconnectConsumer() {
  if (consumerStarted) {
    return;
  }

  try {
    console.log("[ETL] Attempting RabbitMQ reconnect...");
    await startConsumer();
  } catch (err) {
    console.error("[ETL] Reconnect failed:", err.message);

    setTimeout(() => {
      reconnectConsumer();
    }, 5000);
  }
}

app.listen(PORT, async () => {
  console.log(`[ETL] Service running on port ${PORT}`);

  try {
    await startConsumer();
  } catch (err) {
    console.error("[ETL] Initial RabbitMQ connection failed:", err.message);
    setTimeout(() => {
      reconnectConsumer();
    }, 5000);
  }
});

async function shutdown() {
  console.log("[ETL] Shutting down...");

  try {
    if (rabbitChannel) {
      await rabbitChannel.close();
    }
  } catch (err) {
    console.error("[ETL] Error closing channel:", err.message);
  }

  try {
    if (rabbitConnection) {
      await rabbitConnection.close();
    }
  } catch (err) {
    console.error("[ETL] Error closing connection:", err.message);
  }

  try {
    await pool.end();
  } catch (err) {
    console.error("[ETL] Error closing DB pool:", err.message);
  }

  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
