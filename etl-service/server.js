const express = require("express");
const amqp = require("amqplib");
const pool = require("./db");

require("dotenv").config();

const app = express();

const PORT = process.env.ETL_SERVICE_PORT || 3001;
const RABBITMQ_URL = process.env.ETL_SERVICE_RABBITMQ_URL || "amqp://rabbitmq:5672";

const MODERATED_QUEUE = process.env.ETL_SERVICE_MODERATED_QUEUE || "moderated";

const TYPE_UPDATE_EXCHANGE = process.env.ETL_SERVICE_TYPE_UPDATE_EXCHANGE || "type_update";

let rabbitConnection = null;
let rabbitChannel = null;
let consumerStarted = false;

app.get("/alive", (req, res) => {
  res.json({
    service: "etl-service",
    status: "alive",
    queue: MODERATED_QUEUE,
    typeUpdateExchange: TYPE_UPDATE_EXCHANGE,
  });
});

async function publishTypeUpdate(typeName) {
  const cleanType = String(typeName || "").trim();

  if (!cleanType) {
    return;
  }

  const payload = {
    type: cleanType,
    emittedAt: new Date().toISOString(),
    source: "etl-service",
  };

  await rabbitChannel.assertExchange(TYPE_UPDATE_EXCHANGE, "fanout", {
    durable: true,
  });

  rabbitChannel.publish(TYPE_UPDATE_EXCHANGE, "", Buffer.from(JSON.stringify(payload)), { persistent: true });

  console.log(`[ETL] Published type_update event for type="${cleanType}"`);
}

async function ensureTypeExists(connection, typeName) {
  const cleanType = String(typeName || "").trim();

  if (!cleanType) {
    throw new Error("Type is required");
  }

  const [existingRows] = await connection.query("SELECT id FROM types WHERE name = ? LIMIT 1", [cleanType]);

  if (existingRows.length > 0) {
    return {
      typeId: existingRows[0].id,
      wasCreated: false,
      type: cleanType,
    };
  }

  try {
    const [insertResult] = await connection.query("INSERT INTO types (name) VALUES (?)", [cleanType]);

    return {
      typeId: insertResult.insertId,
      wasCreated: true,
      type: cleanType,
    };
  } catch (err) {
    if (err.code === "ER_DUP_ENTRY") {
      const [rowsAfterDuplicate] = await connection.query("SELECT id FROM types WHERE name = ? LIMIT 1", [cleanType]);

      if (rowsAfterDuplicate.length > 0) {
        return {
          typeId: rowsAfterDuplicate[0].id,
          wasCreated: false,
          type: cleanType,
        };
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

    const typeResult = await ensureTypeExists(connection, type);

    await connection.query("INSERT INTO jokes (setup, punchline, type_id) VALUES (?, ?, ?)", [
      setup,
      punchline,
      typeResult.typeId,
    ]);

    await connection.commit();

    return {
      setup,
      punchline,
      type,
      typeId: typeResult.typeId,
      typeWasCreated: typeResult.wasCreated,
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

  await rabbitChannel.assertQueue(MODERATED_QUEUE, { durable: true });
  await rabbitChannel.assertExchange(TYPE_UPDATE_EXCHANGE, "fanout", {
    durable: true,
  });
  await rabbitChannel.prefetch(1);

  console.log(`[ETL] Connected to RabbitMQ: ${RABBITMQ_URL}`);
  console.log(`[ETL] Listening to queue: ${MODERATED_QUEUE}`);
  console.log(`[ETL] Publishing type updates to exchange: ${TYPE_UPDATE_EXCHANGE}`);

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

  await rabbitChannel.consume(MODERATED_QUEUE, async (msg) => {
    if (!msg) {
      return;
    }

    const rawMessage = msg.content.toString();

    try {
      console.log("[ETL] Message received:", rawMessage);

      const payload = JSON.parse(rawMessage);

      const result = await insertJokeMessage(payload);

      if (result.typeWasCreated) {
        await publishTypeUpdate(result.type);
      }

      rabbitChannel.ack(msg);

      console.log(
        `[ETL] Message processed successfully | type="${result.type}" | typeId=${result.typeId} | newType=${result.typeWasCreated}`,
      );
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
