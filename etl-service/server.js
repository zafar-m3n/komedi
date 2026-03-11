const express = require("express");
const amqp = require("amqplib");

const repository = require("./repositories");

require("dotenv").config();

const app = express();

const PORT = process.env.ETL_SERVICE_PORT || 3001;
const RABBITMQ_URL = process.env.ETL_SERVICE_RABBITMQ_URL || "amqp://rabbitmq:5672";

const MODERATED_QUEUE = process.env.ETL_SERVICE_MODERATED_QUEUE || "moderated";
const TYPE_UPDATE_EXCHANGE = process.env.ETL_SERVICE_TYPE_UPDATE_EXCHANGE || "type_update";

let rabbitConnection = null;
let rabbitChannel = null;

app.get("/alive", (req, res) => {
  res.json({
    service: "etl-service",
    status: "alive",
    dbEngine: process.env.ETL_SERVICE_DB_ENGINE || "MYSQL",
  });
});

async function publishTypeUpdate(typeName) {
  const payload = {
    type: typeName,
    emittedAt: new Date().toISOString(),
    source: "etl-service",
  };

  await rabbitChannel.assertExchange(TYPE_UPDATE_EXCHANGE, "fanout", {
    durable: true,
  });

  rabbitChannel.publish(TYPE_UPDATE_EXCHANGE, "", Buffer.from(JSON.stringify(payload)), { persistent: true });

  console.log(`[ETL] Published type_update event for type="${typeName}"`);
}

async function insertJokeMessage(payload) {
  const setup = String(payload.setup || "").trim();
  const punchline = String(payload.punchline || "").trim();
  const type = String(payload.type || "").trim();

  if (!setup || !punchline || !type) {
    throw new Error("Invalid payload");
  }

  const result = await repository.insertJoke({
    setup,
    punchline,
    type,
  });

  return result;
}

async function startConsumer() {
  rabbitConnection = await amqp.connect(RABBITMQ_URL);
  rabbitChannel = await rabbitConnection.createChannel();

  await rabbitChannel.assertQueue(MODERATED_QUEUE, { durable: true });
  await rabbitChannel.assertExchange(TYPE_UPDATE_EXCHANGE, "fanout", {
    durable: true,
  });

  await rabbitChannel.prefetch(1);

  console.log(`[ETL] Listening to queue: ${MODERATED_QUEUE}`);

  await rabbitChannel.consume(MODERATED_QUEUE, async (msg) => {
    if (!msg) return;

    const rawMessage = msg.content.toString();

    try {
      const payload = JSON.parse(rawMessage);

      const result = await insertJokeMessage(payload);

      if (result.typeWasCreated) {
        await publishTypeUpdate(result.type);
      }

      rabbitChannel.ack(msg);

      console.log(`[ETL] Joke inserted successfully`);
    } catch (err) {
      console.error("[ETL] Failed:", err.message);
      rabbitChannel.nack(msg, false, true);
    }
  });
}

app.listen(PORT, async () => {
  console.log(`[ETL] Service running on port ${PORT}`);
  console.log(`[ETL] DB_ENGINE=${process.env.ETL_SERVICE_DB_ENGINE || "MYSQL"}`);

  await startConsumer();
});
