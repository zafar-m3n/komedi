const { getMongoDb } = require("../db/mongo");

async function insertJoke(payload) {
  const db = await getMongoDb();

  const type = payload.type.trim();

  const existing = await db.collection("types").findOne({ name: type });

  let typeWasCreated = false;

  if (!existing) {
    await db.collection("types").insertOne({ name: type });
    typeWasCreated = true;
  }

  await db.collection("jokes").insertOne({
    setup: payload.setup,
    punchline: payload.punchline,
    type: type,
    created_at: new Date(),
  });

  return {
    type: type,
    typeWasCreated,
  };
}

module.exports = { insertJoke };
