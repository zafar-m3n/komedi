const { getMongoDb } = require("../db/mongo");

async function getTypes() {
  const db = await getMongoDb();

  const rows = await db
    .collection("types")
    .find({}, { projection: { _id: 0, name: 1 } })
    .sort({ name: 1 })
    .toArray();

  return rows;
}

async function getJokesByType(type, count = 1) {
  const db = await getMongoDb();

  const filter = type === "any" ? {} : { type };

  const rows = await db
    .collection("jokes")
    .aggregate([
      { $match: filter },
      { $sample: { size: count } },
      {
        $project: {
          _id: 0,
          setup: 1,
          punchline: 1,
          type: 1,
        },
      },
    ])
    .toArray();

  return rows;
}

module.exports = {
  getTypes,
  getJokesByType,
};
