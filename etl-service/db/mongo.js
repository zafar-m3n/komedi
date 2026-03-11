const { MongoClient } = require("mongodb");
require("dotenv").config();

let client;
let database;

async function getMongoDb() {
  if (database) return database;

  const url = process.env.ETL_SERVICE_MONGO_URL || "mongodb://mongo:27017";
  const dbName = process.env.ETL_SERVICE_MONGO_DB_NAME || "jokes_db";

  client = new MongoClient(url);
  await client.connect();

  database = client.db(dbName);

  console.log(`[ETL] Connected to MongoDB: ${dbName}`);

  return database;
}

module.exports = { getMongoDb };
