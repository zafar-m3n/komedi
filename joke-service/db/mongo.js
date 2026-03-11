const { MongoClient } = require("mongodb");
require("dotenv").config();

let client;
let database;

async function getMongoDb() {
  if (database) {
    return database;
  }

  const mongoUrl = process.env.JOKE_SERVICE_MONGO_URL || "mongodb://mongo:27017";
  const dbName = process.env.JOKE_SERVICE_MONGO_DB_NAME || "jokes_db";

  client = new MongoClient(mongoUrl);
  await client.connect();

  database = client.db(dbName);

  console.log(`Connected to MongoDB database: ${dbName}`);

  return database;
}

module.exports = { getMongoDb };
