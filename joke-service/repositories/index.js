require("dotenv").config();

const dbEngine = (process.env.JOKE_SERVICE_DB_ENGINE || "MYSQL").toUpperCase();

let repository;

if (dbEngine === "MYSQL") {
  repository = require("./mysqlRepository");
} else if (dbEngine === "MONGO") {
  repository = require("./mongoRepository");
} else {
  throw new Error(`Unsupported DB_ENGINE: ${dbEngine}`);
}

module.exports = repository;
