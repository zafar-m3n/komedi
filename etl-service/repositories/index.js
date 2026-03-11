require("dotenv").config();

const dbEngine = (process.env.ETL_SERVICE_DB_ENGINE || "MYSQL").toUpperCase();

let repository;

if (dbEngine === "MYSQL") {
  repository = require("./mysqlWriter");
} else if (dbEngine === "MONGO") {
  repository = require("./mongoWriter");
} else {
  throw new Error(`Unsupported DB_ENGINE: ${dbEngine}`);
}

module.exports = repository;
