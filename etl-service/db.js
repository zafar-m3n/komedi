const mysql = require("mysql2/promise");
require("dotenv").config();

const pool = mysql.createPool({
  host: process.env.ETL_SERVICE_DB_HOST,
  user: process.env.ETL_SERVICE_DB_USER,
  password: process.env.ETL_SERVICE_DB_PASSWORD,
  database: process.env.ETL_SERVICE_DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

module.exports = pool;
