const mysql = require("mysql2/promise");
require("dotenv").config();

const pool = mysql.createPool({
  host: process.env.SUBMIT_SERVICE_DB_HOST,
  user: process.env.SUBMIT_SERVICE_DB_USER,
  password: process.env.SUBMIT_SERVICE_DB_PASSWORD,
  database: process.env.SUBMIT_SERVICE_DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
});

module.exports = pool;
