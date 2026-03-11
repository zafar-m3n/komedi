const express = require("express");
const cors = require("cors");
require("dotenv").config();

const repository = require("./repositories");

const app = express();

app.use(cors());
app.use(express.json());
app.use(express.static("public"));

const PORT = process.env.JOKE_SERVICE_PORT || 3000;

/*
GET /types
Returns all joke types
*/
app.get("/types", async (req, res) => {
  try {
    const rows = await repository.getTypes();
    res.json(rows);
  } catch (err) {
    console.error("GET /types failed:", err);
    res.status(500).json({ error: err.message });
  }
});

/*
GET /joke/:type?count
Returns random jokes
*/
app.get("/joke/:type", async (req, res) => {
  try {
    const type = req.params.type;
    const parsedCount = parseInt(req.query.count, 10);
    const count = Number.isInteger(parsedCount) && parsedCount > 0 ? parsedCount : 1;

    const rows = await repository.getJokesByType(type, count);
    res.json(rows);
  } catch (err) {
    console.error("GET /joke/:type failed:", err);
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`Joke service running on port ${PORT}`);
  console.log(`Active DB engine: ${process.env.JOKE_SERVICE_DB_ENGINE || "MYSQL"}`);
});
