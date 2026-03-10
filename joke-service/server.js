const express = require("express");
const cors = require("cors");
const pool = require("./db");
require("dotenv").config();

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
    const [rows] = await pool.query("SELECT name FROM types");
    res.json(rows);
  } catch (err) {
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
    const count = parseInt(req.query.count) || 1;

    let query;
    let params = [];

    if (type === "any") {
      query = `
        SELECT setup, punchline, name AS type
        FROM jokes
        JOIN types ON jokes.type_id = types.id
        ORDER BY RAND()
        LIMIT ?
      `;
      params = [count];
    } else {
      query = `
        SELECT setup, punchline, name AS type
        FROM jokes
        JOIN types ON jokes.type_id = types.id
        WHERE types.name = ?
        ORDER BY RAND()
        LIMIT ?
      `;
      params = [type, count];
    }

    const [rows] = await pool.query(query, params);

    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`Joke service running on port ${PORT}`);
});
