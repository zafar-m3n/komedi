const express = require("express");
const cors = require("cors");
const pool = require("./db");
const swaggerUi = require("swagger-ui-express");
const swaggerSpec = require("./swagger");

require("dotenv").config();

const app = express();

app.use(cors());
app.use(express.json());
app.use(express.static("public"));

const PORT = process.env.SUBMIT_SERVICE_PORT || 3200;

/**
 * @swagger
 * /types:
 *   get:
 *     summary: Get all joke types
 *     responses:
 *       200:
 *         description: List of types
 */
app.get("/types", async (req, res) => {
  try {
    const [rows] = await pool.query("SELECT name FROM types");

    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * @swagger
 * /submit:
 *   post:
 *     summary: Submit a new joke
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               setup:
 *                 type: string
 *               punchline:
 *                 type: string
 *               type:
 *                 type: string
 *     responses:
 *       200:
 *         description: Joke added
 */
app.post("/submit", async (req, res) => {
  try {
    const { setup, punchline, type } = req.body;

    if (!setup || !punchline || !type) {
      return res.status(400).json({ error: "All fields required" });
    }

    let typeId;

    const [existing] = await pool.query("SELECT id FROM types WHERE name = ?", [type]);

    if (existing.length > 0) {
      typeId = existing[0].id;
    } else {
      const [result] = await pool.query("INSERT INTO types (name) VALUES (?)", [type]);

      typeId = result.insertId;
    }

    await pool.query("INSERT INTO jokes (setup, punchline, type_id) VALUES (?, ?, ?)", [setup, punchline, typeId]);

    res.json({ message: "Joke added successfully" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.use("/docs", swaggerUi.serve, swaggerUi.setup(swaggerSpec));

app.listen(PORT, () => {
  console.log(`Submit service running on port ${PORT}`);
});
