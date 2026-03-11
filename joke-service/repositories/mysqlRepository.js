const pool = require("../db/mysql");

async function getTypes() {
  const [rows] = await pool.query(
    `
    SELECT name
    FROM types
    ORDER BY name ASC
    `,
  );

  return rows;
}

async function getJokesByType(type, count = 1) {
  let query = "";
  let params = [];

  if (type === "any") {
    query = `
      SELECT jokes.setup, jokes.punchline, types.name AS type
      FROM jokes
      JOIN types ON jokes.type_id = types.id
      ORDER BY RAND()
      LIMIT ?
    `;
    params = [count];
  } else {
    query = `
      SELECT jokes.setup, jokes.punchline, types.name AS type
      FROM jokes
      JOIN types ON jokes.type_id = types.id
      WHERE types.name = ?
      ORDER BY RAND()
      LIMIT ?
    `;
    params = [type, count];
  }

  const [rows] = await pool.query(query, params);
  return rows;
}

module.exports = {
  getTypes,
  getJokesByType,
};
