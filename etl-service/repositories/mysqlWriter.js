const pool = require("../db/mysql");

async function ensureTypeExists(connection, typeName) {
  const [existingRows] = await connection.query("SELECT id FROM types WHERE name = ? LIMIT 1", [typeName]);

  if (existingRows.length > 0) {
    return {
      typeId: existingRows[0].id,
      wasCreated: false,
      type: typeName,
    };
  }

  try {
    const [insertResult] = await connection.query("INSERT INTO types (name) VALUES (?)", [typeName]);

    return {
      typeId: insertResult.insertId,
      wasCreated: true,
      type: typeName,
    };
  } catch (err) {
    if (err.code === "ER_DUP_ENTRY") {
      const [rows] = await connection.query("SELECT id FROM types WHERE name = ? LIMIT 1", [typeName]);

      return {
        typeId: rows[0].id,
        wasCreated: false,
        type: typeName,
      };
    }

    throw err;
  }
}

async function insertJoke(payload) {
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const typeResult = await ensureTypeExists(connection, payload.type);

    await connection.query("INSERT INTO jokes (setup, punchline, type_id) VALUES (?, ?, ?)", [
      payload.setup,
      payload.punchline,
      typeResult.typeId,
    ]);

    await connection.commit();

    return {
      type: payload.type,
      typeWasCreated: typeResult.wasCreated,
    };
  } catch (err) {
    await connection.rollback();
    throw err;
  } finally {
    connection.release();
  }
}

module.exports = { insertJoke };
