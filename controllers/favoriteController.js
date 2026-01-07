import { v4 as uuidv4 } from "uuid";
import connection from "../config/snowflake.js";
import { safeLogWithRequest } from "../utils/activityLogger.js";

let favoritesTableEnsured = false;

const ensureFavoritesTable = () =>
  new Promise((resolve, reject) => {
    if (favoritesTableEnsured) return resolve();

    const sql = `
      CREATE TABLE IF NOT EXISTS FAVORITES (
        ID STRING NOT NULL,
        USER_EMAIL STRING NOT NULL,
        ITEM_ID STRING NOT NULL,
        ITEM_TYPE STRING NOT NULL,
        CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
      )
    `;

    connection.execute({
      sqlText: sql,
      complete: (err) => {
        if (err) return reject(err);
        favoritesTableEnsured = true;
        resolve();
      },
    });
  });

const ensureFavoritesIndexes = () =>
  new Promise((resolve) => {
    if (!favoritesTableEnsured) return resolve();
    // Snowflake automatically optimizes; placeholder to keep API symmetrical.
    resolve();
  });

export const listFavorites = async (req, res) => {
  try {
    await ensureFavoritesTable();
    await ensureFavoritesIndexes();
  } catch (error) {
    console.error("Error ensuring FAVORITES table:", error);
    return res.status(500).json({ error: error.message });
  }

  const userEmail = req.user.email;

  const sql = `
    WITH FOLDER_STATS AS (
      SELECT
        FOLDER_ID,
        COUNT(*) AS FILE_COUNT,
        SUM(COALESCE(FILE_SIZE, 0)) AS TOTAL_SIZE
      FROM FILES
      GROUP BY FOLDER_ID
    )
    SELECT
      FAV.ID,
      FAV.ITEM_ID,
      FAV.ITEM_TYPE,
      FAV.CREATED_AT,
      CASE WHEN FAV.ITEM_TYPE = 'folder' THEN F.NAME ELSE FI.FILE_NAME END AS NAME,
      CASE WHEN FAV.ITEM_TYPE = 'folder' THEN FS.FILE_COUNT ELSE NULL END AS FILE_COUNT,
      CASE WHEN FAV.ITEM_TYPE = 'folder' THEN FS.TOTAL_SIZE ELSE FI.FILE_SIZE END AS SIZE_BYTES,
      CASE WHEN FAV.ITEM_TYPE = 'file' THEN FI.FILE_TYPE ELSE NULL END AS FILE_TYPE,
      FI.FOLDER_ID AS PARENT_FOLDER_ID,
      FI.FILE_URL
    FROM FAVORITES FAV
    LEFT JOIN FOLDERS F ON FAV.ITEM_TYPE = 'folder' AND FAV.ITEM_ID = F.ID
    LEFT JOIN FOLDER_STATS FS ON FS.FOLDER_ID = F.ID
    LEFT JOIN FILES FI ON FAV.ITEM_TYPE = 'file' AND FAV.ITEM_ID = FI.ID
    WHERE FAV.USER_EMAIL='${userEmail}'
    ORDER BY FAV.CREATED_AT DESC
  `;

  connection.execute({
    sqlText: sql,
    complete: (err, stmt, rows) => {
      if (err) return res.status(500).json({ error: err.message });
      res.json(rows || []);
    },
  });
};

export const addFavorite = async (req, res) => {
  const { itemId, itemType } = req.body;
  const userEmail = req.user.email;

  if (!itemId || !itemType) {
    return res.status(400).json({ message: "itemId and itemType are required" });
  }

  try {
    await ensureFavoritesTable();
  } catch (error) {
    console.error("Error ensuring FAVORITES table:", error);
    return res.status(500).json({ error: error.message });
  }

  const id = uuidv4();
  // Run DELETE and INSERT as two separate statements because the Snowflake
  // SDK may not accept multi-statement SQL in a single execute call.
  const deleteSql = `DELETE FROM FAVORITES WHERE USER_EMAIL='${userEmail}' AND ITEM_ID='${itemId}' AND ITEM_TYPE='${itemType}'`;
  const insertSql = `INSERT INTO FAVORITES (ID, USER_EMAIL, ITEM_ID, ITEM_TYPE, CREATED_AT)
    VALUES ('${id}', '${userEmail}', '${itemId}', '${itemType}', CURRENT_TIMESTAMP())`;

  // log incoming data for debugging
  console.log("[addFavorite] req.user:", req.user);
  console.log("[addFavorite] body:", req.body);

  try {
    await new Promise((resolve, reject) => {
      connection.execute({
        sqlText: deleteSql,
        complete: (err) => (err ? reject(err) : resolve()),
      });
    });

    console.log("[addFavorite] deleteSql:", deleteSql);
    console.log("[addFavorite] insertSql:", insertSql);

    await new Promise((resolve, reject) => {
      connection.execute({
        sqlText: insertSql,
        complete: (err) => (err ? reject(err) : resolve()),
      });
    });

    safeLogWithRequest(req, {
      action: "add_favorite",
      description: `Added ${itemId} to favorites`,
      resourceId: itemId,
      resourceType: itemType,
    });

    return res.status(201).json({ id, itemId, itemType });
  } catch (err) {
    console.error("Error adding favorite:", err);
    return res.status(500).json({ error: err.message });
  }
};

export const removeFavorite = async (req, res) => {
  const { itemId } = req.params;
  const { type } = req.query;
  const userEmail = req.user.email;

  if (!itemId || !type) {
    return res.status(400).json({ message: "itemId and type are required" });
  }

  try {
    await ensureFavoritesTable();
  } catch (error) {
    console.error("Error ensuring FAVORITES table:", error);
    return res.status(500).json({ error: error.message });
  }

  const sql = `DELETE FROM FAVORITES WHERE USER_EMAIL='${userEmail}' AND ITEM_ID='${itemId}' AND ITEM_TYPE='${type}'`;
  connection.execute({
    sqlText: sql,
    complete: (err) => {
      if (err) return res.status(500).json({ error: err.message });
      res.json({ message: "Removed from favorites" });
      safeLogWithRequest(req, {
        action: "remove_favorite",
        description: `Removed ${itemId} from favorites`,
        resourceId: itemId,
        resourceType: type,
      });
    },
  });
};

