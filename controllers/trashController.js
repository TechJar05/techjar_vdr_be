import connection from "../config/snowflake.js";
import ensureTrashTable from "../utils/ensureTrashTable.js";
import { safeLogWithRequest } from "../utils/activityLogger.js";
import s3 from "../config/s3.js";

const runQuery = (sqlText, binds = []) =>
  new Promise((resolve, reject) => {
    connection.execute({
      sqlText,
      binds,
      complete: (err, stmt, rows) => {
        if (err) {
          try { err.message = `${err.message} | SQL: ${sqlText}`; } catch(e) {}
          return reject(err);
        }
        resolve({ stmt, rows });
      },
    });
  });

export const listTrash = async (req, res) => {
  try {
    await ensureTrashTable();
    const { rows } = await runQuery(
      `SELECT
        T.ID,
        T.ITEM_TYPE,
        T.ITEM_NAME,
        T.FOLDER_ID,
        T.FILE_URL,
        T.FILE_SIZE,
        T.FILE_TYPE,
        T.UPLOADED_BY,
        T.UPLOADED_AT,
        T.CREATED_BY,
        T.CREATED_AT,
        T.DELETED_BY,
        T.DELETED_AT,
        T.RESTORED
      FROM TRASH AS T
      ORDER BY T.DELETED_AT DESC`
    );

    res.json(rows || []);
  } catch (error) {
    console.error("listTrash error:", error);
    res.status(500).json({ error: error.message || "Failed to list trash" });
  }
};

export const restoreItem = async (req, res) => {
  const { id } = req.params;

  try {
    await ensureTrashTable();
    const { rows } = await runQuery(`SELECT * FROM TRASH WHERE ID = ?`, [id]);

    if (!rows || !rows.length) {
      return res.status(404).json({ message: "Item not found in trash" });
    }

    const item = rows[0];
    const type = String(item.ITEM_TYPE || "").toLowerCase();
    const fallbackTimestamp = new Date().toISOString();

    if (type === "file") {
      await runQuery(
        `INSERT INTO FILES (
          ID,
          FOLDER_ID,
          FILE_NAME,
          FILE_URL,
          FILE_SIZE,
          FILE_TYPE,
          UPLOADED_BY,
          UPLOADED_AT
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          item.ID,
          item.FOLDER_ID,
          item.ITEM_NAME,
          item.FILE_URL,
          item.FILE_SIZE || 0,
          item.FILE_TYPE || "application/octet-stream",
          item.UPLOADED_BY || item.CREATED_BY || req.user?.email || "system",
          item.UPLOADED_AT || item.CREATED_AT || fallbackTimestamp,
        ]
      );
    } else if (type === "folder") {
      await runQuery(
        `INSERT INTO FOLDERS (
          ID,
          NAME,
          CREATED_BY,
          CREATED_AT
        )
        VALUES (?, ?, ?, ?)`,
        [
          item.ID,
          item.ITEM_NAME,
          item.CREATED_BY || req.user?.email || "system",
          item.CREATED_AT || fallbackTimestamp,
        ]
      );
    } else {
      return res.status(400).json({ message: "Unsupported item type" });
    }

    await runQuery(`DELETE FROM TRASH WHERE ID = ?`, [id]);

    res.json({ message: "Item restored" });
    safeLogWithRequest(req, {
      action: "restore_item",
      description: `Restored ${type} ${id} from trash`,
      resourceId: id,
      resourceType: type,
    });
  } catch (error) {
    console.error("restoreItem error:", error);
    res.status(500).json({ error: error.message || "Failed to restore item" });
  }
};

export const permanentlyDeleteItem = async (req, res) => {
  const { id } = req.params;

  try {
    await ensureTrashTable();
    const { rows } = await runQuery(`SELECT * FROM TRASH WHERE ID = ?`, [id]);

    if (!rows || !rows.length) {
      return res.status(404).json({ message: "Item not found in trash" });
    }

    const item = rows[0];
    const type = String(item.ITEM_TYPE || "").toLowerCase();
    const itemName = item.ITEM_NAME || "Unknown";

    // If it's a file, delete from S3
    if (type === "file" && item.FILE_URL) {
      try {
        const fileUrl = item.FILE_URL;
        const urlParts = new URL(fileUrl);
        const fileKey = decodeURIComponent(urlParts.pathname.replace(/^\/+/, ""));

        await s3.deleteObject({
          Bucket: process.env.AWS_BUCKET_NAME,
          Key: fileKey,
        }).promise();
        console.log(`[permanentlyDeleteItem] Deleted file from S3: ${fileKey}`);
      } catch (s3Error) {
        console.error("[permanentlyDeleteItem] S3 delete error:", s3Error.message);
        // Continue with database deletion even if S3 delete fails
      }
    }

    // Delete from TRASH table
    await runQuery(`DELETE FROM TRASH WHERE ID = ?`, [id]);

    res.json({ message: "Item permanently deleted" });
    safeLogWithRequest(req, {
      action: "permanent_delete",
      description: `Permanently deleted ${type} "${itemName}" from trash`,
      resourceId: id,
      resourceType: type,
      meta: { itemName, wasFile: type === "file" },
    });
  } catch (error) {
    console.error("permanentlyDeleteItem error:", error);
    res.status(500).json({ error: error.message || "Failed to permanently delete item" });
  }
};
