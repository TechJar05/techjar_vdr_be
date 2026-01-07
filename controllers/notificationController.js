import connection from "../config/snowflake.js";
import { esc } from "./_helpers.js";

// Helper: wrap connection.execute in Promise
const execSql = (sql) =>
  new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      complete: (err, stmt, rows) => {
        if (err) return reject(err);
        resolve({ stmt, rows });
      },
    });
  });

// List notifications for current user
export const listNotifications = async (req, res) => {
  try {
    const userEmail = req.user?.email;
    if (!userEmail) return res.status(400).json({ error: "User email missing in token" });

    const sql = `SELECT ID, USER_EMAIL, TITLE, BODY, IS_READ, CREATED_AT FROM NOTIFICATIONS WHERE USER_EMAIL='${esc(userEmail)}' ORDER BY CREATED_AT DESC`;
    const { rows } = await execSql(sql);
    return res.json(rows || []);
  } catch (err) {
    console.error("[listNotifications]", err.message);
    return res.status(500).json({ error: err.message });
  }
};

// Mark a notification read
export const markRead = async (req, res) => {
  try {
    const userEmail = req.user?.email;
    const { id } = req.params;
    if (!userEmail) return res.status(400).json({ error: "User email missing in token" });

    const sql = `UPDATE NOTIFICATIONS SET IS_READ=TRUE WHERE ID=${esc(id)} AND USER_EMAIL='${esc(userEmail)}'`;
    await execSql(sql);
    return res.json({ message: "Marked read" });
  } catch (err) {
    console.error("[markRead]", err.message);
    return res.status(500).json({ error: err.message });
  }
};

// Delete a single notification (or clear all)
export const deleteNotification = async (req, res) => {
  try {
    const userEmail = req.user?.email;
    const { id } = req.params;
    if (!userEmail) return res.status(400).json({ error: "User email missing in token" });

    if (id) {
      const sql = `DELETE FROM NOTIFICATIONS WHERE ID=${esc(id)} AND USER_EMAIL='${esc(userEmail)}'`;
      await execSql(sql);
      return res.json({ message: "Notification deleted" });
    }

    // clear all
    const sql = `DELETE FROM NOTIFICATIONS WHERE USER_EMAIL='${esc(userEmail)}'`;
    await execSql(sql);
    return res.json({ message: "All notifications cleared" });
  } catch (err) {
    console.error("[deleteNotification]", err.message);
    return res.status(500).json({ error: err.message });
  }
};

export default {
  listNotifications,
  markRead,
  deleteNotification,
};
