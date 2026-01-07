import connection from "../config/snowflake.js";
import { ensureUserLogTable } from "../utils/activityLogger.js";

const runQuery = (sqlText, binds = []) =>
  new Promise((resolve, reject) => {
    connection.execute({
      sqlText,
      binds,
      complete: (err, stmt, rows) => {
        if (err) {
          try { err.message = `${err.message} | SQL: ${sqlText}`; } catch (e) {}
          return reject(err);
        }
        resolve({ stmt, rows });
      },
    });
  });

const normalizeDate = (value, endOfDay = false) => {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  if (endOfDay) {
    date.setHours(23, 59, 59, 999);
  }
  return date.toISOString();
};

export const listLogs = async (req, res) => {
  try {
    await ensureUserLogTable();

    const {
      user = "",
      q = "",
      from = "",
      to = "",
      page = 1,
      limit = 100,
    } = req.query;

    const limitNum = Math.min(500, Math.max(1, parseInt(limit, 10) || 100));
    const pageNum = Math.max(1, parseInt(page, 10) || 1);
    const offset = (pageNum - 1) * limitNum;

    const conditions = [];
    const binds = [];

    if (user) {
      conditions.push("(LOWER(USER_EMAIL) = LOWER(?) OR LOWER(USER_NAME) = LOWER(?))");
      binds.push(user, user);
    }

    if (q) {
      const like = `%${q.toLowerCase()}%`;
      conditions.push(
        `(LOWER(DESCRIPTION) LIKE ? OR LOWER(ACTION) LIKE ? OR LOWER(RESOURCE_ID) LIKE ? OR LOWER(RESOURCE_TYPE) LIKE ?)`
      );
      binds.push(like, like, like, like);
    }

    const fromDate = normalizeDate(from, false);
    const toDate = normalizeDate(to, true);

    if (fromDate) {
      conditions.push("CREATED_AT >= ?");
      binds.push(fromDate);
    }

    if (toDate) {
      conditions.push("CREATED_AT <= ?");
      binds.push(toDate);
    }

    const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

    const sql = `
      SELECT
        ID,
        USER_EMAIL,
        USER_NAME,
        ROLE,
        ACTION,
        DESCRIPTION,
        RESOURCE_ID,
        RESOURCE_TYPE,
        IP_ADDRESS,
        USER_AGENT,
        META,
        CREATED_AT
      FROM USER_LOGS
      ${where}
      ORDER BY CREATED_AT DESC
      LIMIT ${limitNum} OFFSET ${offset}
    `;

    const { rows } = await runQuery(sql, binds);

    return res.json({
      data: rows || [],
      meta: { page: pageNum, limit: limitNum, count: rows?.length || 0 },
    });
  } catch (err) {
    console.error("[listLogs] error:", err.message);
    return res.status(500).json({ error: err.message || "Failed to fetch logs" });
  }
};

export default {
  listLogs,
};

