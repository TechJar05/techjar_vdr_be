// backend/controllers/groupController.js
import connection from "../config/snowflake.js";
import { v4 as uuidv4 } from "uuid";
import { sendMail, isMailerConfigured } from "../config/mailer.js";

// Helper to create a notification for a user
const createNotificationForUser = async (email, title, body) => {
  // Ensure NOTIFICATIONS table exists (simple schema)
  const createTableSql = `CREATE TABLE IF NOT EXISTS NOTIFICATIONS (
    ID NUMBER AUTOINCREMENT,
    USER_EMAIL STRING,
    TITLE STRING,
    BODY STRING,
    IS_READ BOOLEAN DEFAULT FALSE,
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
  )`;
  await execSql(createTableSql);

  const insertSql = `
    INSERT INTO NOTIFICATIONS (USER_EMAIL, TITLE, BODY, IS_READ, CREATED_AT)
    SELECT
      '${esc(email)}' as USER_EMAIL,
      '${esc(title)}' as TITLE,
      '${esc(body)}' as BODY,
      FALSE as IS_READ,
      CURRENT_TIMESTAMP() as CREATED_AT
  `;
  await execSql(insertSql);
};

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

// Helper: escape single quotes for SQL
const esc = (s = "") => String(s).replace(/'/g, "''");

// List all groups from USER_GROUPS table
export const listGroups = (req, res) => {
  const sql = `SELECT ID, GROUP_NAME, MEMBERS, CREATED_BY, CREATED_AT FROM USER_GROUPS ORDER BY CREATED_AT DESC`;
  execSql(sql)
    .then(({ rows }) => {
      // Normalize MEMBERS (may be VARIANT, object, array, or JSON string)
      const normalized = (rows || []).map((r) => {
        try {
          if (!r) return { ...r, MEMBERS: [] };
          const m = r.MEMBERS;
          if (Array.isArray(m)) return { ...r, MEMBERS: m };
          if (m && typeof m === "object") {
            // Snowflake VARIANT as object — convert to array if it looks like a list
            return { ...r, MEMBERS: Array.isArray(m) ? m : Object.values(m) };
          }
          if (typeof m === "string") {
            try {
              const parsed = JSON.parse(m || "[]");
              return { ...r, MEMBERS: Array.isArray(parsed) ? parsed : [] };
            } catch {
              return { ...r, MEMBERS: (m || "").split(",").map((s) => s.trim()).filter(Boolean) };
            }
          }
          return { ...r, MEMBERS: [] };
        } catch (e) {
          console.error("Error normalizing group:", e);
          return { ...r, MEMBERS: [] };
        }
      });
      res.json(normalized);
    })
    .catch((err) => {
      console.error("[listGroups] error:", err.message);
      res.status(500).json({ error: err.message });
    });
};

// Create new group
export const createGroup = async (req, res) => {
  try {
    const { groupName, users, adminName } = req.body;

    if (!groupName?.trim()) {
      return res.status(400).json({ error: "Group name is required" });
    }
    if (!Array.isArray(users) || users.length === 0) {
      return res.status(400).json({ error: "At least one member email is required" });
    }

    const groupId = uuidv4();
    const membersJson = JSON.stringify(users);

    // Insert into USER_GROUPS using INSERT...SELECT to allow TRY_PARSE_JSON
    // Don't specify ID — let Snowflake auto-generate or use CURRENT_TIMESTAMP() for uniqueness
    const insertGroupSql = `
      INSERT INTO USER_GROUPS (GROUP_NAME, MEMBERS, CREATED_BY, CREATED_AT)
      SELECT 
        '${esc(groupName)}' as GROUP_NAME,
        TRY_PARSE_JSON('${esc(membersJson)}') as MEMBERS,
        '${esc(adminName || "")}' as CREATED_BY,
        CURRENT_TIMESTAMP() as CREATED_AT
    `;

    console.log("[createGroup] insertGroupSql:", insertGroupSql);

    await execSql(insertGroupSql);

    // Create in-app notifications for each user (ensure persistence even if email fails)
    (async () => {
      for (const email of users) {
        try {
          await createNotificationForUser(
            email,
            `Added to Group: ${groupName}`,
            `You have been added to group ${groupName}${adminName ? ` by ${adminName}` : ""}.` 
          );
        } catch (err) {
          console.error("[createGroup] createNotification error for", email, ":", err.message);
        }
      }
    })();

    // Send notification emails asynchronously (best-effort). Missing SMTP credentials won't block API success.
    (async () => {
      for (const email of users) {
        try {
          if (!isMailerConfigured) {
            console.warn("SMTP not configured; skipping send for", email);
          } else {
            await sendMail(email, `Added to Group: ${groupName}`, `\n              <p>Dear User,</p>\n              <p>You have been added to <strong>${groupName}</strong>${adminName ? ` by Admin <strong>${adminName}</strong>` : "."}  </p>\n              <p>Login to your dashboard to view group details.</p>\n              <br/>\n              <p>Best regards,<br/>VDR Team</p>\n            `);
            console.log("[createGroup] Email sent to:", email);
          }
        } catch (err) {
          console.error("[createGroup] sendMail error for", email, ":", err && err.message ? err.message : err);
        }
      }
    })();

    // Return success immediately
    return res.status(201).json({ 
      message: "Group created successfully. Users will be notified (in-app).", 
      groupName
    });
  } catch (err) {
    console.error("[createGroup] error:", err.message);
    return res.status(500).json({ error: err.message || "Failed to create group" });
  }
};

// Delete a group
export const deleteGroup = async (req, res) => {
  const { id } = req.params;
  try {
    // Fetch the group's members so we can notify them
    const selectSql = `SELECT ID, GROUP_NAME, MEMBERS FROM USER_GROUPS WHERE ID='${esc(id)}'`;
    console.log("[deleteGroup] selectSql:", selectSql);
    const { rows } = await execSql(selectSql);
    const row = (rows && rows[0]) || null;
    let members = [];
    const groupName = row && row.GROUP_NAME;

    if (row && row.MEMBERS) {
      try {
        const m = row.MEMBERS;
        if (Array.isArray(m)) members = m;
        else if (typeof m === "string") members = JSON.parse(m || "[]");
        else if (typeof m === "object") members = Array.isArray(m) ? m : Object.values(m);
      } catch (e) {
        try {
          members = (row.MEMBERS || "").split(",").map((s) => s.trim()).filter(Boolean);
        } catch (e2) {
          members = [];
        }
      }
    }

    // Send notification emails to members (non-blocking for API response but we await here to log any errors)
    // Create in-app notifications for removal and try to send emails (best-effort)
    (async () => {
      for (const email of members) {
        const target = typeof email === "string" ? email : (email.email || email);
        try {
          await createNotificationForUser(
            target,
            `Removed from Group: ${groupName || "(group)"}`,
            `You have been removed from group ${groupName || "(group)"}.`
          );
        } catch (err) {
          console.error("[deleteGroup] createNotification error for", target, ":", err.message);
        }

        try {
          if (!isMailerConfigured) {
            console.warn("SMTP not configured; skipping removal send for", target);
          } else {
            await sendMail(target, `Removed from Group: ${groupName || "(group)"}`, `\n              <p>Dear User,</p>\n              <p>You have been removed from the group <strong>${groupName || "(group)"}</strong>.</p>\n              <p>If you have questions, contact your admin.</p>\n              <br/>\n              <p>Best regards,<br/>VDR Team</p>\n            `);
            console.log("[deleteGroup] removal email sent to:", target);
          }
        } catch (err) {
          console.error("[deleteGroup] sendMail error for", target, ":", err && err.message ? err.message : err);
        }
      }
    })();

    // Delete the group
    const deleteSql = `DELETE FROM USER_GROUPS WHERE ID='${esc(id)}'`;
    console.log("[deleteGroup] deleteSql:", deleteSql);
    await execSql(deleteSql);

    return res.json({ message: "Group deleted successfully" });
  } catch (err) {
    console.error("[deleteGroup] error:", err.message);
    return res.status(500).json({ error: err.message });
  }
};
