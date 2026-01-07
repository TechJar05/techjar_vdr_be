import connection from "../config/snowflake.js";
import { safeLogWithRequest } from "../utils/activityLogger.js";

// ✅ List all users (optionally filter by role)
export const listUsers = (req, res) => {
  const role = req.query.role; // e.g., /api/users?role=user
  let sql = `SELECT NAME, EMAIL, ROLE, CREATED_AT FROM USERS`;
  if (role) {
    sql += ` WHERE LOWER(ROLE)='${role.toLowerCase()}'`;
  }
  sql += ` ORDER BY CREATED_AT DESC`;

  connection.execute({
    sqlText: sql,
    complete: (err, stmt, rows) => {
      if (err) {
        try { err.message = `${err.message} | SQL: ${sql}`; } catch(e) {}
        return res.status(500).json({ error: err.message });
      }
      res.json(rows || []);
    },
  });
};

// ✅ Update user (Edit)
export const updateUser = (req, res) => {
  const { email } = req.params;
  const { name, role } = req.body;

  const sql = `UPDATE USERS SET NAME='${name}', ROLE='${role}' WHERE EMAIL='${email}'`;
  connection.execute({
    sqlText: sql,
    complete: (err) => {
      if (err) {
        try { err.message = `${err.message} | SQL: ${sql}`; } catch(e) {}
        return res.status(500).json({ error: err.message });
      }
      res.json({ message: "User updated successfully" });
      safeLogWithRequest(req, {
        action: "update_user",
        description: `Updated user ${email}`,
        resourceId: email,
        resourceType: "user",
      });
    },
  });
};

// ✅ Delete a user
export const deleteUser = (req, res) => {
  const { email } = req.params;
  const sql = `DELETE FROM USERS WHERE EMAIL='${email}'`;
  connection.execute({
    sqlText: sql,
    complete: (err) => {
      if (err) {
        try { err.message = `${err.message} | SQL: ${sql}`; } catch(e) {}
        return res.status(500).json({ error: err.message });
      }
      res.json({ message: "User deleted successfully" });
      safeLogWithRequest(req, {
        action: "delete_user",
        description: `Deleted user ${email}`,
        resourceId: email,
        resourceType: "user",
      });
    },
  });
};

// ✅ Get current authenticated user profile
export const getCurrentUser = (req, res) => {
  const email = req.user?.email;
  if (!email) return res.status(401).json({ message: "Unauthorized" });

  const sql = `SELECT NAME, EMAIL, ROLE, CREATED_AT FROM USERS WHERE EMAIL='${email}' LIMIT 1`;
  connection.execute({
    sqlText: sql,
    complete: (err, stmt, rows) => {
      if (err) {
        try { err.message = `${err.message} | SQL: ${sql}`; } catch(e) {}
        return res.status(500).json({ error: err.message });
      }
      if (!rows || rows.length === 0) {
        return res.status(404).json({ message: "User not found" });
      }
      const user = rows[0];
      res.json({
        name: user.NAME,
        email: user.EMAIL,
        role: user.ROLE,
        createdAt: user.CREATED_AT,
      });
    },
  });
};