import connection from "../config/snowflake.js";
import { v4 as uuidv4 } from "uuid";
import { sendMail } from "../utils/sendMail.js";
import { safeLogWithRequest } from "../utils/activityLogger.js";

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

/**
 * REQUEST ACCESS - User requests access to file or folder
 * Body: { itemId, itemType: 'file'|'folder', itemName, accessTypes: ['VIEW', 'DOWNLOAD'] }
 */
export const requestAccess = async (req, res) => {
  try {
    const { itemId, itemType, itemName, accessTypes } = req.body;
    const userEmail = req.user?.email;
    if (!userEmail) return res.status(400).json({ error: "User email missing" });
    if (!itemId || !itemType || !itemName || !Array.isArray(accessTypes) || accessTypes.length === 0) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    const requestId = uuidv4();
    const accessTypesStr = accessTypes.join(",");

    // Ensure ACCESS_REQUESTS table exists with correct schema (CREATE IF NOT EXISTS)
    const createTableSql = `CREATE TABLE IF NOT EXISTS ACCESS_REQUESTS (
      ID STRING PRIMARY KEY,
      USER_EMAIL STRING,
      ITEM_ID STRING,
      ITEM_TYPE STRING,
      ITEM_NAME STRING,
      ACCESS_TYPES STRING,
      STATUS STRING DEFAULT 'pending',
      REQUESTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
      APPROVED_AT TIMESTAMP_LTZ,
      APPROVED_BY STRING
    )`;
    try {
      await execSql(createTableSql);
      console.log("[requestAccess] ACCESS_REQUESTS table verified");
    } catch (createErr) {
      console.error("[requestAccess] CREATE TABLE error:", createErr.message);
    }

    const insertSql = `
      INSERT INTO ACCESS_REQUESTS (ID, USER_EMAIL, ITEM_ID, ITEM_TYPE, ITEM_NAME, ACCESS_TYPES, STATUS)
      VALUES ('${esc(requestId)}', '${esc(userEmail)}', '${esc(itemId)}', '${esc(itemType)}', '${esc(itemName)}', '${esc(accessTypesStr)}', 'pending')
    `;
    console.log("[requestAccess] Executing INSERT with ID:", requestId);
    await execSql(insertSql);
    safeLogWithRequest(req, {
      action: "request_access",
      description: `Requested ${accessTypesStr} for ${itemType} ${itemName}`,
      resourceId: itemId,
      resourceType: itemType,
      meta: { accessTypes: accessTypes },
    });

    // Notify admins (best-effort)
    try {
      const adminEmails = await getAdminEmails();
      for (const adminEmail of adminEmails) {
        await sendMail(
          adminEmail,
          `Access Request: ${itemName}`,
          `<p>${userEmail} requested ${accessTypes.join(", ")} access to ${itemType}: ${itemName}</p>`
        );
      }
    } catch (e) {
      console.error("Failed to notify admins:", e.message);
    }

    return res.status(201).json({ message: "Access request sent", requestId });
  } catch (err) {
    console.error("[requestAccess] error:", err.message);
    return res.status(500).json({ error: err.message });
  }
};

/**
 * GET ADMIN EMAILS - Helper to get all admin emails
 */
const getAdminEmails = async () => {
  try {
    const sql = `SELECT EMAIL FROM USERS WHERE ROLE='admin' LIMIT 10`;
    const { rows } = await execSql(sql);
    return (rows || []).map((r) => r.EMAIL).filter(Boolean);
  } catch (e) {
    console.error("getAdminEmails error:", e.message);
    return [];
  }
};

/**
 * LIST ACCESS REQUESTS - Admin views all pending requests
 */
export const listAccessRequests = async (req, res) => {
  try {
    const userRole = req.user?.role;
    if (userRole !== "admin") {
      return res.status(403).json({ error: "Admin only" });
    }

    const sql = `SELECT * FROM ACCESS_REQUESTS ORDER BY REQUESTED_AT DESC LIMIT 100`;
    const { rows } = await execSql(sql);
    return res.json(rows || []);
  } catch (err) {
    console.error("[listAccessRequests] error:", err.message);
    return res.status(500).json({ error: err.message });
  }
};

/**
 * APPROVE/REJECT REQUEST - Admin approves or rejects a request
 * Body: { status: 'approved'|'rejected' }
 */
export const updateAccessStatus = async (req, res) => {
  try {
    const { id } = req.params;
    const { status } = req.body;
    const adminEmail = req.user?.email;
    if (!adminEmail) return res.status(400).json({ error: "Admin email missing" });
    if (!["approved", "rejected"].includes(status)) {
      return res.status(400).json({ error: "Invalid status" });
    }

    // Fetch request details
    const selectSql = `SELECT * FROM ACCESS_REQUESTS WHERE ID='${esc(id)}'`;
    const { rows } = await execSql(selectSql);
    const request = rows && rows[0];
    if (!request) return res.status(404).json({ error: "Request not found" });

    // Update status
    const updateSql = `
      UPDATE ACCESS_REQUESTS
      SET STATUS='${status}', APPROVED_AT=CURRENT_TIMESTAMP(), APPROVED_BY='${esc(adminEmail)}'
      WHERE ID='${esc(id)}'
    `;
    await execSql(updateSql);

    // If approved and it's a file, increment SHARES_COUNT only if this is the first approval for this user+file combination
    if (status === "approved" && request.ITEM_TYPE === "file") {
      try {
        // Check if this user already has another approved request for this file
        const checkSql = `
          SELECT COUNT(*) AS CNT FROM ACCESS_REQUESTS 
          WHERE ITEM_ID='${esc(request.ITEM_ID)}' 
          AND USER_EMAIL='${esc(request.USER_EMAIL)}' 
          AND ITEM_TYPE='file' 
          AND STATUS='approved'
          AND ID != '${esc(id)}'
        `;
        const { rows: checkRows } = await execSql(checkSql);
        const existingApprovals = checkRows && checkRows.length > 0 ? Number(checkRows[0].CNT || 0) : 0;
        
        // Only increment if this is the first approval for this user+file combination
        if (existingApprovals === 0) {
          // Ensure SHARES_COUNT column exists
          await execSql("ALTER TABLE FILES ADD COLUMN IF NOT EXISTS SHARES_COUNT NUMBER DEFAULT 0");
          // Read current value and update with a literal to avoid ambiguous column compile errors
          try {
            const selSql = `SELECT SHARES_COUNT FROM FILES WHERE ID='${esc(request.ITEM_ID)}' LIMIT 1`;
            let current = 0;
            try {
              const { rows: curRows } = await execSql(selSql);
              if (curRows && curRows.length > 0) current = Number(curRows[0].SHARES_COUNT || 0);
            } catch (selErr) {
              console.warn('[updateAccessStatus] Could not read SHARES_COUNT for', request.ITEM_ID, selErr.message);
            }
            const newVal = current + 1;
            const updateSql = `UPDATE FILES SET SHARES_COUNT = ${newVal} WHERE ID='${esc(request.ITEM_ID)}'`;
            try {
              await execSql(updateSql);
              console.log(`[updateAccessStatus] Incremented SHARES_COUNT for file: ${request.ITEM_ID} (user: ${request.USER_EMAIL}) -> ${newVal}`);
            } catch (updateErr) {
              console.warn('[updateAccessStatus] Initial SHARES_COUNT update failed:', updateErr && updateErr.message ? updateErr.message : updateErr);
              if (updateErr && typeof updateErr.message === 'string' && updateErr.message.toLowerCase().includes('ambiguous')) {
                const fallbackSql = `UPDATE FILES SET SHARES_COUNT = (SELECT COALESCE(F2.SHARES_COUNT,0) + 1 FROM FILES F2 WHERE F2.ID='${esc(request.ITEM_ID)}') WHERE ID='${esc(request.ITEM_ID)}'`;
                try {
                  await execSql(fallbackSql);
                  console.log(`[updateAccessStatus] Fallback SHARES_COUNT increment succeeded for file: ${request.ITEM_ID}`);
                } catch (fbErr) {
                  console.warn('[updateAccessStatus] Fallback update also failed:', fbErr && fbErr.message ? fbErr.message : fbErr, 'FallbackSQL:', fallbackSql);
                }
              } else {
                console.warn('[updateAccessStatus] Could not update SHARES_COUNT:', updateErr && updateErr.message ? updateErr.message : updateErr);
              }
            }
          } catch (innerErr) {
            console.warn('[updateAccessStatus] Could not update SHARES_COUNT:', innerErr.message);
          }
        } else {
          console.log(`[updateAccessStatus] User ${request.USER_EMAIL} already has approved access to file ${request.ITEM_ID}, SHARES_COUNT not incremented`);
        }
      } catch (shareErr) {
        console.warn("[updateAccessStatus] Could not update SHARES_COUNT:", shareErr.message);
      }
    }

    // Notify user (best-effort)
    try {
      const subject = `Access Request ${status === "approved" ? "Approved" : "Rejected"}`;
      const body = status === "approved"
        ? `Your request for ${request.ACCESS_TYPES} access to ${request.ITEM_NAME} has been approved.`
        : `Your request for ${request.ACCESS_TYPES} access to ${request.ITEM_NAME} has been rejected.`;
      await sendMail(request.USER_EMAIL, subject, `<p>${body}</p>`);
    } catch (e) {
      console.error("Failed to notify user:", e.message);
    }

    // Create in-app notification for user
    try {
      const notifTitle = `Access ${status === "approved" ? "Approved" : "Rejected"}: ${request.ITEM_NAME}`;
      const notifBody = `Your request for ${request.ACCESS_TYPES} has been ${status}.`;
      const notifSql = `
        INSERT INTO NOTIFICATIONS (USER_EMAIL, TITLE, BODY, IS_READ, CREATED_AT)
        SELECT '${esc(request.USER_EMAIL)}', '${esc(notifTitle)}', '${esc(notifBody)}', FALSE, CURRENT_TIMESTAMP()
      `;
      await execSql(notifSql);
    } catch (e) {
      console.error("Failed to create notification:", e.message);
    }

    res.json({ message: `Request ${status}` });
    safeLogWithRequest(req, {
      action: `access_${status}`,
      description: `Access ${status} for ${request.ITEM_TYPE} ${request.ITEM_NAME}`,
      resourceId: request.ITEM_ID,
      resourceType: request.ITEM_TYPE,
      meta: { accessTypes: request.ACCESS_TYPES, requestId: id },
    });
    return;
  } catch (err) {
    console.error("[updateAccessStatus] error:", err.message);
    return res.status(500).json({ error: err.message });
  }
};

/**
 * CHECK USER ACCESS - Check if user has access to an item
 * GET /api/access/check?itemId=X&itemType=file
 */
export const checkUserAccess = async (req, res) => {
  try {
    const { itemId, itemType } = req.query;
    const userEmail = req.user?.email;
    const userRole = req.user?.role;
    console.log("[checkUserAccess] Query:", { itemId, itemType, userEmail, userRole });
    if (!userEmail) return res.status(400).json({ error: "User email missing" });

    // Admin has all access
    if (userRole === "admin") {
      console.log("[checkUserAccess] Admin user, granting full access");
      return res.json({ hasAccess: true, accessTypes: ["VIEW", "DOWNLOAD", "COMMENT", "UPLOAD"] });
    }

    // Ensure table exists
    try {
      const tableCheckSql = `SHOW TABLES LIKE 'ACCESS_REQUESTS'`;
      console.log("[checkUserAccess] Checking table existence...");
      await execSql(tableCheckSql);
    } catch (tableErr) {
      console.warn("[checkUserAccess] Table check:", tableErr.message);
    }

    // Check if user has approved access request - aggregate all approved access types
    const sql = `
      SELECT ACCESS_TYPES FROM ACCESS_REQUESTS
      WHERE USER_EMAIL='${esc(userEmail)}' AND ITEM_ID='${esc(itemId)}' AND ITEM_TYPE='${esc(itemType)}' AND STATUS='approved'
    `;
    console.log("[checkUserAccess] Executing SQL...");
    const { rows } = await execSql(sql);
    console.log("[checkUserAccess] Query result:", rows);
    
    if (!rows || rows.length === 0) {
      console.log("[checkUserAccess] No approved access found, returning empty");
      return res.json({ hasAccess: false, accessTypes: [] });
    }

    // Aggregate all access types from all approved requests
    const allAccessTypes = new Set();
    rows.forEach((row) => {
      const accessTypesStr = row.ACCESS_TYPES || "";
      const types = accessTypesStr.split(",").map((s) => s.trim()).filter(Boolean);
      types.forEach((t) => allAccessTypes.add(t));
    });
    
    const accessTypes = Array.from(allAccessTypes);
    console.log("[checkUserAccess] Approved access found:", accessTypes);
    return res.json({ hasAccess: true, accessTypes });
  } catch (err) {
    console.error("[checkUserAccess] error:", err.message);
    console.error("[checkUserAccess] full error:", err);
    return res.status(500).json({ error: err.message });
  }
};

/**
 * REVOKE ALL ACCESS - Admin revokes all access to an item for a user
 * DELETE /api/access/requests/:id
 */
export const revokeAllAccess = async (req, res) => {
  try {
    const { id } = req.params;
    const userRole = req.user?.role;
    if (userRole !== "admin") {
      return res.status(403).json({ error: "Admin only" });
    }

    // Fetch request details to get user email
    const selectSql = `SELECT * FROM ACCESS_REQUESTS WHERE ID='${esc(id)}'`;
    const { rows } = await execSql(selectSql);
    const request = rows && rows[0];
    if (!request) return res.status(404).json({ error: "Request not found" });

    // Delete the access request record
    const deleteSql = `DELETE FROM ACCESS_REQUESTS WHERE ID='${esc(id)}'`;
    await execSql(deleteSql);
    console.log("[revokeAllAccess] Revoked all access for request:", id);

    // Notify user of revocation (best-effort)
    try {
      await sendMail(
        request.USER_EMAIL,
        `Access Revoked: ${request.ITEM_NAME}`,
        `<p>Your access to ${request.ITEM_NAME} has been revoked.</p>`
      );
    } catch (e) {
      console.error("Failed to notify user of revocation:", e.message);
    }

    res.json({ message: "All access revoked successfully" });
    safeLogWithRequest(req, {
      action: "revoke_access_all",
      description: `Revoked all access for ${request.USER_EMAIL} on ${request.ITEM_NAME}`,
      resourceId: request.ITEM_ID,
      resourceType: request.ITEM_TYPE,
      meta: { requestId: id },
    });
    return;
  } catch (err) {
    console.error("[revokeAllAccess] error:", err.message);
    return res.status(500).json({ error: err.message });
  }
};

/**
 * GET USERS WITH ACCESS - Get list of users who have access to an item
 * GET /api/access/item-users?itemId=X&itemType=file
 */
export const getItemUsers = async (req, res) => {
  try {
    const { itemId, itemType } = req.query;
    if (!itemId || !itemType) {
      return res.status(400).json({ error: "Missing itemId or itemType" });
    }

    // Get all approved access requests for this item
    const sql = `
      SELECT USER_EMAIL, ACCESS_TYPES, APPROVED_AT, APPROVED_BY
      FROM ACCESS_REQUESTS
      WHERE ITEM_ID='${esc(itemId)}' AND ITEM_TYPE='${esc(itemType)}' AND STATUS='approved'
      ORDER BY APPROVED_AT DESC
    `;
    const { rows } = await execSql(sql);

    // Also include the admin users (they always have access)
    const adminSql = `SELECT EMAIL, NAME FROM USERS WHERE ROLE='admin' LIMIT 20`;
    let admins = [];
    try {
      const { rows: adminRows } = await execSql(adminSql);
      admins = (adminRows || []).map((a) => ({
        USER_EMAIL: a.EMAIL,
        ACCESS_TYPES: "VIEW,DOWNLOAD,UPLOAD,COMMENT",
        isAdmin: true,
      }));
    } catch (e) {
      console.warn("Could not fetch admin users:", e.message);
    }

    // Combine and deduplicate
    const allUsers = [...admins];
    (rows || []).forEach((r) => {
      if (!allUsers.find((u) => u.USER_EMAIL === r.USER_EMAIL)) {
        allUsers.push(r);
      }
    });

    return res.json(allUsers);
  } catch (err) {
    console.error("[getItemUsers] error:", err.message);
    return res.status(500).json({ error: err.message });
  }
};

/**
 * REVOKE SPECIFIC ACCESS TYPE - Admin revokes specific access type
 * PUT /api/access/requests/:id
 * Body: { action: 'revoke', accessType: 'UPLOAD' }
 */
export const revokeSpecificAccess = async (req, res) => {
  try {
    const { id } = req.params;
    const { action, accessType } = req.body;
    const userRole = req.user?.role;
    if (userRole !== "admin") {
      return res.status(403).json({ error: "Admin only" });
    }

    if (action !== "revoke" || !accessType) {
      return res.status(400).json({ error: "Invalid action or accessType" });
    }

    // Fetch current request
    const selectSql = `SELECT * FROM ACCESS_REQUESTS WHERE ID='${esc(id)}'`;
    const { rows } = await execSql(selectSql);
    const request = rows && rows[0];
    if (!request) return res.status(404).json({ error: "Request not found" });

    // Parse current access types, remove the specified one
    const currentTypes = (request.ACCESS_TYPES || "").split(",").map((s) => s.trim()).filter(Boolean);
    const updatedTypes = currentTypes.filter((t) => t !== accessType);

    if (updatedTypes.length === 0) {
      // If no access types left, delete the entire request
      const deleteSql = `DELETE FROM ACCESS_REQUESTS WHERE ID='${esc(id)}'`;
      await execSql(deleteSql);
      console.log("[revokeSpecificAccess] All access types removed, deleted request:", id);
    } else {
      // Update with remaining access types
      const updatedAccessTypesStr = updatedTypes.join(",");
      const updateSql = `
        UPDATE ACCESS_REQUESTS
        SET ACCESS_TYPES='${esc(updatedAccessTypesStr)}'
        WHERE ID='${esc(id)}'
      `;
      await execSql(updateSql);
      console.log("[revokeSpecificAccess] Revoked access type:", accessType, "for request:", id);
    }

    // Notify user of revocation (best-effort)
    try {
      await sendMail(
        request.USER_EMAIL,
        `Access Revoked: ${request.ITEM_NAME}`,
        `<p>Your ${accessType} access to ${request.ITEM_NAME} has been revoked.</p>`
      );
    } catch (e) {
      console.error("Failed to notify user of revocation:", e.message);
    }

    res.json({ message: `${accessType} access revoked successfully` });
    safeLogWithRequest(req, {
      action: "revoke_access_partial",
      description: `Revoked ${accessType} for ${request.USER_EMAIL} on ${request.ITEM_NAME}`,
      resourceId: request.ITEM_ID,
      resourceType: request.ITEM_TYPE,
      meta: { requestId: id, accessType },
    });
    return;
  } catch (err) {
    console.error("[revokeSpecificAccess] error:", err.message);
    return res.status(500).json({ error: err.message });
  }
};
