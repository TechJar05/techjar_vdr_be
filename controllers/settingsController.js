// backend/controllers/settingsController.js
import connection from "../config/snowflake.js";
import bcrypt from "bcryptjs";
import s3 from "../config/s3.js";
import { v4 as uuidv4 } from "uuid";
import { safeLogWithRequest } from "../utils/activityLogger.js";

// Helper: wrap connection.execute in a Promise
const execSql = (sql) =>
  new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      complete: (err, stmt, rows) => {
        if (err) {
          try { err.message = `${err.message} | SQL: ${sql}`; } catch(e) {}
          return reject(err);
        }
        resolve({ stmt, rows });
      },
    });
  });

// Helper: Escape SQL strings
const esc = (str) => (str || "").replace(/'/g, "''");

// Ensure PROFILE table exists
const ensureProfileTable = async () => {
  const createTableSql = `
    CREATE TABLE IF NOT EXISTS PROFILE (
      USER_EMAIL VARCHAR(255) PRIMARY KEY,
      COMPANY_NAME VARCHAR(255),
      FIRST_NAME VARCHAR(255),
      LAST_NAME VARCHAR(255),
      ADDRESS VARCHAR(500),
      CONTACT_NO VARCHAR(50),
      EXPIRY_DATE DATE,
      LOGO_URL VARCHAR(1000),
      AVAILABLE_SPACE_MB NUMBER(38,0) DEFAULT 2048,
      UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `;
  try {
    await execSql(createTableSql);
  } catch (err) {
    console.error("[ensureProfileTable] error:", err.message);
  }
};

// Get user profile
export const getProfile = async (req, res) => {
  const email = req.user?.email;
  if (!email) return res.status(401).json({ message: "Unauthorized" });

  try {
    await ensureProfileTable();

    // Get user basic info
    const userSql = `SELECT NAME, EMAIL, ROLE, CREATED_AT FROM USERS WHERE EMAIL='${esc(email)}' LIMIT 1`;
    const { rows: userRows } = await execSql(userSql);
    if (!userRows || userRows.length === 0) {
      return res.status(404).json({ message: "User not found" });
    }

    // Get profile info
    const profileSql = `SELECT * FROM PROFILE WHERE USER_EMAIL='${esc(email)}' LIMIT 1`;
    const { rows: profileRows } = await execSql(profileSql);

    const user = userRows[0];
    const profile = profileRows?.[0] || {};

    // Parse name into first/last if needed
    const nameParts = (user.NAME || "").split(" ");
    const firstName = profile.FIRST_NAME || nameParts[0] || "";
    const lastName = profile.LAST_NAME || nameParts.slice(1).join(" ") || "";

    res.json({
      email: user.EMAIL,
      companyName: profile.COMPANY_NAME || "",
      firstName: firstName,
      lastName: lastName,
      address: profile.ADDRESS || "",
      contactNo: profile.CONTACT_NO || "",
      expiryDate: profile.EXPIRY_DATE || null,
      logoUrl: profile.LOGO_URL || null,
      availableSpaceMB: profile.AVAILABLE_SPACE_MB || 2048,
      role: user.ROLE,
    });
  } catch (err) {
    console.error("[getProfile] error:", err.message);
    res.status(500).json({ error: err.message });
  }
};

// Update user profile
export const updateProfile = async (req, res) => {
  const email = req.user?.email;
  if (!email) return res.status(401).json({ message: "Unauthorized" });

  const {
    companyName,
    firstName,
    lastName,
    address,
    contactNo,
    expiryDate,
  } = req.body;

  try {
    await ensureProfileTable();

    // Update or insert profile
    const upsertSql = `
      MERGE INTO PROFILE AS target
      USING (
        SELECT 
          '${esc(email)}' AS USER_EMAIL,
          '${esc(companyName || "")}' AS COMPANY_NAME,
          '${esc(firstName || "")}' AS FIRST_NAME,
          '${esc(lastName || "")}' AS LAST_NAME,
          '${esc(address || "")}' AS ADDRESS,
          '${esc(contactNo || "")}' AS CONTACT_NO,
          ${expiryDate ? `'${expiryDate}'` : "NULL"} AS EXPIRY_DATE,
          CURRENT_TIMESTAMP() AS UPDATED_AT
      ) AS source
      ON target.USER_EMAIL = source.USER_EMAIL
      WHEN MATCHED THEN
        UPDATE SET
          COMPANY_NAME = source.COMPANY_NAME,
          FIRST_NAME = source.FIRST_NAME,
          LAST_NAME = source.LAST_NAME,
          ADDRESS = source.ADDRESS,
          CONTACT_NO = source.CONTACT_NO,
          EXPIRY_DATE = source.EXPIRY_DATE,
          UPDATED_AT = source.UPDATED_AT
      WHEN NOT MATCHED THEN
        INSERT (USER_EMAIL, COMPANY_NAME, FIRST_NAME, LAST_NAME, ADDRESS, CONTACT_NO, EXPIRY_DATE, UPDATED_AT)
        VALUES (source.USER_EMAIL, source.COMPANY_NAME, source.FIRST_NAME, source.LAST_NAME, source.ADDRESS, source.CONTACT_NO, source.EXPIRY_DATE, source.UPDATED_AT)
    `;

    await execSql(upsertSql);

    // Also update USERS.NAME to match firstName + lastName
    if (firstName || lastName) {
      const fullName = `${firstName || ""} ${lastName || ""}`.trim();
      if (fullName) {
        const updateNameSql = `UPDATE USERS SET NAME='${esc(fullName)}' WHERE EMAIL='${esc(email)}'`;
        await execSql(updateNameSql);
      }
    }

    res.json({ message: "Profile updated successfully" });
    safeLogWithRequest(req, {
      action: "update_profile",
      description: "Updated profile details",
      resourceId: email,
      resourceType: "user",
    });
  } catch (err) {
    console.error("[updateProfile] error:", err.message);
    res.status(500).json({ error: err.message });
  }
};

// Upload logo
export const uploadLogo = async (req, res) => {
  const email = req.user?.email;
  if (!email) return res.status(401).json({ message: "Unauthorized" });

  const file = req.file;
  if (!file) return res.status(400).json({ message: "No file uploaded" });

  try {
    await ensureProfileTable();

    const params = {
      Bucket: process.env.AWS_BUCKET_NAME,
      Key: `logos/${email}/${Date.now()}_${file.originalname}`,
      Body: file.buffer,
      ContentType: file.mimetype,
    };

    s3.upload(params, async (err, data) => {
      if (err) return res.status(500).json({ error: err.message });

      // Update profile with logo URL
      const updateSql = `
        MERGE INTO PROFILE AS target
        USING (
          SELECT '${esc(email)}' AS USER_EMAIL, '${data.Location}' AS LOGO_URL, CURRENT_TIMESTAMP() AS UPDATED_AT
        ) AS source
        ON target.USER_EMAIL = source.USER_EMAIL
        WHEN MATCHED THEN UPDATE SET LOGO_URL = source.LOGO_URL, UPDATED_AT = source.UPDATED_AT
        WHEN NOT MATCHED THEN INSERT (USER_EMAIL, LOGO_URL, UPDATED_AT) VALUES (source.USER_EMAIL, source.LOGO_URL, source.UPDATED_AT)
      `;

      try {
        await execSql(updateSql);
        res.json({ message: "Logo uploaded successfully", logoUrl: data.Location });
        safeLogWithRequest(req, {
          action: "upload_logo",
          description: "Updated profile logo",
          resourceId: email,
          resourceType: "user",
        });
      } catch (updateErr) {
        console.error("[uploadLogo] update error:", updateErr.message);
        res.status(500).json({ error: updateErr.message });
      }
    });
  } catch (err) {
    console.error("[uploadLogo] error:", err.message);
    res.status(500).json({ error: err.message });
  }
};

// Reset password
export const resetPassword = async (req, res) => {
  const email = req.user?.email;
  if (!email) return res.status(401).json({ message: "Unauthorized" });

  const { currentPassword, newPassword, confirmPassword } = req.body;

  if (!currentPassword || !newPassword || !confirmPassword) {
    return res.status(400).json({ message: "All password fields are required" });
  }

  if (newPassword !== confirmPassword) {
    return res.status(400).json({ message: "New password and confirm password do not match" });
  }

  if (newPassword.length < 6) {
    return res.status(400).json({ message: "New password must be at least 6 characters" });
  }

  try {
    // Get current user password
    const userSql = `SELECT PASSWORD FROM USERS WHERE EMAIL='${esc(email)}' LIMIT 1`;
    const { rows: userRows } = await execSql(userSql);
    if (!userRows || userRows.length === 0) {
      return res.status(404).json({ message: "User not found" });
    }

    // Verify current password
    const isPasswordValid = bcrypt.compareSync(currentPassword, userRows[0].PASSWORD);
    if (!isPasswordValid) {
      return res.status(400).json({ message: "Current password is incorrect" });
    }

    // Hash new password and update
    const hashedPassword = bcrypt.hashSync(newPassword, 10);
    const updateSql = `UPDATE USERS SET PASSWORD='${hashedPassword}' WHERE EMAIL='${esc(email)}'`;
    await execSql(updateSql);

    res.json({ message: "Password updated successfully" });
    safeLogWithRequest(req, {
      action: "reset_password",
      description: "Password updated",
      resourceId: email,
      resourceType: "user",
    });
  } catch (err) {
    console.error("[resetPassword] error:", err.message);
    res.status(500).json({ error: err.message });
  }
};

// Change email
export const changeEmail = async (req, res) => {
  const email = req.user?.email;
  if (!email) return res.status(401).json({ message: "Unauthorized" });

  const { newEmail } = req.body;
  if (!newEmail) return res.status(400).json({ message: "New email is required" });

  try {
    // Check if new email already exists
    const checkSql = `SELECT EMAIL FROM USERS WHERE EMAIL='${esc(newEmail)}' LIMIT 1`;
    const { rows: existingRows } = await execSql(checkSql);
    if (existingRows && existingRows.length > 0) {
      return res.status(400).json({ message: "Email already in use" });
    }

    // Update email in USERS table
    const updateUserSql = `UPDATE USERS SET EMAIL='${esc(newEmail)}' WHERE EMAIL='${esc(email)}'`;
    await execSql(updateUserSql);

    // Update email in PROFILE table if exists
    await ensureProfileTable();
    const updateProfileSql = `
      UPDATE PROFILE SET USER_EMAIL='${esc(newEmail)}' WHERE USER_EMAIL='${esc(email)}'
    `;
    await execSql(updateProfileSql);

    res.json({ message: "Email updated successfully", newEmail });
    safeLogWithRequest(req, {
      action: "change_email",
      description: `Changed email to ${newEmail}`,
      resourceId: newEmail,
      resourceType: "user",
    });
  } catch (err) {
    console.error("[changeEmail] error:", err.message);
    res.status(500).json({ error: err.message });
  }
};

// Ensure TAGS table exists
const ensureTagsTable = async () => {
  const createTableSql = `
    CREATE TABLE IF NOT EXISTS TAGS (
      ID VARCHAR(255) PRIMARY KEY,
      NAME VARCHAR(255) NOT NULL UNIQUE,
      COLOR VARCHAR(50) DEFAULT '#10b981',
      CREATED_BY VARCHAR(255),
      CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
    )
  `;
  try {
    await execSql(createTableSql);
  } catch (err) {
    console.error("[ensureTagsTable] error:", err.message);
  }
};

// Get all tags
export const getTags = async (req, res) => {
  try {
    await ensureTagsTable();
    const sql = `SELECT ID, NAME, COLOR, CREATED_BY, CREATED_AT FROM TAGS ORDER BY CREATED_AT DESC`;
    const { rows } = await execSql(sql);
    res.json(rows || []);
  } catch (err) {
    console.error("[getTags] error:", err.message);
    res.status(500).json({ error: err.message });
  }
};

// Create tag
export const createTag = async (req, res) => {
  const email = req.user?.email;
  if (!email) return res.status(401).json({ message: "Unauthorized" });

  const { name, color } = req.body;
  if (!name || !name.trim()) {
    return res.status(400).json({ message: "Tag name is required" });
  }

    try {
      await ensureTagsTable();

      const id = uuidv4();
    const tagName = name.trim();
    const tagColor = color || "#10b981";

    const insertSql = `
      INSERT INTO TAGS (ID, NAME, COLOR, CREATED_BY, CREATED_AT)
      VALUES ('${id}', '${esc(tagName)}', '${esc(tagColor)}', '${esc(email)}', CURRENT_TIMESTAMP())
    `;

    await execSql(insertSql);
    res.status(201).json({ message: "Tag created successfully", id, name: tagName, color: tagColor });
    safeLogWithRequest(req, {
      action: "create_tag",
      description: `Created tag "${tagName}"`,
      resourceId: id,
      resourceType: "tag",
    });
  } catch (err) {
    console.error("[createTag] error:", err.message);
    // Check if it's a duplicate name error
    if (err.message.includes("already exists") || err.message.includes("duplicate")) {
      return res.status(400).json({ message: "Tag name already exists" });
    }
    res.status(500).json({ error: err.message });
  }
};

// Update tag
export const updateTag = async (req, res) => {
  const { id } = req.params;
  const { name, color } = req.body;

  if (!name || !name.trim()) {
    return res.status(400).json({ message: "Tag name is required" });
  }

  try {
    await ensureTagsTable();

    const updateSql = `
      UPDATE TAGS 
      SET NAME='${esc(name.trim())}', COLOR='${esc(color || "#10b981")}'
      WHERE ID='${esc(id)}'
    `;

    await execSql(updateSql);
    res.json({ message: "Tag updated successfully" });
    safeLogWithRequest(req, {
      action: "update_tag",
      description: `Updated tag ${id}`,
      resourceId: id,
      resourceType: "tag",
    });
  } catch (err) {
    console.error("[updateTag] error:", err.message);
    res.status(500).json({ error: err.message });
  }
};

// Delete tag
export const deleteTag = async (req, res) => {
  const { id } = req.params;

  try {
    await ensureTagsTable();

    const deleteSql = `DELETE FROM TAGS WHERE ID='${esc(id)}'`;
    await execSql(deleteSql);
    res.json({ message: "Tag deleted successfully" });
    safeLogWithRequest(req, {
      action: "delete_tag",
      description: `Deleted tag ${id}`,
      resourceId: id,
      resourceType: "tag",
    });
  } catch (err) {
    console.error("[deleteTag] error:", err.message);
    res.status(500).json({ error: err.message });
  }
};

