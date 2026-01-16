import connection from "../config/snowflake.js";
import { v4 as uuidv4 } from "uuid";
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
 * GET USER STORAGE - Get total uploaded files size across all folders
 * This now shows the total size of all files uploaded to the system (not virtual storage)
 */
export const getUserStorage = async (req, res) => {
  try {
    const userEmail = req.user?.email;
    if (!userEmail) return res.status(400).json({ error: "User email missing" });

    // Default quota (5GB = 5000MB)
    const totalQuotaMb = 5000;

    // Calculate total size of all uploaded files from FILES table
    let usedBytes = 0;
    try {
      const sumSql = `SELECT COALESCE(SUM(FILE_SIZE), 0) AS TOTAL_SIZE FROM FILES`;
      const { rows: sumRows } = await execSql(sumSql);
      if (sumRows && sumRows.length > 0) {
        usedBytes = Number(sumRows[0].TOTAL_SIZE || 0);
      }
    } catch (sumErr) {
      console.warn('[getUserStorage] Could not sum FILES:', sumErr.message);
      usedBytes = 0;
    }

    // Convert bytes to MB
    const usedMb = usedBytes / (1024 * 1024);
    const percentUsed = totalQuotaMb > 0 ? ((usedMb / totalQuotaMb) * 100) : 0;

    return res.json({
      userEmail,
      totalQuotaMb: totalQuotaMb,
      usedMb: parseFloat(usedMb.toFixed(2)),
      percentUsed: parseFloat(percentUsed.toFixed(2)),
    });
  } catch (err) {
    console.error("[getUserStorage] error:", err.message);
    return res.status(500).json({ error: err.message });
  }
};

/**
 * ADD FILE TO STORAGE - When user "downloads" (adds to virtual storage)
 * Body: { itemId, itemName, fileSizeMb, itemType: 'file'|'folder' }
 * Allows duplicates - same file/folder can be added multiple times (e.g., once standalone, once as part of folder)
 */
export const addToStorage = async (req, res) => {
  try {
    const { itemId, itemName, fileSizeMb, itemType } = req.body;
    const userEmail = req.user?.email;
    if (!userEmail) return res.status(400).json({ error: "User email missing" });
    if (!itemId || !itemName || !fileSizeMb) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    // Create STORAGE_FILES table if missing (with ITEM_TYPE, STORAGE_REF and PARENT_REF columns)
    const createTableSql = `CREATE TABLE IF NOT EXISTS STORAGE_FILES (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      USER_EMAIL STRING,
      ITEM_ID STRING,
      ITEM_NAME STRING,
      ITEM_TYPE STRING DEFAULT 'file',
      FILE_SIZE_MB NUMBER,
      PARENT_ITEM_ID STRING,
      STORAGE_REF STRING,
      PARENT_REF STRING,
      ADDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
    )`;
    try {
      await execSql(createTableSql);
    } catch (createErr) {
      console.warn("[addToStorage] CREATE TABLE warning:", createErr.message);
    }

    // Try to add missing columns if they don't exist
    try {
      const alterSql1 = `ALTER TABLE STORAGE_FILES ADD COLUMN ITEM_TYPE STRING DEFAULT 'file'`;
      await execSql(alterSql1);
    } catch (alterErr) {
      // Column might already exist
    }

    try {
      const alterSql2 = `ALTER TABLE STORAGE_FILES ADD COLUMN PARENT_ITEM_ID STRING`;
      await execSql(alterSql2);
    } catch (alterErr) {
      // Column might already exist
    }

    // Check quota - NOTE: Removed duplicate check to allow duplication
    const storageSql = `SELECT * FROM USER_STORAGE WHERE USER_EMAIL='${esc(userEmail)}' LIMIT 1`;
    const { rows: storageRows } = await execSql(storageSql);
    if (storageRows && storageRows.length > 0) {
      const storage = storageRows[0];
      if (storage.USED_MB + fileSizeMb > storage.TOTAL_QUOTA_MB) {
        return res.status(402).json({
          error: "Storage quota exceeded",
          needed: fileSizeMb,
          available: storage.TOTAL_QUOTA_MB - storage.USED_MB,
        });
      }
    }

    // Add file/folder to storage (allow duplicates)
    const itemTypeStr = itemType || 'file';
    const storageRef = uuidv4();
    let insertFileSql = `
      INSERT INTO STORAGE_FILES (USER_EMAIL, ITEM_ID, ITEM_NAME, ITEM_TYPE, FILE_SIZE_MB, STORAGE_REF)
      SELECT '${esc(userEmail)}', '${esc(itemId)}', '${esc(itemName)}', '${esc(itemTypeStr)}', ${fileSizeMb}, '${esc(storageRef)}'
    `;

    try {
      await execSql(insertFileSql);
    } catch (insertErr) {
      if (insertErr.message && (insertErr.message.includes("ITEM_TYPE") || insertErr.message.includes("PARENT_ITEM_ID") || insertErr.message.includes("STORAGE_REF"))) {
        // Columns don't exist yet, try without optional columns
        insertFileSql = `
          INSERT INTO STORAGE_FILES (USER_EMAIL, ITEM_ID, ITEM_NAME, FILE_SIZE_MB, STORAGE_REF)
          SELECT '${esc(userEmail)}', '${esc(itemId)}', '${esc(itemName)}', ${fileSizeMb}, '${esc(storageRef)}'
        `;
        await execSql(insertFileSql);
      } else {
        throw insertErr;
      }
    }

    // Increment DOWNLOADS_COUNT for the file (only for files, not folders)
    if (itemTypeStr === 'file') {
      try {
        // Ensure DOWNLOADS_COUNT column exists (use supported syntax)
        await execSql("ALTER TABLE FILES ADD COLUMN IF NOT EXISTS DOWNLOADS_COUNT NUMBER DEFAULT 0");

        // Read current value then update explicitly to avoid ambiguous column errors
        const selSql = `SELECT DOWNLOADS_COUNT FROM FILES WHERE ID='${esc(itemId)}' LIMIT 1`;
        let current = 0;
        try {
          const { rows: curRows } = await execSql(selSql);
          if (curRows && curRows.length > 0) current = Number(curRows[0].DOWNLOADS_COUNT || 0);
        } catch (selErr) {
          console.warn('[addToStorage] Could not read current DOWNLOADS_COUNT for', itemId, selErr.message);
        }

        const newVal = current + 1;
        const updateSql = `UPDATE FILES SET DOWNLOADS_COUNT = ${newVal} WHERE ID='${esc(itemId)}'`;
        try {
          await execSql(updateSql);
          console.log(`[addToStorage] Incremented DOWNLOADS_COUNT for file: ${itemId} -> ${newVal}`);
        } catch (updateErr) {
          // If Snowflake complains about ambiguous column names, try a correlated subquery fallback
          console.warn('[addToStorage] Initial DOWNLOADS_COUNT update failed:', updateErr && updateErr.message ? updateErr.message : updateErr);
          if (updateErr && typeof updateErr.message === 'string' && updateErr.message.toLowerCase().includes('ambiguous')) {
            const fallbackSql = `UPDATE FILES SET DOWNLOADS_COUNT = (SELECT COALESCE(F2.DOWNLOADS_COUNT,0) + 1 FROM FILES F2 WHERE F2.ID='${esc(itemId)}') WHERE ID='${esc(itemId)}'`;
            try {
              await execSql(fallbackSql);
              console.log(`[addToStorage] Fallback increment succeeded for file: ${itemId}`);
            } catch (fbErr) {
              console.warn('[addToStorage] Fallback update also failed:', fbErr && fbErr.message ? fbErr.message : fbErr, 'FallbackSQL:', fallbackSql);
            }
          } else {
            console.warn('[addToStorage] Could not update DOWNLOADS_COUNT:', updateErr && updateErr.message ? updateErr.message : updateErr);
          }
        }
      } catch (downloadErr) {
        console.warn("[addToStorage] Could not update DOWNLOADS_COUNT:", downloadErr.message);
      }
    }

    // Update storage usage
    const updateSql = `
      UPDATE USER_STORAGE
      SET USED_MB = USED_MB + ${fileSizeMb}, UPDATED_AT = CURRENT_TIMESTAMP()
      WHERE USER_EMAIL='${esc(userEmail)}'
    `;
    await execSql(updateSql);

    safeLogWithRequest(req, {
      action: "add_to_storage",
      description: `Added ${itemTypeStr} to storage`,
      resourceId: itemId,
      resourceType: itemTypeStr,
      meta: { sizeMb: fileSizeMb },
    });

    return res.status(201).json({ message: "Item added to storage" });
  } catch (err) {
    console.error("[addToStorage] error:", err.message);
    return res.status(500).json({ error: err.message });
  }
};

/**
 * LIST STORAGE FILES - Get user's stored files
 */
export const listStorageFiles = async (req, res) => {
  try {
    const userEmail = req.user?.email;
    if (!userEmail) return res.status(400).json({ error: "User email missing" });

    const sql = `SELECT * FROM STORAGE_FILES WHERE USER_EMAIL='${esc(userEmail)}' ORDER BY ADDED_AT DESC`;
    const { rows } = await execSql(sql);
    return res.json(rows || []);
  } catch (err) {
    console.error("[listStorageFiles] error:", err.message);
    return res.status(500).json({ error: err.message });
  }
};

/**
 * REMOVE FILE FROM STORAGE
 */
export const removeFromStorage = async (req, res) => {
  try {
    const { storageRef } = req.params;
    const userEmail = req.user?.email;
    if (!userEmail) return res.status(400).json({ error: "User email missing" });

    // Get the storage entry by its unique STORAGE_REF
    const selectSql = `SELECT FILE_SIZE_MB, ITEM_TYPE, STORAGE_REF, ITEM_ID FROM STORAGE_FILES WHERE USER_EMAIL='${esc(userEmail)}' AND STORAGE_REF='${esc(storageRef)}' LIMIT 1`;
    let { rows } = await execSql(selectSql);
    // Backwards compatibility: if not found by STORAGE_REF, allow lookup by ITEM_ID (older entries)
    if (!rows || rows.length === 0) {
      const fallbackSql = `SELECT FILE_SIZE_MB, ITEM_TYPE, STORAGE_REF, ITEM_ID FROM STORAGE_FILES WHERE USER_EMAIL='${esc(userEmail)}' AND ITEM_ID='${esc(storageRef)}' LIMIT 1`;
      const fallback = await execSql(fallbackSql);
      rows = fallback.rows;
      if (!rows || rows.length === 0) {
        return res.status(404).json({ error: "Item not in storage" });
      }
    }

    const item = rows[0];
    const isFolder = item.ITEM_TYPE === 'folder';

    // Compute total size to free (parent + children if folder)
    let totalToFree = Number(item.FILE_SIZE_MB || 0);
    if (isFolder) {
      const sumSql = `SELECT SUM(FILE_SIZE_MB) AS TOTAL FROM STORAGE_FILES WHERE USER_EMAIL='${esc(userEmail)}' AND (STORAGE_REF='${esc(storageRef)}' OR PARENT_REF='${esc(storageRef)}')`;
      const { rows: sumRows } = await execSql(sumSql);
      if (sumRows && sumRows.length > 0) {
        totalToFree = Number(sumRows[0].TOTAL || 0);
      }
      // Delete parent + children
      const deleteSql = `DELETE FROM STORAGE_FILES WHERE USER_EMAIL='${esc(userEmail)}' AND (STORAGE_REF='${esc(storageRef)}' OR PARENT_REF='${esc(storageRef)}')`;
      await execSql(deleteSql);
    } else {
      // File entry: delete only this storage entry
      const deleteSql = `DELETE FROM STORAGE_FILES WHERE USER_EMAIL='${esc(userEmail)}' AND STORAGE_REF='${esc(storageRef)}'`;
      await execSql(deleteSql);
    }

    // Update storage usage
    const updateSql = `
      UPDATE USER_STORAGE
      SET USED_MB = USED_MB - ${totalToFree}, UPDATED_AT = CURRENT_TIMESTAMP()
      WHERE USER_EMAIL='${esc(userEmail)}'
    `;
    await execSql(updateSql);

    safeLogWithRequest(req, {
      action: "remove_from_storage",
      description: `Removed ${isFolder ? "folder" : "file"} from storage`,
      resourceId: storageRef,
      resourceType: isFolder ? "folder" : "file",
      meta: { freedMb: totalToFree },
    });

    return res.json({ message: isFolder ? "Folder removed from storage" : "File removed from storage" });
  } catch (err) {
    console.error("[removeFromStorage] error:", err.message);
    return res.status(500).json({ error: err.message });
  }
};

/**
 * ADD FOLDER WITH CONTENTS TO STORAGE - Downloads entire folder structure
 * Body: { folderId, folderName, folderSizeMb }
 * This recursively copies all files from the folder into the user's storage
 */
export const addFolderToStorage = async (req, res) => {
  try {
    const { folderId, folderName, folderSizeMb } = req.body;
    const userEmail = req.user?.email;
    if (!userEmail) return res.status(400).json({ error: "User email missing" });
    if (!folderId || !folderName || !folderSizeMb) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    // Ensure STORAGE_FILES table exists with required columns
    const createTableSql = `CREATE TABLE IF NOT EXISTS STORAGE_FILES (
      ID NUMBER AUTOINCREMENT PRIMARY KEY,
      USER_EMAIL STRING,
      ITEM_ID STRING,
      ITEM_NAME STRING,
      ITEM_TYPE STRING DEFAULT 'file',
      FILE_SIZE_MB NUMBER,
      PARENT_ITEM_ID STRING,
      ADDED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
    )`;
    try {
      await execSql(createTableSql);
    } catch (createErr) {
      console.warn("[addFolderToStorage] CREATE TABLE warning:", createErr.message);
    }

    // Try to add PARENT_ITEM_ID, STORAGE_REF and PARENT_REF columns if they don't exist
    try {
      const alterSql1 = `ALTER TABLE STORAGE_FILES ADD COLUMN PARENT_ITEM_ID STRING`;
      await execSql(alterSql1);
    } catch (alterErr) {
      // ignore
    }
    try {
      const alterSql2 = `ALTER TABLE STORAGE_FILES ADD COLUMN STORAGE_REF STRING`;
      await execSql(alterSql2);
    } catch (alterErr) {
      // ignore
    }
    try {
      const alterSql3 = `ALTER TABLE STORAGE_FILES ADD COLUMN PARENT_REF STRING`;
      await execSql(alterSql3);
    } catch (alterErr) {
      // ignore
    }

    // Check quota
    const storageSql = `SELECT * FROM USER_STORAGE WHERE USER_EMAIL='${esc(userEmail)}' LIMIT 1`;
    const { rows: storageRows } = await execSql(storageSql);
    if (storageRows && storageRows.length > 0) {
      const storage = storageRows[0];
      if (storage.USED_MB + folderSizeMb > storage.TOTAL_QUOTA_MB) {
        return res.status(402).json({
          error: "Storage quota exceeded",
          needed: folderSizeMb,
          available: storage.TOTAL_QUOTA_MB - storage.USED_MB,
        });
      }
    }

    // Get all files in this folder
    const filesSql = `SELECT * FROM FILES WHERE FOLDER_ID='${esc(folderId)}'`;
    const { rows: files } = await execSql(filesSql);

    // Generate a storage reference to group folder + its children uniquely
    const folderStorageRef = uuidv4();

    if (!files || files.length === 0) {
      // Empty folder - just add the folder entry
      const insertFolderSql = `
        INSERT INTO STORAGE_FILES (USER_EMAIL, ITEM_ID, ITEM_NAME, ITEM_TYPE, FILE_SIZE_MB, STORAGE_REF)
        SELECT '${esc(userEmail)}', '${esc(folderId)}', '${esc(folderName)}', 'folder', ${folderSizeMb}, '${esc(folderStorageRef)}'
      `;
      try {
        await execSql(insertFolderSql);
      } catch (insertErr) {
        if (insertErr.message && insertErr.message.includes("ITEM_TYPE")) {
          const fallbackSql = `
            INSERT INTO STORAGE_FILES (USER_EMAIL, ITEM_ID, ITEM_NAME, FILE_SIZE_MB, STORAGE_REF)
            SELECT '${esc(userEmail)}', '${esc(folderId)}', '${esc(folderName)}', ${folderSizeMb}, '${esc(folderStorageRef)}'
          `;
          await execSql(fallbackSql);
        } else {
          throw insertErr;
        }
      }
    } else {
      // Add folder entry first with unique STORAGE_REF
      const insertFolderSql = `
        INSERT INTO STORAGE_FILES (USER_EMAIL, ITEM_ID, ITEM_NAME, ITEM_TYPE, FILE_SIZE_MB, STORAGE_REF)
        SELECT '${esc(userEmail)}', '${esc(folderId)}', '${esc(folderName)}', 'folder', ${folderSizeMb}, '${esc(folderStorageRef)}'
      `;
      try {
        await execSql(insertFolderSql);
      } catch (folderErr) {
        console.warn("[addFolderToStorage] Could not insert folder entry:", folderErr.message);
      }

      // Add each file in the folder with PARENT_REF set to folderStorageRef
      for (const file of files) {
        const fileId = file.ID || file.id;
        const fileName = file.FILE_NAME || file.fileName || "Untitled";
        const fileSize = Number(file.FILE_SIZE || file.fileSize || 0);
        const fileSizeMb = Math.max(1, Math.ceil(fileSize / (1024 * 1024)));

        const insertFileSql = `
          INSERT INTO STORAGE_FILES (USER_EMAIL, ITEM_ID, ITEM_NAME, ITEM_TYPE, FILE_SIZE_MB, PARENT_REF, STORAGE_REF)
          SELECT '${esc(userEmail)}', '${esc(fileId)}', '${esc(fileName)}', 'file', ${fileSizeMb}, '${esc(folderStorageRef)}', '${esc(uuidv4())}'
        `;

        try {
          await execSql(insertFileSql);
          // Increment DOWNLOADS_COUNT for this file since user effectively downloaded the file into storage
          try {
            await execSql("ALTER TABLE FILES ADD COLUMN IF NOT EXISTS DOWNLOADS_COUNT NUMBER DEFAULT 0");
            // Read current value
            const selSql = `SELECT DOWNLOADS_COUNT FROM FILES WHERE ID='${esc(fileId)}' LIMIT 1`;
            let current = 0;
            try {
              const { rows: curRows } = await execSql(selSql);
              if (curRows && curRows.length > 0) current = Number(curRows[0].DOWNLOADS_COUNT || 0);
            } catch (selErr) {
              console.warn('[addFolderToStorage] Could not read DOWNLOADS_COUNT for', fileId, selErr.message);
            }
            const newVal = current + 1;
            const updateSql = `UPDATE FILES SET DOWNLOADS_COUNT = ${newVal} WHERE ID='${esc(fileId)}'`;
            try {
              await execSql(updateSql);
              console.log(`[addFolderToStorage] Incremented DOWNLOADS_COUNT for file: ${fileId} -> ${newVal}`);
            } catch (updateErr) {
              console.warn('[addFolderToStorage] Initial DOWNLOADS_COUNT update failed:', updateErr && updateErr.message ? updateErr.message : updateErr);
              if (updateErr && typeof updateErr.message === 'string' && updateErr.message.toLowerCase().includes('ambiguous')) {
                const fallbackSql = `UPDATE FILES SET DOWNLOADS_COUNT = (SELECT COALESCE(F2.DOWNLOADS_COUNT,0) + 1 FROM FILES F2 WHERE F2.ID='${esc(fileId)}') WHERE ID='${esc(fileId)}'`;
                try {
                  await execSql(fallbackSql);
                  console.log(`[addFolderToStorage] Fallback increment succeeded for file: ${fileId}`);
                } catch (fbErr) {
                  console.warn('[addFolderToStorage] Fallback update also failed:', fbErr && fbErr.message ? fbErr.message : fbErr, 'FallbackSQL:', fallbackSql);
                }
              } else {
                console.warn('[addFolderToStorage] Could not increment DOWNLOADS_COUNT for file', fileId, updateErr && updateErr.message ? updateErr.message : updateErr);
              }
            }
          } catch (incErr) {
            console.warn('[addFolderToStorage] Could not increment DOWNLOADS_COUNT for file', fileId, incErr.message);
          }
        } catch (fileInsertErr) {
          console.warn("[addFolderToStorage] Could not insert file:", fileId, fileInsertErr.message);
        }
      }
    }

    // Update storage usage
    const updateSql = `
      UPDATE USER_STORAGE
      SET USED_MB = USED_MB + ${folderSizeMb}, UPDATED_AT = CURRENT_TIMESTAMP()
      WHERE USER_EMAIL='${esc(userEmail)}'
    `;
    await execSql(updateSql);

    safeLogWithRequest(req, {
      action: "add_folder_to_storage",
      description: `Folder ${folderName} added to storage`,
      resourceId: folderId,
      resourceType: "folder",
      meta: { sizeMb: folderSizeMb },
    });

    return res.status(201).json({ message: "Folder added to storage with all contents" });
  } catch (err) {
    console.error("[addFolderToStorage] error:", err.message);
    return res.status(500).json({ error: err.message });
  }
};
