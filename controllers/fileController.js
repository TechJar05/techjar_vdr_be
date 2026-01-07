// controllers/fileController.js
import s3 from "../config/s3.js";
import connection from "../config/snowflake.js";
import { v4 as uuidv4 } from "uuid";
import ensureTrashTable from "../utils/ensureTrashTable.js";
import { safeLogWithRequest } from "../utils/activityLogger.js";

let fileMetaColumnsEnsured = false;

// Helper: wrap connection.execute in a Promise for single-statement execution
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

const ensureFileMetaColumns = () =>
  new Promise((resolve, reject) => {
    if (fileMetaColumnsEnsured) return resolve();

    const statements = [
      "ALTER TABLE IF EXISTS FILES ADD COLUMN IF NOT EXISTS FILE_SIZE NUMBER(38,0)",
      "ALTER TABLE IF EXISTS FILES ADD COLUMN IF NOT EXISTS FILE_TYPE STRING",
    ];

    const runStatement = (index = 0) => {
      if (index >= statements.length) {
        fileMetaColumnsEnsured = true;
        return resolve();
      }

      connection.execute({
        sqlText: statements[index],
        complete: (err) => {
          if (err) return reject(err);
          runStatement(index + 1);
        },
      });
    };

    runStatement();
  });

const runQuery = (sqlText, binds = []) =>
  new Promise((resolve, reject) => {
    connection.execute({
      sqlText,
      binds,
      complete: (err, stmt, rows) => {
        if (err) return reject(err);
        resolve({ stmt, rows });
      },
    });
  });

/* ------------------ Folder Logic ------------------ */

// Create a new folder
export const createFolder = (req, res) => {
  const { name } = req.body;
  const createdBy = req.user.email;
  const id = uuidv4();

  const sql = `INSERT INTO FOLDERS (ID, NAME, CREATED_BY, CREATED_AT)
               VALUES ('${id}', '${name}', '${createdBy}', CURRENT_TIMESTAMP())`;

  connection.execute({
    sqlText: sql,
    complete: (err) => {
      if (err) return res.status(500).json({ error: err.message });
      // Return full folder info to frontend
      res.status(201).json({ id, name, createdBy, createdAt: new Date() });
      safeLogWithRequest(req, {
        action: "create_folder",
        description: `Folder "${name}" created`,
        resourceId: id,
        resourceType: "folder",
      });
    },
  });
};

// List all folders
export const listFolders = async (req, res) => {
  try {
    await ensureFileMetaColumns();
  } catch (error) {
    console.error("Error ensuring FILES metadata columns:", error);
    return res.status(500).json({ error: error.message });
  }

  const sql = `
    SELECT 
      F.*,
      COALESCE(FS.FILE_COUNT, 0) AS FILE_COUNT,
      COALESCE(FS.TOTAL_SIZE, 0) AS TOTAL_SIZE
    FROM FOLDERS F
    LEFT JOIN (
      SELECT 
        FOLDER_ID,
        COUNT(*) AS FILE_COUNT,
        SUM(COALESCE(FILE_SIZE, 0)) AS TOTAL_SIZE
      FROM FILES
      GROUP BY FOLDER_ID
    ) FS
    ON F.ID = FS.FOLDER_ID
    ORDER BY F.CREATED_AT DESC
  `;
  connection.execute({
    sqlText: sql,
    complete: (err, stmt, rows) =>
      err ? res.status(500).json({ error: err.message }) : res.json(rows || []),
  });
};

// Open folder (get files inside)
export const openFolder = (req, res) => {
  const { id } = req.params;
  const sql = `SELECT * FROM FILES WHERE FOLDER_ID='${id}' ORDER BY UPLOADED_AT DESC`;
  connection.execute({
    sqlText: sql,
    complete: (err, stmt, rows) =>
      err ? res.status(500).json({ error: err.message }) : res.json(rows || []),
  });
};

// Rename folder
export const renameFolder = (req, res) => {
  const { id } = req.params;
  const { name } = req.body;
  const userEmail = req.user?.email;

  if (!name || !name.trim()) {
    return res.status(400).json({ error: "Folder name is required" });
  }

  // Check if user is the creator (only creator can rename)
  const checkSql = `SELECT CREATED_BY FROM FOLDERS WHERE ID='${id}'`;
  connection.execute({
    sqlText: checkSql,
    complete: (err, stmt, rows) => {
      if (err) return res.status(500).json({ error: err.message });
      if (!rows || rows.length === 0) {
        return res.status(404).json({ error: "Folder not found" });
      }
      if (rows[0].CREATED_BY !== userEmail) {
        return res.status(403).json({ error: "Only folder creator can rename" });
      }

      // Update folder name
      const updateSql = `UPDATE FOLDERS SET NAME='${name.trim()}' WHERE ID='${id}'`;
      connection.execute({
        sqlText: updateSql,
        complete: (err) => {
          if (err) return res.status(500).json({ error: err.message });
          res.json({ message: "Folder renamed successfully", id, name: name.trim() });
          safeLogWithRequest(req, {
            action: "rename_folder",
            description: `Folder renamed to "${name.trim()}"`,
            resourceId: id,
            resourceType: "folder",
          });
        },
      });
    },
  });
};
/* ------------------ File Upload / List / Delete ------------------ */

// Upload file to folder
export const uploadFile = async (req, res) => {
  try {
    await ensureFileMetaColumns();
  } catch (error) {
    console.error("Error ensuring FILES metadata columns before upload:", error);
    return res.status(500).json({ error: error.message });
  }

  const { folderId } = req.params;
  const uploadedBy = req.user.email;
  const file = req.file;
  const id = uuidv4();

  if (!file) return res.status(400).json({ message: "No file uploaded" });

  const fileSize = file.size || 0;
  const fileType = file.mimetype || "application/octet-stream";

  const params = {
    Bucket: process.env.AWS_BUCKET_NAME,
    Key: `${folderId}/${file.originalname}`,
    Body: file.buffer,
  };

  s3.upload(params, (err, data) => {
    if (err) return res.status(500).json({ error: err.message });

    const sql = `INSERT INTO FILES 
      (ID, FOLDER_ID, FILE_NAME, FILE_URL, FILE_SIZE, FILE_TYPE, UPLOADED_BY, UPLOADED_AT)
      VALUES ('${id}', '${folderId}', '${file.originalname}', '${data.Location}', ${fileSize}, '${fileType}', '${uploadedBy}', CURRENT_TIMESTAMP())`;

    connection.execute({
      sqlText: sql,
      complete: (err) =>
        err
          ? res.status(500).json({ error: err.message })
          : res.status(201).json({
              id,
              fileName: file.originalname,
              url: data.Location,
              size: fileSize,
              type: fileType,
            }),
    });
    safeLogWithRequest(req, {
      action: "upload_file",
      description: `Uploaded "${file.originalname}" to folder ${folderId}`,
      resourceId: id,
      resourceType: "file",
      meta: { folderId, size: fileSize, type: fileType },
    });
  });
};

// List files with pagination
export const listFiles = (req, res) => {
  const { folderId } = req.params;
  const { page = 1, limit = 10 } = req.query;
  const offset = (page - 1) * limit;

  const sql = `SELECT * FROM FILES WHERE FOLDER_ID='${folderId}' 
               ORDER BY UPLOADED_AT DESC 
               LIMIT ${limit} OFFSET ${offset}`;

  connection.execute({
    sqlText: sql,
    complete: (err, stmt, rows) => {
      if (err) return res.status(500).json({ error: err.message });
      // ✅ Always return a plain array
      res.json(rows || []);
    },
  });
};


// Delete file (move to trash)
export const deleteFile = async (req, res) => {
  const { id } = req.params;
  const deletedBy = req.user?.email || "system";

  try {
    await ensureFileMetaColumns();
    await ensureTrashTable();

    const insertSql = `
      INSERT INTO TRASH (
        ID,
        ITEM_TYPE,
        ITEM_NAME,
        DELETED_BY,
        DELETED_AT,
        RESTORED,
        FOLDER_ID,
        FILE_URL,
        FILE_SIZE,
        FILE_TYPE,
        UPLOADED_BY,
        UPLOADED_AT,
        CREATED_BY,
        CREATED_AT
      )
      SELECT
        ID,
        'file',
        FILE_NAME,
        ?,
        CURRENT_TIMESTAMP(),
        FALSE,
        FOLDER_ID,
        FILE_URL,
        COALESCE(FILE_SIZE, 0),
        FILE_TYPE,
        UPLOADED_BY,
        UPLOADED_AT,
        UPLOADED_BY,
        UPLOADED_AT
      FROM FILES
      WHERE ID = ?
    `;

    const { stmt } = await runQuery(insertSql, [deletedBy, id]);

    if (!stmt.getNumRows()) {
      return res.status(404).json({ message: "File not found" });
    }

    await runQuery(`DELETE FROM FILES WHERE ID = ?`, [id]);
    res.json({ message: "File moved to trash" });
    safeLogWithRequest(req, {
      action: "delete_file",
      description: `File ${id} moved to trash`,
      resourceId: id,
      resourceType: "file",
    });
  } catch (error) {
    console.error("deleteFile error:", error);
    res.status(500).json({ error: error.message || "Failed to delete file" });
  }
};

export const deleteFolder = async (req, res) => {
  const { id } = req.params;
  const deletedBy = req.user?.email || "system";

  try {
    await ensureFileMetaColumns();
    await ensureTrashTable();

    const insertFolderSql = `
      INSERT INTO TRASH (
        ID,
        ITEM_TYPE,
        ITEM_NAME,
        DELETED_BY,
        DELETED_AT,
        RESTORED,
        FOLDER_ID,
        FILE_URL,
        FILE_SIZE,
        FILE_TYPE,
        UPLOADED_BY,
        UPLOADED_AT,
        CREATED_BY,
        CREATED_AT
      )
      SELECT
        ID,
        'folder',
        NAME,
        ?,
        CURRENT_TIMESTAMP(),
        FALSE,
        NULL,
        NULL,
        NULL,
        NULL,
        CREATED_BY,
        CREATED_AT,
        CREATED_BY,
        CREATED_AT
      FROM FOLDERS
      WHERE ID = ?
    `;

    const folderInsert = await runQuery(insertFolderSql, [deletedBy, id]);

    if (!folderInsert.stmt.getNumRows()) {
      return res.status(404).json({ message: "Folder not found" });
    }

    const insertFilesSql = `
      INSERT INTO TRASH (
        ID,
        ITEM_TYPE,
        ITEM_NAME,
        DELETED_BY,
        DELETED_AT,
        RESTORED,
        FOLDER_ID,
        FILE_URL,
        FILE_SIZE,
        FILE_TYPE,
        UPLOADED_BY,
        UPLOADED_AT,
        CREATED_BY,
        CREATED_AT
      )
      SELECT
        ID,
        'file',
        FILE_NAME,
        ?,
        CURRENT_TIMESTAMP(),
        FALSE,
        FOLDER_ID,
        FILE_URL,
        COALESCE(FILE_SIZE, 0),
        FILE_TYPE,
        UPLOADED_BY,
        UPLOADED_AT,
        UPLOADED_BY,
        UPLOADED_AT
      FROM FILES
      WHERE FOLDER_ID = ?
    `;

    await runQuery(insertFilesSql, [deletedBy, id]);
    await runQuery(`DELETE FROM FILES WHERE FOLDER_ID = ?`, [id]);
    await runQuery(`DELETE FROM FOLDERS WHERE ID = ?`, [id]);

    res.json({ message: "Folder and its files moved to trash" });
    safeLogWithRequest(req, {
      action: "delete_folder",
      description: `Folder ${id} moved to trash`,
      resourceId: id,
      resourceType: "folder",
    });
  } catch (error) {
    console.error("deleteFolder error:", error);
    res.status(500).json({ error: error.message || "Failed to delete folder" });
  }
};

// Add comment to file
export const addComment = (req, res) => {
  const { id } = req.params;
  const { comment } = req.body;
  const userEmail = req.user.email;

  const sql = `INSERT INTO FILE_COMMENTS (FILE_ID, USER_EMAIL, COMMENT, CREATED_AT)
               VALUES ('${id}', '${userEmail}', '${comment}', CURRENT_TIMESTAMP())`;

  connection.execute({
    sqlText: sql,
    complete: (err) =>
      err ? res.status(500).json({ error: err.message }) : res.json({ message: "Comment added successfully" }),
  });
  safeLogWithRequest(req, {
    action: "comment_file",
    description: `Comment added on file ${id}`,
    resourceId: id,
    resourceType: "file",
  });
};






export const viewFile = async (req, res) => {
  const { id } = req.params;

  try {
    // Ensure VIEWS_COUNT column exists
    try {
      await execSql("ALTER TABLE FILES ADD COLUMN IF NOT EXISTS VIEWS_COUNT NUMBER DEFAULT 0");
    } catch (e) {
      console.warn("Warning: Could not ensure VIEWS_COUNT column:", e.message);
    }

    // Fetch file URL
    const sql = `SELECT FILE_URL FROM FILES WHERE ID='${id}'`;
    connection.execute({
      sqlText: sql,
      complete: async (err, stmt, rows) => {
        if (err || !rows || rows.length === 0) {
          return res.status(404).json({ message: "File not found" });
        }

        const fileUrl = rows[0].FILE_URL;
        const urlParts = new URL(fileUrl);
        const fileKey = decodeURIComponent(urlParts.pathname.replace(/^\/+/, ""));

        // Increment VIEWS_COUNT
        try {
          await execSql(`UPDATE FILES SET VIEWS_COUNT = COALESCE(FILES.VIEWS_COUNT, 0) + 1 WHERE ID='${id}'`);
          console.log(`[viewFile] Incremented VIEWS_COUNT for file: ${id}`);
        } catch (updateErr) {
          console.warn("[viewFile] Could not update VIEWS_COUNT:", updateErr.message);
        }

        // 🧠 Generate a presigned URL valid for 1 hour (using AWS SDK v2)
        const signedUrl = await s3.getSignedUrlPromise("getObject", {
          Bucket: process.env.AWS_BUCKET_NAME,
          Key: fileKey,
          Expires: 3600,
        });

        // ✅ Return signed URL (browser can open directly)
        res.json({ url: signedUrl });
        safeLogWithRequest(req, {
          action: "view_file",
          description: `Viewed file ${id}`,
          resourceId: id,
          resourceType: "file",
        });
      },
    });
  } catch (error) {
    console.error("Error in viewFile:", error);
    res.status(500).json({ message: "Internal Server Error" });
  }
};


// Download file
export const downloadFile = async (req, res) => {
  const { id } = req.params;

  try {
    // Ensure DOWNLOADS_COUNT column exists
    try {
      await execSql("ALTER TABLE FILES ADD COLUMN IF NOT EXISTS DOWNLOADS_COUNT NUMBER DEFAULT 0");
    } catch (e) {
      console.warn("Warning: Could not ensure DOWNLOADS_COUNT column:", e.message);
    }

    const sql = `SELECT FILE_URL FROM FILES WHERE ID='${id}'`;
    connection.execute({
      sqlText: sql,
      complete: async (err, stmt, rows) => {
        if (err || !rows || rows.length === 0) {
          return res.status(404).json({ message: "File not found" });
        }

        const fileUrl = rows[0].FILE_URL;
        const fileKey = fileUrl.split("/").slice(-2).join("/"); // folderId/filename for S3

        // Increment DOWNLOADS_COUNT using a read-then-update to avoid ambiguous column errors
        try {
          const selSql = `SELECT DOWNLOADS_COUNT FROM FILES WHERE ID='${id}' LIMIT 1`;
          let current = 0;
          try {
            const { rows: curRows } = await execSql(selSql);
            if (curRows && curRows.length > 0) current = Number(curRows[0].DOWNLOADS_COUNT || 0);
          } catch (selErr) {
            console.warn('[downloadFile] Could not read DOWNLOADS_COUNT for', id, selErr.message);
          }
          const newVal = current + 1;
          const updateSql = `UPDATE FILES SET DOWNLOADS_COUNT = ${newVal} WHERE ID='${id}'`;
          try {
            await execSql(updateSql);
            console.log(`[downloadFile] Incremented DOWNLOADS_COUNT for file: ${id} -> ${newVal}`);
          } catch (updateErr) {
            console.warn('[downloadFile] Initial DOWNLOADS_COUNT update failed:', updateErr && updateErr.message ? updateErr.message : updateErr);
            if (updateErr && typeof updateErr.message === 'string' && updateErr.message.toLowerCase().includes('ambiguous')) {
              const fallbackSql = `UPDATE FILES SET DOWNLOADS_COUNT = (SELECT COALESCE(F2.DOWNLOADS_COUNT,0) + 1 FROM FILES F2 WHERE F2.ID='${id}') WHERE ID='${id}'`;
              try {
                await execSql(fallbackSql);
                console.log(`[downloadFile] Fallback increment succeeded for file: ${id}`);
              } catch (fbErr) {
                console.warn('[downloadFile] Fallback update also failed:', fbErr && fbErr.message ? fbErr.message : fbErr, 'FallbackSQL:', fallbackSql);
              }
            } else {
              console.warn("[downloadFile] Could not update DOWNLOADS_COUNT:", updateErr && updateErr.message ? updateErr.message : updateErr);
            }
          }
        } catch (updateErr) {
          console.warn("[downloadFile] Could not update DOWNLOADS_COUNT:", updateErr.message);
        }

        const params = {
          Bucket: process.env.AWS_BUCKET_NAME,
          Key: fileKey,
        };

        s3.getObject(params, (err, data) => {
          if (err) return res.status(500).json({ message: "Error downloading file" });
          res.setHeader("Content-Disposition", `attachment; filename=${fileKey.split("/")[1]}`);
          res.send(data.Body);
        });
      },
    });
  } catch (error) {
    console.error("Error in downloadFile:", error);
    res.status(500).json({ message: "Error downloading file" });
  }
};
